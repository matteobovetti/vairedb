//! Label a result column the way PostgreSQL labels it.
//!
//! PostgreSQL derives the name of an unaliased result column with one function,
//! `FigureColname` in `parse_target.c`, and that function answers in one of two ways.
//! Either the expression has a bare name it can be called after — a function call is named
//! after the function (`SELECT sum(n)` is `sum`), a subscript after what is subscripted
//! (`SELECT a[1]` is `a`), a cast after what is cast and failing that after the target type
//! (`SELECT '1'::int` is `int4`) — or it has none at all, and then the column is called
//! **`?column?`**. An operator, a literal, a comparison, an `IN`, an `ANY`/`ALL`: all
//! `?column?`.
//!
//! DataFusion names an unaliased column after the whole expression **as it renders
//! internally**, which is neither of those. `SELECT 5 # 3` comes back as
//! `Int64(5) BIT_XOR Int64(3)`; `SELECT row_number() OVER (ORDER BY x)` as
//! `row_number() ORDER BY [t.x ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND
//! CURRENT ROW`, 99 bytes, past PostgreSQL's 63-byte `NAMEDATALEN`; and an expression one
//! of the read path's own rewrites has expanded renders as **the expansion** — `1 < ALL
//! (ARRAY[2,3])` becomes a ~400-character `CASE WHEN make_array(…)` header naming
//! functions the client never wrote.
//!
//! A label is not cosmetic to a client. A driver that reads columns by name, an ORM
//! that maps them onto fields, and `psql`'s own header all take the label as the
//! contract, so the read path adds the alias PostgreSQL would have given.
//!
//! Labelling here also keeps a label from *becoming* internal as the read path rewrites
//! an expression. The subscript clamp ([`super::pg_subscripts`]) would turn `t.a[Int64(1)]`
//! into `t.a[nullif(greatest(Int64(1),Int64(0)), Int64(0))]` — the rewrite's own spelling,
//! in a name, over the 63-byte limit — and a `::bytea` becomes a `vaire_bytea_in(…)` call
//! ([`vairedb_common::bytea_in`]), which a client asking for `s::bytea` should not be told
//! its column is named after. Both are labelled before the rewrite runs.
//!
//! ## A label that repeats, which PostgreSQL allows and a plan cannot hold
//!
//! PostgreSQL is happy to return two columns with the same name: `SELECT sum(a), sum(b)`
//! is two columns both called `sum`, `SELECT 1+1, 2+2` is two both called `?column?`, and
//! `SELECT count, count(*)` is two called `count`. DataFusion refuses that outright —
//! *"Projections require unique expression names"* — so an alias that simply repeats would
//! turn a query that works today into a planning error.
//!
//! So a repeat is **disambiguated inside the plan and collapsed on the way out**. The
//! *n*-th column of one select list to be given the label `sum` is aliased `sum`,
//! `sum`[`DISAMBIGUATOR`]`2`, `sum`[`DISAMBIGUATOR`]`3`, … — names unique in the plan — and
//! [`wire_label`] cuts each back to `sum` in the one place both Describe and Execute derive
//! their columns from ([`super::encoding::wire_schema`]). The plan carries the suffix; no
//! client ever sees it. `?column?` is the same mechanism with the same suffix and is not a
//! case of its own.
//!
//! The disambiguator is **`NUL`**, and that is what makes the collapse exact rather than a
//! guess. Every name a client can ask for arrives inside a SQL string in a Parse or Query
//! message, and those are NUL-terminated: a client cannot put a NUL in an identifier,
//! quoted or not, so a name carrying one is necessarily one of these and nothing else. A
//! printable marker would have had to be a spelling a client merely *probably* never uses.
//!
//! The one place the suffix is visible is a plan **rendered as text**, which is
//! `EXPLAIN`'s output — a NUL inside a value is not something a client can be sent, since
//! libpq reads a text cell up to its first one. [`without_disambiguators`] takes the suffix
//! back out there, and `super::introspection` is the one caller.
//!
//! A name a client wrote occupies its slot as itself: `SELECT 1 AS "?column?", 2` keeps the
//! client's `?column?` and labels the second column `?column?`⟨NUL⟩`2`, so both go out as
//! `?column?` — which is what PostgreSQL returns for it too.
//!
//! The residue left is DataFusion's own: an expression [`pg_label`] has no PostgreSQL rule
//! for keeps the name DataFusion derives, which is a rendering of the expression rather
//! than a name. Those are the forms PostgreSQL has no equivalent of.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use crate::sqlparser::ast::{
    AccessExpr, ArrayElemTypeDef, CastKind, DataType, ExactNumberInfo, Expr, Ident, Query, Select,
    SelectItem, SetExpr, Statement, TimezoneInfo, TrimWhereField, VisitMut, VisitorMut,
};

/// The name PostgreSQL gives a result column that has no name of its own.
pub(super) const ANONYMOUS_LABEL: &str = "?column?";

/// What separates a label from the number that keeps its repeats apart inside the plan.
///
/// `NUL`, because it is the one character no client-supplied name can contain — see the
/// module header.
pub(super) const DISAMBIGUATOR: char = '\0';

/// The name a result field goes on the wire as, or `None` when it goes as itself.
///
/// The only fields renamed are the ones this module disambiguated to keep them unique
/// inside the plan: PostgreSQL returns both columns of `SELECT sum(a), sum(b)` as `sum` and
/// both of `SELECT 1+1, 2+2` as `?column?`, and a DataFusion schema cannot hold either name
/// twice. Exact rather than a guess: a name carrying a `NUL` is one this module built.
pub(super) fn wire_label(name: &str) -> Option<&str> {
    name.split_once(DISAMBIGUATOR).map(|(label, _)| label)
}

/// `text` with every disambiguating suffix taken back out of it.
///
/// For a plan rendered **as text**, which is `EXPLAIN`'s answer: the rendering names the
/// plan's own columns, suffix and all, and a NUL cannot be sent to a client inside a value
/// — libpq reads a text cell up to the first one, so the line would arrive truncated. What
/// is left is the label the client asked about, repeated as often as PostgreSQL repeats it.
///
/// Borrowed unless there is something to remove, which is every plan of every query whose
/// columns are distinctly named.
pub(super) fn without_disambiguators(text: &str) -> Cow<'_, str> {
    if !text.contains(DISAMBIGUATOR) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(DISAMBIGUATOR) {
        out.push_str(&rest[..at]);
        rest =
            rest[at + DISAMBIGUATOR.len_utf8()..].trim_start_matches(|c: char| c.is_ascii_digit());
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// Give every unaliased result column the label PostgreSQL would.
///
/// Applied to a whole statement, so a subquery's and a set operation's own select lists
/// are labelled too — a `UNION` takes its column names from its first branch, and a
/// derived table's names are what the enclosing query refers to.
///
/// Read-path only, and skipped for catalog introspection: those statements come from
/// `datafusion-pg-catalog`'s own rewrites, tuned to the column names particular drivers
/// look for, and this has no business renaming them.
pub(super) fn label_result_columns(stmt: &mut Statement) {
    struct Labeler;

    impl VisitorMut for Labeler {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
            label_set_expr(&mut query.body);
            ControlFlow::Continue(())
        }
    }

    let _ = stmt.visit(&mut Labeler);
}

/// Label the select lists directly under `body`.
///
/// A nested `SetExpr::Query` is deliberately *not* followed: the visitor reaches that
/// `Query` on its own and would then label it twice.
fn label_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => label_select(select),
        SetExpr::SetOperation { left, right, .. } => {
            label_set_expr(left);
            label_set_expr(right);
        }
        _ => {}
    }
}

/// Alias the items of one select list with the labels PostgreSQL gives them.
fn label_select(select: &mut Select) {
    let mut scope = LabelScope::of(&select.projection);

    select.projection = std::mem::take(&mut select.projection)
        .into_iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) if !already_named_by_datafusion(&expr) => {
                match pg_label(&expr) {
                    Some(label) => SelectItem::ExprWithAlias {
                        alias: alias_ident(scope.hand_out(&label)),
                        expr,
                    },
                    None => SelectItem::UnnamedExpr(expr),
                }
            }
            other => other,
        })
        .collect();
}

/// The names one select list has already spoken for, and the numbering that keeps a label
/// it hands out twice distinct inside the plan.
struct LabelScope {
    /// The names held by items this pass does not rename — an alias the client wrote, and a
    /// plain column DataFusion already names PostgreSQL's way. A label equal to one of these
    /// has to be disambiguated even the first time it is handed out.
    reserved: HashSet<String>,
    /// Whether the projection has a wildcard, whose columns are not knowable here.
    has_wildcard: bool,
    /// How many times each label has been handed out so far.
    handed_out: HashMap<String, usize>,
}

impl LabelScope {
    fn of(projection: &[SelectItem]) -> Self {
        let mut reserved = HashSet::new();
        let mut has_wildcard = false;
        for item in projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    reserved.insert(alias.value.to_ascii_lowercase());
                }
                // A plain column keeps DataFusion's name, which is the column's own — see
                // [`already_named_by_datafusion`] — so it holds that name against a label.
                SelectItem::UnnamedExpr(expr) if already_named_by_datafusion(expr) => {
                    if let Some(label) = pg_label(expr) {
                        reserved.insert(label.name().to_string());
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => has_wildcard = true,
                // Everything else either gets a label below, or keeps a DataFusion-derived
                // name that contains the expression itself and so cannot equal a bare one.
                _ => {}
            }
        }
        Self {
            reserved,
            has_wildcard,
            handed_out: HashMap::new(),
        }
    }

    /// The name to alias the next column carrying `label` with: the label itself the first
    /// time it is free, and the label plus [`DISAMBIGUATOR`] and its ordinal after that.
    ///
    /// A wildcard's own columns are not known here, and a label taken **from a column** is
    /// certainly among them where it is — `SELECT *, a[1] FROM t` projects `a` twice — so
    /// that label starts disambiguated. It costs nothing to do so where the wildcard turns
    /// out not to contain it: the client is told `a` either way, because the collapse is on
    /// the wire and not in the plan. A label taken from a type or a function name is not at
    /// risk the same way and is unaffected.
    fn hand_out(&mut self, label: &PgLabel) -> String {
        let name = label.name();
        let held = self.reserved.contains(name) || (self.has_wildcard && label.is_from_column());
        let handed = self.handed_out.entry(name.to_string()).or_insert(0);
        *handed += 1;
        // Only the unsuffixed name can collide with a client's: a suffixed one carries a
        // NUL, which no name a client sent can.
        match *handed + usize::from(held) {
            1 => name.to_string(),
            n => format!("{name}{DISAMBIGUATOR}{n}"),
        }
    }
}

/// `name` as an alias in the AST, quoted where it is not a bare identifier.
///
/// The AST is handed to the planner directly, but anything that renders it has to stay
/// valid SQL — and two of these names are not identifiers: `?column?` is not one at all,
/// and a disambiguated name carries a NUL.
fn alias_ident(name: String) -> Ident {
    let is_bare = name.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if is_bare {
        Ident::new(name)
    } else {
        Ident::with_quote('"', name)
    }
}

/// Whether DataFusion already gives this expression PostgreSQL's own label, so that
/// aliasing it would only cost something.
///
/// A column reference is the case: DataFusion names the field after the column, which is
/// what PostgreSQL does, and an alias would additionally **drop the qualifier** — a
/// `SELECT t.a` field aliased `a` is no longer `t.a` to anything reading the plan.
fn already_named_by_datafusion(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => {
            already_named_by_datafusion(inner)
        }
        _ => false,
    }
}

/// The label PostgreSQL gives an unaliased expression.
enum PgLabel {
    /// A bare name.
    Named {
        name: String,
        /// PostgreSQL's own strength distinction, and the only thing it decides: a name a
        /// *fallback* produced — a cast's target type, `case` — loses to one an inner
        /// expression produced, so `('a'::text)::bytea` is `bytea` while `(s::text)::bytea`
        /// is `s`.
        weak: bool,
        /// Whether the name came from a column, and so can collide with the columns of a
        /// wildcard in the same projection.
        from_column: bool,
    },
    /// PostgreSQL has no name for this expression and calls the column `?column?`.
    Anonymous,
}

impl PgLabel {
    /// The name this label gives a column. For an anonymous one that is [`ANONYMOUS_LABEL`],
    /// which is a name like any other here: it repeats exactly as often, and is kept unique
    /// inside the plan by exactly the same numbering.
    fn name(&self) -> &str {
        match self {
            PgLabel::Named { name, .. } => name,
            PgLabel::Anonymous => ANONYMOUS_LABEL,
        }
    }

    /// Whether the name came from a column, and so can be among a wildcard's own columns.
    fn is_from_column(&self) -> bool {
        matches!(
            self,
            PgLabel::Named {
                from_column: true,
                ..
            }
        )
    }
}

/// A name taken from a column.
fn from_column(name: &str) -> PgLabel {
    PgLabel::Named {
        name: name.to_ascii_lowercase(),
        weak: false,
        from_column: true,
    }
}

/// A name PostgreSQL derives outright — a function, `array`, `row`.
fn strong(name: &str) -> PgLabel {
    PgLabel::Named {
        name: name.to_ascii_lowercase(),
        weak: false,
        from_column: false,
    }
}

/// A name PostgreSQL falls back to when the expression yields none — a cast's target type,
/// `case`.
fn weak(name: String) -> PgLabel {
    PgLabel::Named {
        name: name.to_ascii_lowercase(),
        weak: true,
        from_column: false,
    }
}

/// Whether this label is one PostgreSQL derived outright, which is the only kind that
/// survives being wrapped in a cast or a `CASE`.
fn is_strong(label: &Option<PgLabel>) -> bool {
    matches!(label, Some(PgLabel::Named { weak: false, .. }))
}

/// PostgreSQL's label for `expr`, or `None` if PostgreSQL has no such expression and so no
/// label to match — a DuckDB-only form, which keeps DataFusion's own.
///
/// A port of `FigureColnameInternal`, arm for arm, measured against PostgreSQL 17.
fn pg_label(expr: &Expr) -> Option<PgLabel> {
    let label = match expr {
        Expr::Identifier(ident) => from_column(&ident.value),
        Expr::CompoundIdentifier(parts) => from_column(&parts.last()?.value),
        // PostgreSQL reads the **last field name** out of an indirection, ignoring every
        // subscript, and recurses into what is being subscripted when there is none. So
        // `t.a[1]` and `(s).f[1]` are `a` and `f`, `a[1].f` is `f`, and
        // `(ARRAY[1,2,3])[1]` is `array`.
        Expr::CompoundFieldAccess { root, access_chain } => {
            let field = access_chain.iter().rev().find_map(|access| match access {
                AccessExpr::Dot(Expr::Identifier(ident)) => Some(&ident.value),
                _ => None,
            });
            match field {
                Some(name) => from_column(name),
                None => pg_label(root)?,
            }
        }
        Expr::Function(_) => strong(&function_label(expr)?),
        // A cast is named after what is cast, and after the **target type** when what is
        // cast has no name of its own: `s::bytea` is `s` and `'\x41'::bytea` is `bytea`.
        // `TRY_CAST` is not PostgreSQL syntax, so it has no label to match.
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            data_type,
            ..
        } => {
            let inner_label = pg_label(inner);
            match is_strong(&inner_label) {
                true => inner_label?,
                false => weak(pg_type_name(data_type)?),
            }
        }
        // `DATE '2020-01-01'` and `INTERVAL '1 day'` are casts of a literal in
        // PostgreSQL's grammar, so both land on the type-name fallback.
        Expr::TypedString(typed) => weak(pg_type_name(&typed.data_type)?),
        Expr::Interval(_) => weak("interval".to_string()),
        // Parentheses and `COLLATE` are transparent to the name.
        Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => pg_label(inner)?,
        // A `CASE` is named after its `ELSE` result where that result has a name of its
        // own, and `case` otherwise — so `CASE WHEN … THEN 1 ELSE b END` is `b`.
        Expr::Case { else_result, .. } => {
            let else_label = else_result.as_deref().and_then(pg_label);
            match is_strong(&else_label) {
                true => else_label?,
                false => weak("case".to_string()),
            }
        }
        Expr::Array(_) => strong("array"),
        Expr::Tuple(_) => strong("row"),
        // `NOT EXISTS` is a boolean negation of the `EXISTS`, and a negation has no name.
        Expr::Exists { negated: false, .. } => strong("exists"),
        // A scalar subquery is named after **its own** single result column, which is the
        // same question one level down.
        Expr::Subquery(query) => subquery_label(query)?,
        // The forms PostgreSQL's grammar turns into a function call, each named after the
        // function it becomes — including `TRIM`, which becomes `btrim`/`ltrim`/`rtrim`
        // depending on the side it was asked to trim.
        Expr::Extract { .. } => strong("extract"),
        Expr::Substring { .. } => strong("substring"),
        Expr::Position { .. } => strong("position"),
        Expr::Overlay { .. } => strong("overlay"),
        Expr::Ceil { .. } => strong("ceil"),
        Expr::Floor { .. } => strong("floor"),
        Expr::AtTimeZone { .. } => strong("timezone"),
        Expr::Trim { trim_where, .. } => strong(match trim_where {
            Some(TrimWhereField::Leading) => "ltrim",
            Some(TrimWhereField::Trailing) => "rtrim",
            Some(TrimWhereField::Both) | None => "btrim",
        }),
        // Everything PostgreSQL parses as an operator, a comparison, a test or a bare
        // constant: no name at all.
        Expr::Value(_)
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::SimilarTo { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_)
        | Expr::IsDistinctFrom(..)
        | Expr::IsNotDistinctFrom(..)
        | Expr::JsonAccess { .. }
        | Expr::Exists { negated: true, .. } => PgLabel::Anonymous,
        _ => return None,
    };
    Some(label)
}

/// The label of a scalar subquery: the label of the single column it selects.
///
/// `None` where that column cannot be named from the AST — a `SELECT *` inside the
/// subquery, whose one column is only known once the table is resolved. Guessing
/// `?column?` there would be a *wrong* label rather than a missing one, so DataFusion's
/// is left in place.
fn subquery_label(query: &Query) -> Option<PgLabel> {
    let select = match &*query.body {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    match select.projection.first()? {
        SelectItem::ExprWithAlias { alias, .. } => Some(strong(&alias.value)),
        SelectItem::UnnamedExpr(expr) => pg_label(expr),
        _ => None,
    }
}

/// The label PostgreSQL gives an unaliased call to this function, or `None` if the
/// expression is not a function call.
///
/// The bare function name, lowercased, and the *client's* spelling of it: this runs
/// before the aggregate renames in [`super::pg_operators`], so `SELECT variance(x)`
/// is labelled `variance` and not the `var_samp` VaireDB plans it as.
fn function_label(expr: &Expr) -> Option<String> {
    let Expr::Function(func) = expr else {
        return None;
    };
    Some(func.name.0.last()?.as_ident()?.value.to_ascii_lowercase())
}

/// PostgreSQL's own name for a type written in SQL, which is the name a cast falls back to
/// labelling its column with — `pg_type.typname`, not the SQL spelling. `SELECT '1'::int`
/// is `int4` and `SELECT 'a'::char(2)` is `bpchar`.
///
/// `None` for a type PostgreSQL does not have, since there is no PostgreSQL label to match:
/// the cast then keeps DataFusion's own. An array's name is its **element's**, because
/// PostgreSQL reads the label off the type name and carries the array-ness beside it — so
/// `'{1}'::int[]` is `int4`.
fn pg_type_name(data_type: &DataType) -> Option<String> {
    use DataType as T;
    let name = match data_type {
        T::Bool | T::Boolean => "bool",
        T::SmallInt(_) | T::Int2(_) => "int2",
        T::Int(_) | T::Integer(_) | T::Int4(_) => "int4",
        T::BigInt(_) | T::Int8(_) => "int8",
        T::Real | T::Float4 => "float4",
        T::Double(_) | T::DoublePrecision | T::Float8 => "float8",
        // PostgreSQL resolves `FLOAT(p)` to `float4` up to 24 bits of mantissa and
        // `float8` above it; a bare `FLOAT` is `float8`.
        T::Float(ExactNumberInfo::Precision(p)) if *p <= 24 => "float4",
        T::Float(_) => "float8",
        T::Numeric(_) | T::Decimal(_) | T::Dec(_) => "numeric",
        T::Varchar(_) | T::CharacterVarying(_) | T::CharVarying(_) => "varchar",
        T::Char(_) | T::Character(_) => "bpchar",
        T::Text => "text",
        T::Bytea => "bytea",
        T::Date => "date",
        T::Time(_, TimezoneInfo::Tz | TimezoneInfo::WithTimeZone) => "timetz",
        T::Time(..) => "time",
        T::Timestamp(_, TimezoneInfo::Tz | TimezoneInfo::WithTimeZone) => "timestamptz",
        T::Timestamp(..) => "timestamp",
        T::Interval { .. } => "interval",
        T::JSON => "json",
        T::JSONB => "jsonb",
        T::Uuid => "uuid",
        T::Regclass => "regclass",
        T::Bit(_) => "bit",
        T::BitVarying(_) | T::VarBit(_) => "varbit",
        T::TsVector => "tsvector",
        T::TsQuery => "tsquery",
        T::Array(ArrayElemTypeDef::SquareBracket(element, _))
        | T::Array(ArrayElemTypeDef::AngleBracket(element))
        | T::Array(ArrayElemTypeDef::Parenthesis(element)) => return pg_type_name(element),
        // A user-defined type is named as written: an enum or a domain is a `pg_type` row
        // whose `typname` is the name the client spelled.
        T::Custom(name, _) => {
            return Some(name.0.last()?.as_ident()?.value.to_ascii_lowercase());
        }
        _ => return None,
    };
    Some(name.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType as ArrowType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::sql::parser::Statement as DFStatement;

    use super::*;
    use crate::pgwire_handler::encoding::wire_schema;
    use crate::sqlparser::dialect::PostgreSqlDialect;
    use crate::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statements()
            .unwrap()
            .remove(0)
    }

    fn labelled(sql: &str) -> String {
        let mut stmt = parse(sql);
        label_result_columns(&mut stmt);
        stmt.to_string()
    }

    /// The column names of a labelled statement as **DataFusion's planner** derives them,
    /// beside the ones a client is told.
    ///
    /// The pair is the point: the plan has to hold names that are unique, because
    /// DataFusion refuses a projection that does not, and the client has to be told the
    /// name PostgreSQL uses however often it repeats. `wire_schema` is the one place both
    /// Describe and Execute read, so asserting on it is asserting on what goes on the wire.
    async fn planned_and_wire_labels(sql: &str) -> (Vec<String>, Vec<String>) {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowType::Int32, true),
            Field::new("b", ArrowType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let mut stmt = parse(sql);
        label_result_columns(&mut stmt);
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
            .await
            .unwrap_or_else(|e| panic!("planning `{sql}` failed: {e}"));
        let names = |schema: &Schema| -> Vec<String> {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        };
        let planned = names(plan.schema().as_arrow());
        let wire = names(&wire_schema(plan.schema().as_arrow()));
        (planned, wire)
    }

    #[test]
    fn labels_an_aggregate_with_its_own_name() {
        assert_eq!(
            labelled("SELECT sum(n) FROM t"),
            "SELECT sum(n) AS sum FROM t"
        );
        assert_eq!(
            labelled("SELECT count(*) FROM t"),
            "SELECT count(*) AS count FROM t"
        );
    }

    // The long one: the label DataFusion derives here is 99 bytes of frame internals,
    // past PostgreSQL's own 63-byte limit for a name.
    #[test]
    fn labels_a_window_function() {
        assert_eq!(
            labelled("SELECT row_number() OVER (ORDER BY x) FROM t"),
            "SELECT row_number() OVER (ORDER BY x) AS row_number FROM t"
        );
    }

    // The client's spelling, because this runs before the aggregate renames.
    #[test]
    fn labels_with_the_name_the_client_wrote() {
        assert_eq!(
            labelled("SELECT variance(x) FROM t"),
            "SELECT variance(x) AS variance FROM t"
        );
    }

    // PostgreSQL returns both of these columns as `sum`. DataFusion cannot hold that name
    // twice, so the second carries the disambiguator inside the plan and is collapsed on
    // the wire.
    #[test]
    fn disambiguates_a_label_that_repeats() {
        assert_eq!(
            labelled("SELECT sum(a), sum(b) FROM t"),
            format!("SELECT sum(a) AS sum, sum(b) AS \"sum{DISAMBIGUATOR}2\" FROM t")
        );
        // A third one keeps counting.
        assert_eq!(
            labelled("SELECT sum(a), sum(b), sum(c) FROM t"),
            format!(
                "SELECT sum(a) AS sum, sum(b) AS \"sum{DISAMBIGUATOR}2\", \
                 sum(c) AS \"sum{DISAMBIGUATOR}3\" FROM t"
            )
        );
    }

    // A name another item holds is disambiguated the first time it is handed out, because
    // that item is not one this pass can rename: an alias the client wrote, or a plain
    // column DataFusion already names after itself.
    #[test]
    fn steps_over_a_name_the_projection_already_holds() {
        assert_eq!(
            labelled("SELECT sum(a) AS sum, sum(b) FROM t"),
            format!("SELECT sum(a) AS sum, sum(b) AS \"sum{DISAMBIGUATOR}2\" FROM t")
        );
        assert_eq!(
            labelled("SELECT count, count(*) FROM t"),
            format!("SELECT count, count(*) AS \"count{DISAMBIGUATOR}2\" FROM t")
        );
        // Whichever order they come in.
        assert_eq!(
            labelled("SELECT sum(b), sum(a) AS sum FROM t"),
            format!("SELECT sum(b) AS \"sum{DISAMBIGUATOR}2\", sum(a) AS sum FROM t")
        );
    }

    // Two different functions do not collide, so both are labelled.
    #[test]
    fn labels_each_of_two_distinct_functions() {
        assert_eq!(
            labelled("SELECT sum(a), count(*) FROM t"),
            "SELECT sum(a) AS sum, count(*) AS count FROM t"
        );
    }

    // An alias the client wrote is the client's, and is never replaced.
    #[test]
    fn never_overwrites_an_alias_the_client_wrote() {
        let sql = "SELECT sum(n) AS total FROM t";
        assert_eq!(labelled(sql), sql);
    }

    #[test]
    fn labels_a_subquery_and_both_sides_of_a_union() {
        assert_eq!(
            labelled("SELECT * FROM (SELECT max(x) FROM t) s"),
            "SELECT * FROM (SELECT max(x) AS max FROM t) s"
        );
        assert_eq!(
            labelled("SELECT min(x) FROM a UNION ALL SELECT max(x) FROM b"),
            "SELECT min(x) AS min FROM a UNION ALL SELECT max(x) AS max FROM b"
        );
    }

    // A subscript is labelled after what is subscripted, in each of the shapes that has a
    // bare name — measured against PostgreSQL 17.
    #[test]
    fn labels_a_subscript_after_what_it_subscripts() {
        assert_eq!(labelled("SELECT a[1] FROM t"), "SELECT a[1] AS a FROM t");
        assert_eq!(
            labelled("SELECT a[1:2] FROM t"),
            "SELECT a[1:2] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT a[1][2] FROM t"),
            "SELECT a[1][2] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT t.a[1] FROM t"),
            "SELECT t.a[1] AS a FROM t"
        );
        assert_eq!(
            labelled("SELECT (ARRAY[1, 2, 3])[1]"),
            "SELECT (ARRAY[1, 2, 3])[1] AS array"
        );
        assert_eq!(
            labelled("SELECT string_to_array(s, ',')[1] FROM t"),
            "SELECT string_to_array(s, ',')[1] AS string_to_array FROM t"
        );
    }

    // The same uniqueness rule as a function's: PostgreSQL is happy with two columns called
    // `a` and DataFusion is not, so the repeat is suffixed in the plan.
    #[test]
    fn disambiguates_a_subscript_label_that_repeats() {
        for (sql, expected) in [
            (
                "SELECT a, a[1] FROM t",
                format!("SELECT a, a[1] AS \"a{DISAMBIGUATOR}2\" FROM t"),
            ),
            (
                "SELECT a[1], a[2] FROM t",
                format!("SELECT a[1] AS a, a[2] AS \"a{DISAMBIGUATOR}2\" FROM t"),
            ),
            (
                "SELECT a[1] AS a, a[2] FROM t",
                format!("SELECT a[1] AS a, a[2] AS \"a{DISAMBIGUATOR}2\" FROM t"),
            ),
        ] {
            assert_eq!(labelled(sql), expected, "`{sql}`");
        }
    }

    // A wildcard is the collision that cannot be counted, so a label taken from a column
    // starts suffixed beside one — `SELECT *, a[1] FROM t` does project `a` twice. The client
    // is told `a` either way, because the collapse is on the wire.
    #[test]
    fn a_subscript_beside_a_wildcard_starts_disambiguated() {
        for (sql, expected) in [
            (
                "SELECT *, a[1] FROM t",
                format!("SELECT *, a[1] AS \"a{DISAMBIGUATOR}2\" FROM t"),
            ),
            (
                "SELECT t.*, a[1] FROM t",
                format!("SELECT t.*, a[1] AS \"a{DISAMBIGUATOR}2\" FROM t"),
            ),
        ] {
            assert_eq!(labelled(sql), expected, "`{sql}`");
        }
        // A function's label is not at risk the same way, so a wildcard does not suffix it.
        assert_eq!(
            labelled("SELECT *, sum(n) FROM t"),
            "SELECT *, sum(n) AS sum FROM t"
        );
    }

    // A `.field` before or after the subscript is labelled after the field, which is what
    // PostgreSQL reads out of an indirection: the last field name in it.
    #[test]
    fn labels_a_field_access_after_the_field() {
        assert_eq!(
            labelled("SELECT (s).f[1] FROM t"),
            "SELECT (s).f[1] AS f FROM t"
        );
        assert_eq!(
            labelled("SELECT a[1].f FROM t"),
            "SELECT a[1].f AS f FROM t"
        );
    }

    // A cast is labelled after what is cast, falling back to the **type**'s own
    // PostgreSQL name — measured against PostgreSQL 17.
    #[test]
    fn labels_a_cast_after_what_is_cast() {
        assert_eq!(
            labelled("SELECT s::bytea FROM t"),
            "SELECT s::BYTEA AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT CAST(t.s AS BYTEA) FROM t"),
            "SELECT CAST(t.s AS BYTEA) AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT (s::TEXT)::BYTEA FROM t"),
            "SELECT (s::TEXT)::BYTEA AS s FROM t"
        );
        assert_eq!(
            labelled("SELECT n::BIGINT FROM t"),
            "SELECT n::BIGINT AS n FROM t"
        );
    }

    // The type-name fallback, and PostgreSQL's own spelling of each type rather than the
    // SQL one: `int4`, not `INT`.
    #[test]
    fn falls_back_to_the_postgres_type_name() {
        for (sql, expected) in [
            ("SELECT '1'::INT", "SELECT '1'::INT AS int4"),
            ("SELECT '1'::BIGINT", "SELECT '1'::BIGINT AS int8"),
            ("SELECT '1'::SMALLINT", "SELECT '1'::SMALLINT AS int2"),
            ("SELECT '1'::NUMERIC", "SELECT '1'::NUMERIC AS numeric"),
            (
                "SELECT '1'::DOUBLE PRECISION",
                "SELECT '1'::DOUBLE PRECISION AS float8",
            ),
            ("SELECT 'a'::CHAR(2)", "SELECT 'a'::CHAR(2) AS bpchar"),
            ("SELECT 'a'::TEXT", "SELECT 'a'::TEXT AS text"),
            ("SELECT 't'::BOOLEAN", "SELECT 't'::BOOLEAN AS bool"),
            ("SELECT '\\x41'::bytea", "SELECT '\\x41'::BYTEA AS bytea"),
            // The label PostgreSQL gives this, and the one this module derives — but not
            // the one a client is told, because upstream's `FixArrayLiteral` rule has
            // replaced the string with an `ARRAY[…]` constructor before the read path is
            // handed the statement, and a cast over a constructor is named `array` (which
            // is also what PostgreSQL calls `ARRAY[1]::int[]`). Pinned as a residue in
            // `test_a_nameless_expression_is_labelled_the_way_postgres_labels_it`.
            ("SELECT '{1}'::INT[]", "SELECT '{1}'::INT[] AS int4"),
            (
                "SELECT '2020-01-01'::DATE",
                "SELECT '2020-01-01'::DATE AS date",
            ),
        ] {
            assert_eq!(labelled(sql), expected, "`{sql}`");
        }
    }

    // A weak name loses to a strong one and only to a strong one: the inner cast of
    // `('a'::text)::bytea` has nothing but its own type name, so the outer type wins.
    #[test]
    fn a_type_name_does_not_survive_an_outer_cast() {
        assert_eq!(
            labelled("SELECT ('a'::TEXT)::BYTEA"),
            "SELECT ('a'::TEXT)::BYTEA AS bytea"
        );
    }

    // A type name cannot collide with a wildcard's columns, so the literal cast is labelled
    // outright where a column cast's label starts suffixed.
    #[test]
    fn a_bytea_cast_beside_a_wildcard_is_labelled_by_where_its_name_came_from() {
        assert_eq!(
            labelled("SELECT *, '\\x41'::bytea FROM t"),
            "SELECT *, '\\x41'::BYTEA AS bytea FROM t"
        );
        assert_eq!(
            labelled("SELECT *, s::BYTEA FROM t"),
            format!("SELECT *, s::BYTEA AS \"s{DISAMBIGUATOR}2\" FROM t")
        );
    }

    // The headline of the row: an expression PostgreSQL has no name for is `?column?`,
    // not a rendering of the plan.
    #[test]
    fn labels_a_nameless_expression_anonymously() {
        for (sql, expected) in [
            ("SELECT 5 # 3", "SELECT 5 # 3 AS \"?column?\""),
            ("SELECT a + 1 FROM t", "SELECT a + 1 AS \"?column?\" FROM t"),
            ("SELECT -a FROM t", "SELECT -a AS \"?column?\" FROM t"),
            ("SELECT 1", "SELECT 1 AS \"?column?\""),
            ("SELECT NULL", "SELECT NULL AS \"?column?\""),
            (
                "SELECT a IS NULL FROM t",
                "SELECT a IS NULL AS \"?column?\" FROM t",
            ),
            (
                "SELECT a BETWEEN 1 AND 2 FROM t",
                "SELECT a BETWEEN 1 AND 2 AS \"?column?\" FROM t",
            ),
            (
                "SELECT a IN (1, 2) FROM t",
                "SELECT a IN (1, 2) AS \"?column?\" FROM t",
            ),
            (
                "SELECT a LIKE 'x%' FROM t",
                "SELECT a LIKE 'x%' AS \"?column?\" FROM t",
            ),
            (
                "SELECT 1 < ALL (ARRAY[2, 3])",
                "SELECT 1 < ALL(ARRAY[2, 3]) AS \"?column?\"",
            ),
        ] {
            assert_eq!(labelled(sql), expected, "`{sql}`");
        }
    }

    // Several of them in one select list: PostgreSQL calls every one `?column?`, and the
    // plan cannot, so they are numbered here and collapsed on the wire. `?column?` is not a
    // case of its own — it is the same numbering every other repeated label gets.
    #[test]
    fn numbers_the_anonymous_labels_of_one_select_list() {
        assert_eq!(
            labelled("SELECT a + 1, b * 2, 3 FROM t"),
            format!(
                "SELECT a + 1 AS \"?column?\", b * 2 AS \"?column?{DISAMBIGUATOR}2\", \
                 3 AS \"?column?{DISAMBIGUATOR}3\" FROM t"
            )
        );
    }

    // What the wire collapse answers, which is exact: only a name carrying the NUL is cut,
    // and a client cannot have sent one.
    #[test]
    fn the_collapse_cuts_a_suffixed_name_and_only_that() {
        assert_eq!(
            wire_label(&format!("?column?{DISAMBIGUATOR}2")),
            Some("?column?")
        );
        assert_eq!(wire_label(&format!("sum{DISAMBIGUATOR}17")), Some("sum"));
        // The first of a repeat needs no collapsing, and neither does anything else.
        assert_eq!(wire_label("?column?"), None);
        assert_eq!(wire_label("sum"), None);
        // Including a name that merely *looks* like one of ours, which the old printable
        // scheme could not tell apart from a suffix.
        assert_eq!(wire_label("?column?2"), None);
        assert_eq!(wire_label("sum2"), None);
    }

    // A client that wrote the name itself keeps it, and the numbering steps over it.
    #[test]
    fn steps_over_an_anonymous_name_the_client_wrote() {
        assert_eq!(
            labelled("SELECT 1 AS \"?column?\", 2"),
            format!("SELECT 1 AS \"?column?\", 2 AS \"?column?{DISAMBIGUATOR}2\"")
        );
    }

    // Each select list numbers from the start, because each is a schema of its own.
    #[test]
    fn numbers_each_select_list_separately() {
        assert_eq!(
            labelled("SELECT a + 1, b + 1 FROM t UNION ALL SELECT c + 1, d + 1 FROM u"),
            format!(
                "SELECT a + 1 AS \"?column?\", b + 1 AS \"?column?{DISAMBIGUATOR}2\" FROM t \
                 UNION ALL SELECT c + 1 AS \"?column?\", \
                 d + 1 AS \"?column?{DISAMBIGUATOR}2\" FROM u"
            )
        );
    }

    // `EXPLAIN` renders the plan's own names as **text**, and a NUL cannot go out inside a
    // value — libpq would read the cell up to it. What is left is the label repeated as
    // often as PostgreSQL repeats it.
    #[test]
    fn a_rendered_plan_has_the_suffixes_taken_back_out() {
        let rendered = format!(
            "ProjectionExec: expr=[a@0 + 1 as ?column?, b@1 * 2 as ?column?{DISAMBIGUATOR}2, \
             sum(t.c)@2 as sum{DISAMBIGUATOR}10]"
        );
        assert_eq!(
            without_disambiguators(&rendered),
            "ProjectionExec: expr=[a@0 + 1 as ?column?, b@1 * 2 as ?column?, sum(t.c)@2 as sum]"
        );
        // Nothing to remove is the common case, and it does not copy.
        let plain = "ProjectionExec: expr=[sum(t.a)@0 as sum]";
        assert!(matches!(
            without_disambiguators(plain),
            Cow::Borrowed(kept) if kept == plain
        ));
        // A digit that follows the removed ordinal is text, not part of it.
        assert_eq!(
            without_disambiguators(&format!("as sum{DISAMBIGUATOR}2, 3 rows")),
            "as sum, 3 rows"
        );
    }

    // The alias goes into an AST anything may render, so a name that is not a bare
    // identifier has to come back out quoted — both of the names this module invents are.
    #[test]
    fn a_name_that_is_not_an_identifier_is_quoted() {
        assert_eq!(alias_ident("sum".to_string()).to_string(), "sum");
        assert_eq!(alias_ident("sum_1".to_string()).to_string(), "sum_1");
        assert_eq!(
            alias_ident(ANONYMOUS_LABEL.to_string()).to_string(),
            "\"?column?\""
        );
        assert_eq!(
            alias_ident(format!("sum{DISAMBIGUATOR}2")).to_string(),
            format!("\"sum{DISAMBIGUATOR}2\"")
        );
        // A digit cannot start an identifier, and an upper-case letter would be folded.
        assert_eq!(alias_ident("2x".to_string()).to_string(), "\"2x\"");
        assert_eq!(alias_ident("Sum".to_string()).to_string(), "\"Sum\"");
    }

    // The forms PostgreSQL's grammar turns into a function call are named after the
    // function it becomes, which for `TRIM` depends on the side asked for.
    #[test]
    fn labels_the_forms_postgres_parses_as_a_call() {
        for (sql, expected) in [
            (
                "SELECT EXTRACT(YEAR FROM d) FROM t",
                "SELECT EXTRACT(YEAR FROM d) AS extract FROM t",
            ),
            (
                "SELECT SUBSTRING(s FROM 1 FOR 2) FROM t",
                "SELECT SUBSTRING(s FROM 1 FOR 2) AS substring FROM t",
            ),
            (
                "SELECT POSITION('a' IN s) FROM t",
                "SELECT POSITION('a' IN s) AS position FROM t",
            ),
            ("SELECT TRIM(s) FROM t", "SELECT TRIM(s) AS btrim FROM t"),
            (
                "SELECT TRIM(LEADING ' ' FROM s) FROM t",
                "SELECT TRIM(LEADING ' ' FROM s) AS ltrim FROM t",
            ),
            (
                "SELECT TRIM(TRAILING ' ' FROM s) FROM t",
                "SELECT TRIM(TRAILING ' ' FROM s) AS rtrim FROM t",
            ),
            ("SELECT CEIL(x) FROM t", "SELECT CEIL(x) AS ceil FROM t"),
            (
                "SELECT ts AT TIME ZONE 'UTC' FROM t",
                "SELECT ts AT TIME ZONE 'UTC' AS timezone FROM t",
            ),
            (
                "SELECT INTERVAL '1 day'",
                "SELECT INTERVAL '1 day' AS interval",
            ),
        ] {
            assert_eq!(labelled(sql), expected, "`{sql}`");
        }
    }

    // `CASE` is `case` unless its `ELSE` result has a name of its own, and an array or a
    // row constructor is named after the constructor.
    #[test]
    fn labels_the_constructors_postgres_names() {
        assert_eq!(
            labelled("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t"),
            "SELECT CASE WHEN a THEN 1 ELSE 2 END AS case FROM t"
        );
        assert_eq!(
            labelled("SELECT CASE WHEN a THEN 1 ELSE b END FROM t"),
            "SELECT CASE WHEN a THEN 1 ELSE b END AS b FROM t"
        );
        assert_eq!(
            labelled("SELECT CASE WHEN a THEN 1 END FROM t"),
            "SELECT CASE WHEN a THEN 1 END AS case FROM t"
        );
        assert_eq!(
            labelled("SELECT ARRAY[1, 2]"),
            "SELECT ARRAY[1, 2] AS array"
        );
    }

    // A scalar subquery is named after the column it selects, and left alone where that
    // column cannot be named from the AST.
    #[test]
    fn labels_a_scalar_subquery_after_its_own_column() {
        assert_eq!(
            labelled("SELECT (SELECT max(x) FROM t)"),
            "SELECT (SELECT max(x) AS max FROM t) AS max"
        );
        assert_eq!(
            labelled("SELECT (SELECT 1)"),
            "SELECT (SELECT 1 AS \"?column?\") AS \"?column?\""
        );
        let a_wildcard = "SELECT (SELECT * FROM t)";
        assert_eq!(labelled(a_wildcard), a_wildcard);
    }

    // A plain column is left alone: DataFusion already names it the way PostgreSQL does,
    // and an alias would drop the qualifier the rest of the plan reads.
    #[test]
    fn leaves_a_plain_column_alone() {
        for sql in [
            "SELECT * FROM t",
            "SELECT a, b FROM t WHERE a > 1 ORDER BY b",
            "SELECT t.a FROM t",
            "SELECT (a) FROM t",
        ] {
            assert_eq!(labelled(sql), sql, "`{sql}`");
        }
    }

    // Planned, because the whole reason the anonymous label is numbered is a rule of
    // DataFusion's: three columns all called `?column?` would be refused outright. The plan
    // holds the numbering and the client is told PostgreSQL's name three times.
    #[tokio::test]
    async fn several_anonymous_columns_plan_and_go_out_as_one_name() {
        let (planned, wire) = planned_and_wire_labels("SELECT a + 1, b * 2, 3 FROM t").await;
        assert_eq!(
            planned,
            [
                "?column?".to_string(),
                format!("?column?{DISAMBIGUATOR}2"),
                format!("?column?{DISAMBIGUATOR}3"),
            ]
        );
        assert_eq!(wire, ["?column?", "?column?", "?column?"]);
    }

    // A single one needs no numbering, so nothing is collapsed and the plan already carries
    // what the client is told.
    #[tokio::test]
    async fn one_anonymous_column_is_named_in_the_plan_itself() {
        let (planned, wire) = planned_and_wire_labels("SELECT a + 1 FROM t").await;
        assert_eq!(planned, ["?column?"]);
        assert_eq!(wire, ["?column?"]);
    }

    // A bare name is never numbered: it goes into the plan as itself and out unchanged.
    #[tokio::test]
    async fn a_named_column_is_untouched_by_the_collapse() {
        let (planned, wire) = planned_and_wire_labels("SELECT sum(a), count(*) FROM t").await;
        assert_eq!(planned, ["sum", "count"]);
        assert_eq!(wire, ["sum", "count"]);
    }

    // The headline, planned: PostgreSQL returns both of these as `sum`, DataFusion refuses a
    // projection that holds `sum` twice, and both facts hold at once.
    #[tokio::test]
    async fn a_repeated_bare_label_plans_and_goes_out_twice_under_one_name() {
        let (planned, wire) = planned_and_wire_labels("SELECT sum(a), sum(b) FROM t").await;
        assert_eq!(planned, ["sum".to_string(), format!("sum{DISAMBIGUATOR}2")]);
        assert_eq!(wire, ["sum", "sum"]);
    }

    // A label that collides with a name the projection already holds, planned: a client's
    // alias is the client's, and the label beside it is still PostgreSQL's.
    #[tokio::test]
    async fn a_label_colliding_with_a_clients_alias_plans() {
        let (planned, wire) = planned_and_wire_labels("SELECT sum(a) AS sum, sum(b) FROM t").await;
        assert_eq!(planned, ["sum".to_string(), format!("sum{DISAMBIGUATOR}2")]);
        assert_eq!(wire, ["sum", "sum"]);
    }

    // An anonymous column beside a named one, so the numbering is proved to count each name
    // on its own.
    #[tokio::test]
    async fn the_numbering_counts_each_name_separately() {
        let (planned, wire) = planned_and_wire_labels("SELECT abs(a), a + 1, b + 1 FROM t").await;
        assert_eq!(
            planned,
            [
                "abs".to_string(),
                "?column?".to_string(),
                format!("?column?{DISAMBIGUATOR}2"),
            ]
        );
        assert_eq!(wire, ["abs", "?column?", "?column?"]);
    }

    // The wildcard case, planned: the suffix costs nothing where the wildcard turns out not
    // to contain the name, because the client is told the label either way.
    #[tokio::test]
    async fn a_column_label_beside_a_wildcard_plans() {
        let (planned, wire) = planned_and_wire_labels("SELECT *, abs(a) FROM t").await;
        assert_eq!(planned, ["a", "b", "abs"]);
        assert_eq!(wire, ["a", "b", "abs"]);
    }
}
