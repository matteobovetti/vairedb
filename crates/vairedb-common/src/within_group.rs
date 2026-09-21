//! PostgreSQL's remaining `WITHIN GROUP` aggregates: `mode()` and the hypothetical-set
//! family `rank`, `dense_rank`, `percent_rank` and `cume_dist`.
//!
//! ```text
//! t(n) = 1 … 10
//!
//! SELECT mode() WITHIN GROUP (ORDER BY g) FROM t
//! SELECT rank(5) WITHIN GROUP (ORDER BY n) FROM t
//!
//! PostgreSQL  answers
//! VaireDB     ERROR:  Invalid function 'mode'. Did you mean 'md5'?     -- before this module
//!             ERROR:  Invalid function 'rank'. Did you mean 'rand'?
//! ```
//!
//! DataFusion has neither name in its **aggregate** namespace — `rank` and its three
//! siblings exist only as window functions, which is a different lookup — so a reporting
//! query that asks for the most frequent value, or for where a hypothetical value would land
//! in a distribution, is refused at planning with a spelling suggestion that means something
//! else. There is no rewrite that reaches these: they are functions, so what is missing is
//! the function. This module is the five of them, as UDAFs.
//!
//! ## What each one answers
//!
//! `mode()` is the most frequent non-null value of the ordered column. PostgreSQL documents
//! the tie as arbitrary and resolves it in practice by taking the first of the tied values in
//! the `WITHIN GROUP` sort order, which is what [`ModeAccumulator`] does — so `ORDER BY x`
//! and `ORDER BY x DESC` can legitimately disagree on a tie, and each agrees with
//! PostgreSQL.
//!
//! The other four answer where a **hypothetical** row, whose ordered value is the direct
//! argument, would land if it were added to the group. With `n` rows in the group, `before`
//! rows sorting strictly ahead of the hypothetical one, `distinct` distinct values among
//! those, and `peers` rows equal to it:
//!
//! ```text
//! rank         = before + 1
//! dense_rank   = distinct + 1
//! percent_rank = before / n                       -- 0 for an empty group
//! cume_dist    = (before + peers + 1) / (n + 1)
//! ```
//!
//! Those are the window functions of the same names evaluated on the group *plus* the
//! hypothetical row, which is why the denominators count `n + 1` rows while the numerators
//! count the real ones. Nulls in the ordered column are **not** skipped: they take the
//! position the `ORDER BY` gives them (`NULLS LAST` by default, as in PostgreSQL) and they
//! count towards `n`, because a window function would see them too.
//!
//! ## Sorting, and why the comparison is a row encoding
//!
//! "Sorts strictly ahead of" has to mean exactly what the `WITHIN GROUP ORDER BY` means,
//! including `DESC` and the placement of nulls, and it has to compare the hypothetical value
//! against the column's values under that same rule. Arrow's row format is precisely that
//! rule made comparable: [`RowConverter`] built from the clause's [`SortOptions`] encodes
//! each value into bytes whose ordering *is* the sort order and whose equality is value
//! equality. So the whole of the ordering question — descending, nulls, and the tie that
//! `dense_rank` de-duplicates — is one `cmp` per row against one encoded hypothetical row,
//! with no per-type comparison logic of this module's own to be wrong about.
//!
//! ## Why four of the five are registered under another name
//!
//! `rank`, `dense_rank`, `percent_rank` and `cume_dist` are also **window** function names,
//! and datafusion-sql's `find_window_func` resolves `OVER` by looking in the *aggregate*
//! registry first — an aggregate of that name wins over the built-in window function, for
//! every name but a hardcoded `first_value`/`last_value`/`nth_value`. So registering an
//! aggregate called `rank` takes `rank() OVER (ORDER BY n)` away, measured as
//! `'rank' does not support zero arguments … Candidate functions: rank(Any, Any)`. Trading a
//! window function every reporting query uses for an ordered-set aggregate few do is not a
//! trade worth making, and the priority is upstream's.
//!
//! So the four are registered as `vaire_hypothetical_<name>` and the read path renames the
//! call: [`hypothetical_set_udaf`] is the mapping, and
//! `pg_operators::rewrite_pg_expressions` applies it to exactly the calls that carry a
//! `WITHIN GROUP` clause and no `OVER`. That is the only spelling PostgreSQL has for the
//! aggregate, and it is a spelling the window function cannot wear, so the rename separates
//! the two namespaces the planner conflates. `mode` needs none of this: no window function
//! has that name.
//!
//! ## What is refused, and why each one is
//!
//! * **More than one ordered column.** `rank(a, b) WITHIN GROUP (ORDER BY x, y)` is legal
//!   PostgreSQL and is refused *upstream of this module*: datafusion-sql answers
//!   `Only a single ordering expression is permitted in a WITHIN GROUP clause` before any
//!   UDAF is consulted, so there is no signature this crate could offer that would be
//!   reached. The one-column form is the whole of what is implementable here.
//! * **A non-literal direct argument.** PostgreSQL evaluates the direct arguments once per
//!   group, so a grouped column is a legal hypothetical value there. Here the value is read
//!   off the physical literal, the way [`crate::udaf`]'s percentile fraction is, and anything
//!   else is refused rather than silently read from the first row.
//! * **`DISTINCT`.** PostgreSQL has no `DISTINCT` in an ordered-set aggregate, and
//!   de-duplicating the group would change `n` and every answer that divides by it.
//! * **No `WITHIN GROUP` at all.** `mode(x)` and `rank(x, y)` parse as plain aggregates in
//!   DataFusion, but without the clause there is no sort order, and every answer here is
//!   defined by one. Refused, naming the spelling.
//! * An ordered column whose type Arrow's row format cannot encode. Refused naming the type,
//!   rather than answering under some other order.
//!
//! ## State, and the wire
//!
//! Exact answers need the whole group, so the intermediate state is the values themselves as
//! a list — the same shape [`crate::udaf`] uses, and for the same reason: it is what lets the
//! final aggregate merge the partials one per shard. The hypothetical value is not part of
//! the state because it is a literal every partial already has.
//!
//! Every refusal that can only be reached on an executor is wrapped in [`tagged_message`], so
//! its SQLSTATE survives the Ballista scheduler rendering the error to text; without it a
//! client is told `XX000` and that a retry might help, and none of these will ever succeed on
//! a retry. And, as with every function resolved by name across the wire, these must be
//! registered on every context that plans **or** executes a read.

use std::any::Any;
use std::collections::HashSet;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, ListArray, new_empty_array};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, cast, concat, sort_to_indices};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{Result, ScalarValue, internal_err, not_impl_err, plan_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;

use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// Register `mode` and the four hypothetical-set aggregates on `registry`.
///
/// The four go in under `vaire_hypothetical_<name>`, not under PostgreSQL's names, so that
/// `rank() OVER (…)` keeps resolving to the window function — see the module doc, and
/// [`hypothetical_set_udaf`] for the rename that makes the aggregate reachable.
///
/// Call this on every context that plans **or** executes a read; see the module doc.
pub fn register_within_group_aggregates(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udaf(Arc::new(AggregateUDF::from(Mode::new())))?;
    for kind in Hypothetical::ALL {
        registry.register_udaf(Arc::new(AggregateUDF::from(HypotheticalSet::new(kind))))?;
    }
    Ok(())
}

/// The aggregate that answers PostgreSQL's `name(…) WITHIN GROUP (ORDER BY …)`, for the four
/// hypothetical-set names — and `None` for anything else, including `mode`, which needs no
/// rename.
///
/// The read path calls this on a function whose `WITHIN GROUP` clause is not empty and whose
/// `OVER` is absent. Both conditions matter: with `OVER` the same name is the window function
/// this rename exists to protect, and without either the name is not an aggregate call at all.
pub fn hypothetical_set_udaf(postgres_name: &str) -> Option<&'static str> {
    Hypothetical::ALL
        .into_iter()
        .find(|kind| postgres_name.eq_ignore_ascii_case(kind.postgres_name()))
        .map(Hypothetical::udaf_name)
}

/// `mode() WITHIN GROUP (ORDER BY expr)` — the most frequent value of `expr`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Mode {
    signature: Signature,
}

impl Default for Mode {
    fn default() -> Self {
        Self::new()
    }
}

impl Mode {
    pub fn new() -> Self {
        Self {
            // No direct arguments: the one argument is the ordered value the `WITHIN GROUP`
            // clause supplies, of any type the row format can sort.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Mode {
    fn name(&self) -> &str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // A value of the input, so the input's type — PostgreSQL's `anyelement`.
        match arg_types.first() {
            Some(ordered) => Ok(ordered.clone()),
            None => internal_err!("mode was called without an ordered value"),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordered = ordered_column(self.name(), &args)?;
        Ok(Box::new(ModeAccumulator {
            options: sort_options(self.name(), &args)?,
            group: Grouped::new(ordered),
        }))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// Which hypothetical-set aggregate, i.e. which of the four answers is read off the same
/// three counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Hypothetical {
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
}

impl Hypothetical {
    const ALL: [Self; 4] = [
        Self::Rank,
        Self::DenseRank,
        Self::PercentRank,
        Self::CumeDist,
    ];

    /// What the client writes, which is what every message names.
    fn postgres_name(self) -> &'static str {
        match self {
            Self::Rank => "rank",
            Self::DenseRank => "dense_rank",
            Self::PercentRank => "percent_rank",
            Self::CumeDist => "cume_dist",
        }
    }

    /// What the aggregate is registered as, and so what crosses the wire — deliberately not
    /// the PostgreSQL name, so the window function of that name survives. See the module doc.
    fn udaf_name(self) -> &'static str {
        match self {
            Self::Rank => "vaire_hypothetical_rank",
            Self::DenseRank => "vaire_hypothetical_dense_rank",
            Self::PercentRank => "vaire_hypothetical_percent_rank",
            Self::CumeDist => "vaire_hypothetical_cume_dist",
        }
    }

    /// `bigint` for the two that count rows, `double precision` for the two that divide.
    fn return_type(self) -> DataType {
        match self {
            Self::Rank | Self::DenseRank => DataType::Int64,
            Self::PercentRank | Self::CumeDist => DataType::Float64,
        }
    }

    /// The answer, from the counts the module doc names.
    fn answer(self, counts: Counts) -> ScalarValue {
        let Counts {
            rows,
            before,
            distinct,
            peers,
        } = counts;
        match self {
            Self::Rank => ScalarValue::Int64(Some(before as i64 + 1)),
            Self::DenseRank => ScalarValue::Int64(Some(distinct as i64 + 1)),
            Self::PercentRank => ScalarValue::Float64(Some(match rows {
                // The hypothetical row is the only row, so it is at the very front.
                0 => 0.0,
                rows => before as f64 / rows as f64,
            })),
            Self::CumeDist => {
                ScalarValue::Float64(Some((before + peers + 1) as f64 / (rows + 1) as f64))
            }
        }
    }
}

/// Where the group stands relative to the hypothetical row, in the `WITHIN GROUP` order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Counts {
    /// Rows in the group, nulls included and the hypothetical row excluded.
    rows: usize,
    /// Rows sorting strictly ahead of the hypothetical one.
    before: usize,
    /// Distinct values among those.
    distinct: usize,
    /// Rows equal to the hypothetical value.
    peers: usize,
}

/// One of `rank`, `dense_rank`, `percent_rank`, `cume_dist` as
/// `f(value) WITHIN GROUP (ORDER BY expr)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HypotheticalSet {
    kind: Hypothetical,
    signature: Signature,
}

impl HypotheticalSet {
    fn new(kind: Hypothetical) -> Self {
        Self {
            kind,
            // The ordered value and the hypothetical one, neither coerced by the signature:
            // the two are reconciled in `evaluate` by `comparison_coercion`, which is the
            // rule `=` and `<` use, so `rank(5) WITHIN GROUP (ORDER BY a_float)` compares the
            // way the same two values would compare anywhere else.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for HypotheticalSet {
    fn name(&self) -> &str {
        self.kind.udaf_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.return_type())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // The client-facing name for every message: what it wrote is `rank`, whatever the
        // read path renamed the call to.
        let spelled = self.kind.postgres_name();
        let ordered = ordered_column(spelled, &args)?;
        let options = sort_options(spelled, &args)?;
        let hypothetical = hypothetical_argument(args.exprs, spelled)?;
        Ok(Box::new(HypotheticalAccumulator {
            kind: self.kind,
            hypothetical,
            options,
            group: Grouped::new(ordered),
        }))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }
}

/// The intermediate state of every aggregate here: every value seen, as a list.
///
/// None of these can be summarised into anything smaller — the most frequent value and the
/// position of a hypothetical row are both properties of the whole distribution — so the
/// state is the values themselves, which is what lets the final aggregate merge the partials
/// one per shard.
fn state_fields(function: &str, args: &StateFieldsArgs) -> Result<Vec<FieldRef>> {
    let Some(ordered) = args.input_fields.first() else {
        return internal_err!("{function} was called without an ordered value");
    };
    let element = Field::new_list_field(ordered.data_type().clone(), true);
    Ok(vec![
        Field::new(
            format!("{}[{function}]", args.name),
            DataType::List(Arc::new(element)),
            true,
        )
        .into(),
    ])
}

/// The ordered column's type, which is argument 0 whether or not there are direct arguments.
fn ordered_column(function: &str, args: &AccumulatorArgs) -> Result<DataType> {
    if args.is_distinct {
        return not_impl_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "DISTINCT is not supported for {function}: PostgreSQL has no DISTINCT in \
                     an ordered-set aggregate, and removing duplicates would change the group \
                     the answer describes"
                )
            )
        );
    }
    match args.expr_fields.first() {
        Some(ordered) => Ok(ordered.data_type().clone()),
        None => internal_err!("{function} was called without an ordered value"),
    }
}

/// The `WITHIN GROUP ORDER BY`'s sort options, refusing the spelling that has none.
///
/// Every answer in this module is defined by an order, so an aggregate call without the
/// clause is not a weaker version of one with it — it is a different function, which
/// PostgreSQL does not have.
fn sort_options(function: &str, args: &AccumulatorArgs) -> Result<SortOptions> {
    match args.order_bys.first() {
        Some(sort) => Ok(sort.options),
        None => plan_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "{function} is an ordered-set aggregate and requires a WITHIN GROUP \
                     clause: spell it {function}(…) WITHIN GROUP (ORDER BY expr)"
                )
            )
        ),
    }
}

/// Read the hypothetical value out of the aggregate's second argument.
///
/// It has to be a literal: the value is fixed for the whole group, so there is no row to
/// evaluate an expression against. Read leniently — `parse_float_as_decimal` makes `2.5`
/// arrive as `numeric` — and reconciled with the ordered column's type in `evaluate`.
fn hypothetical_argument(args: &[Arc<dyn PhysicalExpr>], function: &str) -> Result<ScalarValue> {
    let Some(argument) = args.get(1) else {
        return plan_err!("{function} requires a hypothetical value to rank");
    };
    // Upcast rather than call `as_any()`: Arrow's `Array` is in scope in this module and owns
    // that method name too.
    let argument: &dyn Any = argument.as_ref();
    match argument.downcast_ref::<Literal>() {
        Some(literal) => Ok(literal.value().clone()),
        None => plan_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!(
                    "the hypothetical value for {function} must be a literal: it is evaluated \
                     once for the whole group, not once per row"
                )
            )
        ),
    }
}

/// The group's ordered values, gathered batch by batch and concatenated once.
#[derive(Debug)]
struct Grouped {
    /// The ordered column's type, needed to describe an empty group's state.
    ordered_type: DataType,
    /// One entry per batch seen, concatenated only when the answer is needed.
    values: Vec<ArrayRef>,
}

impl Grouped {
    fn new(ordered_type: DataType) -> Self {
        Self {
            ordered_type,
            values: Vec::new(),
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef], function: &str) -> Result<()> {
        let Some(ordered) = values.first() else {
            return internal_err!("{function} needs an ordered value to accumulate");
        };
        if !ordered.is_empty() {
            self.values.push(Arc::clone(ordered));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef], function: &str) -> Result<()> {
        let Some(state) = states.first() else {
            return internal_err!("{function} needs its own state to merge");
        };
        for partial in state.as_list::<i32>().iter().flatten() {
            if !partial.is_empty() {
                self.values.push(partial);
            }
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let values = self.accumulated()?;
        let element = Field::new_list_field(values.data_type().clone(), true);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32]));
        let list = ListArray::new(Arc::new(element), offsets, values, None);
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    /// Every value seen so far, in one array.
    fn accumulated(&self) -> Result<ArrayRef> {
        match self.values.len() {
            0 => Ok(new_empty_array(&self.ordered_type)),
            1 => Ok(Arc::clone(&self.values[0])),
            _ => {
                let parts: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();
                Ok(concat(&parts)?)
            }
        }
    }

    fn size(&self) -> usize {
        self.values
            .iter()
            .map(|values| values.get_array_memory_size())
            .sum()
    }
}

/// Accumulates the group's values and answers the most frequent one once it has them all.
#[derive(Debug)]
struct ModeAccumulator {
    options: SortOptions,
    group: Grouped,
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.group.update_batch(values, "mode")
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.group.merge_batch(states, "mode")
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.group.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        mode_of(&self.group.accumulated()?, self.options.descending)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.group.size()
    }
}

/// Accumulates the group's values and answers where the hypothetical row lands.
#[derive(Debug)]
struct HypotheticalAccumulator {
    kind: Hypothetical,
    hypothetical: ScalarValue,
    options: SortOptions,
    group: Grouped,
}

impl Accumulator for HypotheticalAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.group.update_batch(values, self.kind.postgres_name())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.group.merge_batch(states, self.kind.postgres_name())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.group.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let counts = counts_against(
            &self.group.accumulated()?,
            &self.hypothetical,
            self.options,
            self.kind.postgres_name(),
        )?;
        Ok(self.kind.answer(counts))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.group.size()
    }
}

/// The most frequent value of `values`, ignoring nulls the way every aggregate does.
///
/// Kept separate from the accumulator so the rule that has to match PostgreSQL — including
/// which of two equally frequent values wins — can be tested on an array alone.
fn mode_of(values: &ArrayRef, descending: bool) -> Result<ScalarValue> {
    // Nulls are not candidates, so `sort_to_indices` is asked to park them past the end and
    // only the rows before that are read.
    let rows = values.len() - values.null_count();
    if rows == 0 {
        // An empty group is a null, not an error — PostgreSQL's `mode()` over no rows.
        return ScalarValue::try_from(values.data_type());
    }
    let order = sort_to_indices(
        values,
        Some(SortOptions {
            descending,
            nulls_first: false,
        }),
        None,
    )?;

    // Sorted, equal values are adjacent, so the frequencies are run lengths. `>` and not
    // `>=` is the tie rule: the first run to reach a given length keeps the answer, and the
    // first run is the first value in the clause's own sort order — which is how PostgreSQL
    // resolves the tie it documents as arbitrary.
    let mut best: Option<(usize, ScalarValue)> = None;
    let mut run: Option<(usize, ScalarValue)> = None;
    for rank in 0..rows {
        let value = ScalarValue::try_from_array(values, order.value(rank) as usize)?;
        run = Some(match run {
            Some((length, previous)) if previous == value => (length + 1, previous),
            _ => (1, value),
        });
        let Some((length, value)) = &run else {
            return internal_err!("mode lost the run it just counted");
        };
        if best.as_ref().map(|(best, _)| length > best).unwrap_or(true) {
            best = Some((*length, value.clone()));
        }
    }
    match best {
        Some((_, value)) => Ok(value),
        None => internal_err!("mode found no value in a group that has {rows} of them"),
    }
}

/// Where `hypothetical` lands among `values` under `options`.
///
/// Kept separate from the accumulator so the ordering rules — nulls, `DESC`, and the ties
/// `dense_rank` de-duplicates — can be tested on an array alone.
fn counts_against(
    values: &ArrayRef,
    hypothetical: &ScalarValue,
    options: SortOptions,
    function: &str,
) -> Result<Counts> {
    // The two sides are compared under `=`'s own coercion rule, so an `int` hypothetical
    // value against a `float8` column compares as the two would anywhere else rather than
    // being truncated to fit.
    let compare_at = comparison_coercion(values.data_type(), &hypothetical.data_type())
        .ok_or_else(|| {
            tagged_error(
                VdbErrorCode::TypeMismatch,
                format!(
                    "{function} cannot compare a hypothetical {} against an ordered column of \
                     type {}",
                    hypothetical.data_type(),
                    values.data_type()
                ),
            )
        })?;
    let values = cast(values, &compare_at)?;
    let hypothetical = hypothetical.cast_to(&compare_at)?.to_array()?;

    // The row encoding *is* the sort order the clause asked for: byte order equals value
    // order under these options, and equal bytes mean equal values. So one `cmp` per row
    // answers all three counts, for every type the format supports.
    let field = SortField::new_with_options(compare_at.clone(), options);
    let converter = RowConverter::new(vec![field]).map_err(|e| {
        tagged_error(
            VdbErrorCode::FeatureNotSupported,
            format!("{function} cannot order a column of type {compare_at}: {e}"),
        )
    })?;
    let rows = converter.convert_columns(&[values])?;
    let hypothetical = converter.convert_columns(&[hypothetical])?;
    let hypothetical = hypothetical.row(0);

    let mut counts = Counts {
        rows: rows.num_rows(),
        before: 0,
        distinct: 0,
        peers: 0,
    };
    let mut ahead: HashSet<Vec<u8>> = HashSet::new();
    for index in 0..rows.num_rows() {
        let row = rows.row(index);
        match row.cmp(&hypothetical) {
            std::cmp::Ordering::Less => {
                counts.before += 1;
                ahead.insert(row.as_ref().to_vec());
            }
            std::cmp::Ordering::Equal => counts.peers += 1,
            std::cmp::Ordering::Greater => {}
        }
    }
    counts.distinct = ahead.len();
    Ok(counts)
}

/// A `DataFusionError` whose SQLSTATE survives being rendered to text by the scheduler.
fn tagged_error(code: VdbErrorCode, message: String) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(tagged_message(code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Float64Array, Int64Array, StringArray};

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn text(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn ascending() -> SortOptions {
        // What `ORDER BY x` means in PostgreSQL, and what DataFusion's planner builds for it.
        SortOptions {
            descending: false,
            nulls_first: false,
        }
    }

    fn descending() -> SortOptions {
        SortOptions {
            descending: true,
            nulls_first: true,
        }
    }

    fn counts(values: &ArrayRef, hypothetical: ScalarValue, options: SortOptions) -> Counts {
        counts_against(values, &hypothetical, options, "rank").unwrap()
    }

    fn answer(
        kind: Hypothetical,
        values: &ArrayRef,
        hypothetical: ScalarValue,
        options: SortOptions,
    ) -> ScalarValue {
        kind.answer(counts(values, hypothetical, options))
    }

    /// `SELECT rank(5) WITHIN GROUP (ORDER BY n) FROM (1 … 10)` — PostgreSQL answers 5,
    /// because four rows sort ahead of the hypothetical one.
    #[test]
    fn the_rank_counts_the_rows_ahead_of_the_hypothetical_one() {
        let values = ints(&(1..=10).map(Some).collect::<Vec<_>>());
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(5_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(5))
        );
        // Ahead of everything, and behind everything.
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(0_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(99_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(11))
        );
    }

    /// A value equal to some rows does not count them: `rank` is 1 + the rows *strictly*
    /// ahead, so the hypothetical row ties with its peers rather than following them.
    #[test]
    fn peers_do_not_count_towards_the_rank() {
        let values = ints(&[Some(1), Some(5), Some(5), Some(5), Some(9)]);
        let c = counts(&values, ScalarValue::from(5_i64), ascending());
        assert_eq!(
            c,
            Counts {
                rows: 5,
                before: 1,
                distinct: 1,
                peers: 3
            }
        );
        assert_eq!(
            Hypothetical::Rank.answer(c),
            ScalarValue::Int64(Some(2)),
            "one row ahead, so rank 2"
        );
    }

    /// `dense_rank` counts distinct values ahead, so repeats collapse.
    #[test]
    fn the_dense_rank_counts_distinct_values_ahead() {
        let values = ints(&[Some(1), Some(1), Some(1), Some(2), Some(9)]);
        assert_eq!(
            answer(
                Hypothetical::DenseRank,
                &values,
                ScalarValue::from(5_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(3)),
            "1 and 2 are ahead, so the third dense rank"
        );
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(5_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(5)),
            "four rows are ahead, so the fifth rank"
        );
    }

    /// `percent_rank` divides by the real rows and `cume_dist` by them plus the hypothetical
    /// one, which is what the two window functions do to a group of `n + 1` rows.
    #[test]
    fn the_two_fractions_use_the_denominators_postgresql_uses() {
        let values = ints(&(1..=4).map(Some).collect::<Vec<_>>());
        // 5 sorts behind all four rows: rank 5 of 5, so percent_rank 1.
        assert_eq!(
            answer(
                Hypothetical::PercentRank,
                &values,
                ScalarValue::from(5_i64),
                ascending()
            ),
            ScalarValue::Float64(Some(1.0))
        );
        assert_eq!(
            answer(
                Hypothetical::CumeDist,
                &values,
                ScalarValue::from(5_i64),
                ascending()
            ),
            ScalarValue::Float64(Some(1.0))
        );
        // 3 has two rows ahead of it and one peer, so of the five rows it and its peer are
        // the third and fourth: 2/4, and (2 + 1 + 1)/5.
        assert_eq!(
            answer(
                Hypothetical::PercentRank,
                &values,
                ScalarValue::from(3_i64),
                ascending()
            ),
            ScalarValue::Float64(Some(0.5))
        );
        assert_eq!(
            answer(
                Hypothetical::CumeDist,
                &values,
                ScalarValue::from(3_i64),
                ascending()
            ),
            ScalarValue::Float64(Some(0.8))
        );
    }

    /// An empty group: the hypothetical row is the only row, so it is first, alone, and its
    /// cumulative distribution is the whole of it. `percent_rank` must not divide by zero.
    #[test]
    fn an_empty_group_is_the_hypothetical_row_alone() {
        let values = ints(&[]);
        for (kind, expected) in [
            (Hypothetical::Rank, ScalarValue::Int64(Some(1))),
            (Hypothetical::DenseRank, ScalarValue::Int64(Some(1))),
            (Hypothetical::PercentRank, ScalarValue::Float64(Some(0.0))),
            (Hypothetical::CumeDist, ScalarValue::Float64(Some(1.0))),
        ] {
            assert_eq!(
                answer(kind, &values, ScalarValue::from(5_i64), ascending()),
                expected,
                "{}",
                kind.postgres_name()
            );
        }
    }

    /// `DESC` reverses which rows are ahead, because the comparison is the clause's order and
    /// not the type's.
    #[test]
    fn a_descending_clause_counts_from_the_other_end() {
        let values = ints(&(1..=10).map(Some).collect::<Vec<_>>());
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(5_i64),
                descending()
            ),
            ScalarValue::Int64(Some(6)),
            "6 … 10 sort ahead of 5 when the order is descending"
        );
    }

    /// Nulls take the position the clause gives them and count towards the group, because a
    /// window function over the same rows would see them.
    #[test]
    fn nulls_are_ordered_rather_than_ignored() {
        let values = ints(&[Some(1), Some(2), None, None]);
        // `NULLS LAST`: the nulls sort behind the hypothetical 3.
        assert_eq!(
            counts(&values, ScalarValue::from(3_i64), ascending()),
            Counts {
                rows: 4,
                before: 2,
                distinct: 2,
                peers: 0
            }
        );
        // `NULLS FIRST`, which is what `ORDER BY … DESC` means: they sort ahead of it. Both
        // real values sort ahead too, descending, since 3 is above them.
        assert_eq!(
            counts(&values, ScalarValue::from(3_i64), descending()),
            Counts {
                rows: 4,
                before: 2,
                distinct: 1,
                peers: 0
            }
        );
    }

    /// A NULL hypothetical value is a position and not an unknown: PostgreSQL sorts it where
    /// the clause says, so it lands past every real value under `NULLS LAST`.
    #[test]
    fn a_null_hypothetical_value_takes_its_sort_position() {
        let values = ints(&[Some(1), Some(2), Some(3)]);
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::Int64(None),
                ascending()
            ),
            ScalarValue::Int64(Some(4))
        );
    }

    /// The hypothetical value and the column need not share a type: they are reconciled the
    /// way `=` reconciles them, so an integer literal against a `float8` column is compared
    /// as a number rather than truncated.
    #[test]
    fn the_hypothetical_value_is_coerced_the_way_a_comparison_would_coerce_it() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from(2_i64),
                ascending()
            ),
            ScalarValue::Int64(Some(2)),
            "only 1.5 is below 2"
        );
    }

    /// Text orders too, and `dense_rank` de-duplicates it.
    #[test]
    fn a_text_column_is_ordered_as_text() {
        let values = text(&["a", "b", "b", "z"]);
        assert_eq!(
            answer(
                Hypothetical::Rank,
                &values,
                ScalarValue::from("c"),
                ascending()
            ),
            ScalarValue::Int64(Some(4))
        );
        assert_eq!(
            answer(
                Hypothetical::DenseRank,
                &values,
                ScalarValue::from("c"),
                ascending()
            ),
            ScalarValue::Int64(Some(3))
        );
    }

    /// The mode is the most frequent value, not the largest or the first.
    #[test]
    fn the_mode_is_the_most_frequent_value() {
        let values = ints(&[Some(9), Some(1), Some(2), Some(2), Some(2), Some(3)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(2))
        );
    }

    /// Nulls are not candidates, however many of them there are — PostgreSQL's `mode()`
    /// ignores them like any other aggregate.
    #[test]
    fn nulls_are_never_the_mode() {
        let values = ints(&[None, None, None, Some(7)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(7))
        );
    }

    /// A group with no non-null value is a null, not an error.
    #[test]
    fn a_group_with_no_value_has_no_mode() {
        assert_eq!(
            mode_of(&ints(&[None, None]), false).unwrap(),
            ScalarValue::Int64(None)
        );
        assert_eq!(
            mode_of(&ints(&[]), false).unwrap(),
            ScalarValue::Int64(None)
        );
    }

    /// The tie is broken by the clause's own order, which is why `mode()` is a `WITHIN GROUP`
    /// aggregate at all: `ORDER BY x` and `ORDER BY x DESC` pick opposite ends of the tie,
    /// and PostgreSQL agrees with each.
    #[test]
    fn a_tie_is_broken_by_the_sort_order() {
        let values = ints(&[Some(1), Some(1), Some(5), Some(5)]);
        assert_eq!(
            mode_of(&values, false).unwrap(),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(mode_of(&values, true).unwrap(), ScalarValue::Int64(Some(5)));
    }

    /// The input order does not change the answer: it is a property of the group, and the
    /// accumulator sorts before reading it.
    #[test]
    fn the_input_order_does_not_change_the_mode() {
        let one = ints(&[Some(3), Some(1), Some(3), Some(2)]);
        let other = ints(&[Some(2), Some(3), Some(1), Some(3)]);
        assert_eq!(
            mode_of(&one, false).unwrap(),
            mode_of(&other, false).unwrap()
        );
        assert_eq!(mode_of(&one, false).unwrap(), ScalarValue::Int64(Some(3)));
    }

    /// Text has a mode too.
    #[test]
    fn the_mode_of_text_is_text() {
        let values = text(&["b", "a", "b"]);
        assert_eq!(mode_of(&values, false).unwrap(), ScalarValue::from("b"));
    }
}
