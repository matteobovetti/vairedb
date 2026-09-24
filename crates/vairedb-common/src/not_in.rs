//! PostgreSQL's three-valued `NOT IN`, asked of a candidate **list** instead of a join.
//!
//! `x NOT IN (q)` is `x <> q1 AND x <> q2 AND …` in PostgreSQL, so it is **NULL** — never
//! true — as soon as `x` is NULL or any candidate is NULL, and it is `true` for an *empty*
//! `q` whatever `x` is. A cluster cannot answer that with a join: only DataFusion's
//! `HashJoinExec` carries the `null_aware` flag those rules need, Ballista plans the anti
//! join as a sort-merge join, and turning the option on is unshippable
//! (`vairedb_coordinator::scheduler::scheduler::with_postgres_sql_options` records the
//! measurement). So the predicate is respelled before planning, into a shape a
//! *non*-null-aware anti join answers correctly.
//!
//! That respelling puts `x` inside a subquery, and there is one clause where `x` cannot go
//! there: a `HAVING` or `QUALIFY` predicate written over an **aggregate** or a window
//! function. `NOT EXISTS (… WHERE r.k = MAX(l.k))` is not a plan DataFusion has — it fails
//! with "Aggregate functions are not allowed in the WHERE clause" — so the aggregate has to
//! stay in the clause the client wrote it in, and the *candidates* have to come to it as a
//! value. `array_agg` over the subquery is that value, and this function is the comparison:
//!
//! ```sql
//! -- HAVING MAX(k) NOT IN (SELECT k FROM r)   becomes
//! HAVING vaire_not_in((SELECT array_agg(c) FROM (SELECT k FROM r) AS q (c)), MAX(k))
//! ```
//!
//! ## Why the whole rule is in here, and not spelled in SQL
//!
//! The respelling used everywhere else states the three NULL rules as three SQL conjuncts,
//! because each of them is something a join and an aggregate can answer. Once the
//! candidates are a list, that is no longer true: `array_has` is the only membership test
//! available and it is **two-valued**, so the NULL rules would still need the two extra
//! `count(*)` subqueries beside it — and `array_has` also refuses argument pairs the
//! comparison is defined for. `array_has(List(Int32), Decimal128)` and
//! `array_has(List(Int32), UInt64)` both fail to coerce, and a `HAVING` over
//! `MAX(numeric_col)` or a window `row_number()` is exactly how a client reaches them, so
//! that spelling would trade a wrong answer for a plan error naming a function the client
//! never wrote.
//!
//! Owning the comparison fixes both at once: the rule is stated once, as Rust, and the two
//! operands are coerced by [`comparison_coercion`] — the same rule the `=` this stands in
//! for would have used.
//!
//! ## The rule, measured against PostgreSQL 17
//!
//! | `x` | candidates | PostgreSQL | Why |
//! |---|---|---|---|
//! | `10` | `20, 99, 50` | `true` | no candidate equals it and none is NULL |
//! | `20` | `20, 99, 50` | `false` | a match makes it false |
//! | `10` | `20, NULL` | **NULL** | an unknown candidate may be the match |
//! | `20` | `20, NULL` | **`false`** | a match outranks the unknown — `AND` is false-dominant |
//! | `NULL` | `20, 99` | **NULL** | the compared value is unknown |
//! | `NULL` | *no rows* | **`true`** | an empty `AND` is true, even for a NULL `x` |
//!
//! The fourth row is why the match is looked for before the NULL is: `x <> 20 AND
//! x <> NULL` is `false AND NULL`, which is false and not NULL. The last row is why an
//! absent list is not an absent answer — `array_agg` over no rows is a NULL scalar, and
//! that NULL means "no candidates", which PostgreSQL reads as unconditionally true.
//!
//! ## Cost
//!
//! One `array_agg` materializes `q` into a single list value, where the respelling used in
//! a `WHERE` clause streams `q` through an anti join. That is the trade this shape pays to
//! exist at all, and it is bounded by the clause it is used in: a `HAVING` predicate is
//! evaluated once per *group*, and the list is a constant for the whole statement, so it is
//! built once and scanned per group rather than per row.

use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Scalar};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::columns::list_row;

/// The name the read path emits and every node resolves the call by.
///
/// Prefixed and not spelled as a PostgreSQL function name, for the reason
/// [`crate::float_div`] gives: nothing a client writes produces this name, so the call can
/// only ever be one VaireDB put there.
pub const NOT_IN_UDF_NAME: &str = "vaire_not_in";

/// What the candidate argument has to be, for the refusal when it is not: the rewrite wraps
/// an `array_agg`, so anything else means the call reaching the node is not the one the read
/// path built.
///
/// Spelled as one whole sentence, including the function's own name, because [`list_row`] is
/// handed the caller's wording rather than assembling it — and the name has to be in it for
/// the reason [`NOT_IN_UDF_NAME`] exists: a client who never wrote this call needs the
/// message to say which rewrite did.
const CANDIDATES_MUST_BE_A_LIST: &str = "vaire_not_in takes a list of candidates";

/// Register the list-valued `NOT IN` on `registry`.
///
/// Call this on every context that plans **or** executes a read. A scalar function crosses
/// the Ballista wire as a name with no definition attached, so one registered only where
/// the query is planned fails on the scheduler that decodes the logical plan and on the
/// executor that decodes the stage — see [`crate::pg_udf`], which is the same invariant
/// found the hard way.
pub fn register_not_in(registry: &mut dyn FunctionRegistry) -> Result<()> {
    registry.register_udf(not_in_udf())?;
    Ok(())
}

/// The shared [`ScalarUDF`] handle, built once and handed to every registry.
///
/// Private, because the read path builds the call as SQL text naming [`NOT_IN_UDF_NAME`] —
/// see `vairedb_coordinator::pgwire_handler::compat_rewrite` — so nothing outside this module
/// ever holds the handle.
fn not_in_udf() -> Arc<ScalarUDF> {
    static UDF: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
    Arc::clone(UDF.get_or_init(|| Arc::new(ScalarUDF::from(NotInList::new()))))
}

/// `vaire_not_in(candidates, element)` — PostgreSQL's `element NOT IN (candidates)`, with
/// the candidates given as a list rather than as a subquery.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NotInList {
    signature: Signature,
}

impl NotInList {
    fn new() -> Self {
        Self {
            // `any`, because the two operands are a list and a scalar of types that need
            // not match: `MAX(numeric_col) NOT IN (SELECT int_col …)` is an ordinary
            // PostgreSQL comparison, and a signature that asked the analyzer to make the
            // element the list's own type would reject it before this function ever sees
            // it. The pair is coerced here instead, by the same rule `=` uses.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NotInList {
    fn name(&self) -> &str {
        NOT_IN_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// A predicate, and a **nullable** one: three-valued is the whole point.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [candidates, element] = args.args.as_slice() else {
            return exec_err!(
                "{NOT_IN_UDF_NAME} takes a candidate list and an element, got {} arguments",
                args.args.len()
            );
        };
        let rows = args.number_rows;
        let element = element.to_array(rows)?;
        let answers = not_in(candidates, &element, rows)?;
        Ok(ColumnarValue::Array(Arc::new(answers)))
    }
}

/// PostgreSQL's `NOT IN` for every row of `element` against its candidates.
fn not_in(argument: &ColumnarValue, element: &ArrayRef, rows: usize) -> Result<BooleanArray> {
    let (candidates, element) = Candidates::coerce(argument, element)?;
    let mut answers = BooleanBufferBuilder::new(rows);
    let mut known = BooleanBufferBuilder::new(rows);
    for row in 0..rows {
        let answer = row_answer(candidates.at(row)?.as_ref(), &element, row)?;
        answers.append(answer.unwrap_or(false));
        known.append(answer.is_some());
    }
    Ok(BooleanArray::new(
        answers.finish(),
        Some(NullBuffer::new(known.finish())),
    ))
}

/// Where each row's candidate list comes from, in the type the comparison happens at.
///
/// Both operands are coerced once, when the variant is built, so the per-row loop only reads:
/// the shape the argument arrived in is decided here rather than tested again for every row.
enum Candidates {
    /// No candidates for any row, which is what a NULL candidate list means.
    Absent,
    /// One already-cast list that every row is compared against.
    Shared(ArrayRef),
    /// A list column, one list per row, cast a row at a time: only the row's own slice is
    /// ever needed, so the column is left as it arrived.
    PerRow {
        lists: ArrayRef,
        compare_at: DataType,
    },
}

impl Candidates {
    /// The candidate argument and the `element` column, both in the type the comparison
    /// happens at.
    fn coerce(argument: &ColumnarValue, element: &ArrayRef) -> Result<(Self, ArrayRef)> {
        match argument {
            ColumnarValue::Scalar(scalar) => Self::shared(&scalar.to_array_of_size(1)?, element),
            ColumnarValue::Array(lists) => Self::per_row(lists, element),
        }
    }

    /// One list for the whole batch, which is the shape the rewrite produces: the candidates
    /// come from an uncorrelated subquery, so every row is compared against the same list.
    ///
    /// Read out of the scalar once and kept as one value rather than expanded to one list per
    /// row, which `to_array(rows)` on a scalar list would do by copying the whole candidate
    /// set per row. It is cast once here for the same reason.
    fn shared(list: &ArrayRef, element: &ArrayRef) -> Result<(Self, ArrayRef)> {
        let Some(values) = candidates_at(list, 0)? else {
            // A NULL list is what `array_agg` over no rows produces, and it means "no
            // candidates" rather than "unknown candidates". There is nothing to compare, and
            // so no type to coerce a comparison at — the answer is the empty-conjunction
            // rule in [`row_answer`], which needs neither.
            return Ok((Self::Absent, Arc::clone(element)));
        };
        let compare_at = comparison_type(values.data_type(), element)?;
        let element = cast(element, &compare_at)?;
        Ok((Self::Shared(cast(&values, &compare_at)?), element))
    }

    /// A list per row, which is not what the rewrite builds but is what a client's own
    /// `array_agg` column arrives as.
    fn per_row(lists: &ArrayRef, element: &ArrayRef) -> Result<(Self, ArrayRef)> {
        let compare_at = comparison_type(list_value_type(lists.data_type())?, element)?;
        let element = cast(element, &compare_at)?;
        let lists = Arc::clone(lists);
        Ok((Self::PerRow { lists, compare_at }, element))
    }

    /// The candidates of one row, or `None` where that row has no candidate list at all.
    fn at(&self, row: usize) -> Result<Option<ArrayRef>> {
        match self {
            Self::Absent => Ok(None),
            Self::Shared(values) => Ok(Some(Arc::clone(values))),
            Self::PerRow { lists, compare_at } => Ok(match candidates_at(lists, row)? {
                Some(values) => Some(cast(&values, compare_at)?),
                None => None,
            }),
        }
    }
}

/// The type the `=` this stands in for would have compared at, so an `int4` candidate list
/// and a `numeric` element meet the way PostgreSQL makes them meet.
fn comparison_type(value_type: &DataType, element: &ArrayRef) -> Result<DataType> {
    comparison_coercion(value_type, element.data_type()).ok_or_else(|| {
        exec_datafusion_err!(
            "{NOT_IN_UDF_NAME} cannot compare {} against a list of {value_type}",
            element.data_type()
        )
    })
}

/// PostgreSQL's whole rule for one row, in the order the rule is defined in: `None` is SQL
/// NULL.
///
/// The order of the tests is the rule, not an implementation detail. `x NOT IN (20, NULL)`
/// for `x = 20` is `20 <> 20 AND 20 <> NULL` — `false AND NULL` — which is **false**, because
/// `AND` is false-dominant. So a match is looked for first and a NULL candidate only decides
/// the rows that did not match.
fn row_answer(
    candidates: Option<&ArrayRef>,
    element: &ArrayRef,
    row: usize,
) -> Result<Option<bool>> {
    // No candidates — an absent list or a list of no values — is an empty conjunction, which
    // is true whatever the element is, including a NULL element.
    let Some(candidates) = candidates.filter(|values| !values.is_empty()) else {
        return Ok(Some(true));
    };
    // A NULL element makes every comparison unknown, and there is at least one.
    if element.is_null(row) {
        return Ok(None);
    }
    let matches = eq(candidates, &Scalar::new(element.slice(row, 1)))?;
    if matches.true_count() > 0 {
        return Ok(Some(false));
    }
    if candidates.null_count() > 0 {
        return Ok(None);
    }
    Ok(Some(true))
}

/// The element type of a list type.
fn list_value_type(list: &DataType) -> Result<&DataType> {
    match list {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Ok(field.data_type()),
        other => exec_err!("{CANDIDATES_MUST_BE_A_LIST}, got {other}"),
    }
}

/// The candidates of one row, or `None` where that row's list is NULL.
fn candidates_at(list: &ArrayRef, row: usize) -> Result<Option<ArrayRef>> {
    // `array_agg` produces a `List`, which [`list_row`] reads along with the large variant. A
    // fixed-size list is read here as well, because a cast or a different aggregate could
    // produce one and its fixed width changes nothing about the rule.
    let values = match list.data_type() {
        DataType::FixedSizeList(_, _) => list.as_fixed_size_list().value(row),
        _ => list_row(list, row, CANDIDATES_MUST_BE_A_LIST)?,
    };
    Ok((!list.is_null(row)).then_some(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int32Array, ListArray, UInt64Array};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::execution::context::SessionContext;
    use datafusion::scalar::ScalarValue;

    /// An `int4` candidate list as the scalar the rewrite builds, where `None` is the NULL
    /// list `array_agg` over no rows produces.
    fn int_list(candidates: Option<Vec<Option<i32>>>) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::List(Arc::new(
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![candidates]),
        )))
    }

    /// `element NOT IN (candidates)` over whole `int4` columns, the way a physical expression
    /// invokes it.
    fn not_in_scalar_list(
        candidates: Option<Vec<Option<i32>>>,
        element: Vec<Option<i32>>,
    ) -> Result<Vec<Option<bool>>> {
        invoke(
            int_list(candidates),
            ColumnarValue::Array(Arc::new(Int32Array::from(element))),
        )
    }

    /// Invoke the function and collect its answers, nulls included.
    fn invoke(candidates: ColumnarValue, element: ColumnarValue) -> Result<Vec<Option<bool>>> {
        let rows = match &element {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let args = ScalarFunctionArgs {
            args: vec![candidates, element],
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("p", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        };
        let out = NotInList::new().invoke_with_args(args)?;
        let out = out.to_array(rows)?;
        Ok(out.as_boolean().iter().collect())
    }

    /// The ordinary two-valued case, which the anti join already answered correctly.
    #[test]
    fn a_value_not_among_the_candidates_is_true_and_a_match_is_false() {
        assert_eq!(
            not_in_scalar_list(
                Some(vec![Some(20), Some(99), Some(50)]),
                vec![Some(10), Some(20)]
            )
            .expect("should not raise"),
            vec![Some(true), Some(false)]
        );
    }

    /// The gap itself: a NULL among the candidates makes every non-matching row unknown,
    /// where the cluster's anti join answered `true` and returned the row.
    #[test]
    fn a_null_candidate_makes_a_non_matching_row_null() {
        assert_eq!(
            not_in_scalar_list(Some(vec![Some(20), None]), vec![Some(10)])
                .expect("should not raise"),
            vec![None]
        );
    }

    /// Measured against PostgreSQL 17: a match outranks an unknown candidate, because
    /// `false AND NULL` is false. This is the row an implementation that checks for NULLs
    /// first gets wrong.
    #[test]
    fn a_match_beside_a_null_candidate_is_still_false() {
        assert_eq!(
            not_in_scalar_list(Some(vec![Some(20), None]), vec![Some(20)])
                .expect("should not raise"),
            vec![Some(false)]
        );
    }

    /// A NULL element is unknown against any non-empty candidate list.
    #[test]
    fn a_null_element_is_null() {
        assert_eq!(
            not_in_scalar_list(Some(vec![Some(20)]), vec![None]).expect("should not raise"),
            vec![None]
        );
    }

    /// Measured against PostgreSQL 17, and the one rule that overrides all the others: an
    /// empty subquery is `true` even for a NULL element. `array_agg` over no rows is a NULL
    /// list, so both spellings of "no candidates" are tested.
    #[test]
    fn no_candidates_is_true_even_for_a_null_element() {
        assert_eq!(
            not_in_scalar_list(None, vec![Some(10), None]).expect("should not raise"),
            vec![Some(true), Some(true)]
        );
        assert_eq!(
            not_in_scalar_list(Some(vec![]), vec![Some(10), None]).expect("should not raise"),
            vec![Some(true), Some(true)]
        );
    }

    /// The pair `array_has` refuses: a `numeric` element against an `int4` candidate list,
    /// which is what `HAVING MAX(numeric_col) NOT IN (SELECT int_col …)` produces.
    #[test]
    fn a_decimal_element_compares_against_an_integer_list() {
        let element = Decimal128Array::from(vec![Some(2000), Some(1000)])
            .with_precision_and_scale(10, 2)
            .expect("a valid numeric");
        assert_eq!(
            invoke(
                int_list(Some(vec![Some(20), Some(99)])),
                ColumnarValue::Array(Arc::new(element)),
            )
            .expect("should not raise"),
            // 20.00 is the candidate 20; 10.00 is not among them.
            vec![Some(false), Some(true)]
        );
    }

    /// The other pair `array_has` refuses: a window function's `UInt64` against an `int4`
    /// list, which is what `QUALIFY row_number() OVER (…) NOT IN (SELECT int_col …)`
    /// produces.
    #[test]
    fn an_unsigned_element_compares_against_a_signed_list() {
        assert_eq!(
            invoke(
                int_list(Some(vec![Some(2)])),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![Some(2), Some(3)]))),
            )
            .expect("should not raise"),
            vec![Some(false), Some(true)]
        );
    }

    /// A list per row, which is not what the rewrite builds but is what a client's own
    /// `array_agg` column would arrive as.
    #[test]
    fn a_list_per_row_is_answered_per_row() {
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10)]),
            Some(vec![Some(20), None]),
            None,
        ]);
        assert_eq!(
            invoke(
                ColumnarValue::Array(Arc::new(lists)),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(10),
                    Some(10)
                ]))),
            )
            .expect("should not raise"),
            // a match, a NULL candidate with no match, and no candidates at all.
            vec![Some(false), None, Some(true)]
        );
    }

    /// A batch of no rows answers nothing rather than one row.
    #[test]
    fn an_empty_batch_answers_no_rows() {
        assert_eq!(
            not_in_scalar_list(Some(vec![Some(20)]), vec![]).expect("should not raise"),
            Vec::<Option<bool>>::new()
        );
    }

    /// A list of a type the element cannot be compared against is reported, not answered:
    /// a wrong answer here is the thing this module exists to prevent.
    #[test]
    fn an_incomparable_pair_is_an_error() {
        let err = invoke(
            int_list(Some(vec![Some(1)])),
            ColumnarValue::Array(Arc::new(BooleanArray::from(vec![Some(true)]))),
        )
        .expect_err("a boolean and an int4 have no comparison type");
        assert!(
            err.to_string().contains(NOT_IN_UDF_NAME),
            "the message should name the function: {err}"
        );
    }

    /// A predicate: the type the client's row description carries is boolean, whatever the
    /// two operands were.
    #[test]
    fn the_result_type_is_boolean() {
        assert_eq!(
            NotInList::new()
                .return_type(&[DataType::Boolean, DataType::Int32])
                .expect("a predicate"),
            DataType::Boolean
        );
    }

    /// The wire carries only the name, so the name has to resolve after registration — and
    /// registering twice has to stay a no-op, because a context reached by two registration
    /// paths is exactly what [`register_not_in`]'s "every context" instruction produces.
    #[test]
    fn the_function_resolves_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        assert!(ctx.udf(NOT_IN_UDF_NAME).is_err());
        register_not_in(&mut ctx).expect("first registration failed");
        register_not_in(&mut ctx).expect("second registration failed");
        assert!(ctx.udf(NOT_IN_UDF_NAME).is_ok());
    }
}
