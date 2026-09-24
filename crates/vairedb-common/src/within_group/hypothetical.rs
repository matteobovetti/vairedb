//! The hypothetical-set family: `rank`, `dense_rank`, `percent_rank` and `cume_dist` as
//! `f(value) WITHIN GROUP (ORDER BY expr)`.
//!
//! All four answer where a **hypothetical** row, whose ordered value is the direct argument,
//! would land if it were added to the group. With `n` rows in the group, `before` rows
//! sorting strictly ahead of the hypothetical one, `distinct` distinct values among those,
//! and `peers` rows equal to it:
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
//! count towards `n`, because a window function would see them too. That is the one place
//! this family differs from [`super::mode`], which ignores nulls as an aggregate does.
//!
//! ## Why the comparison is a row encoding
//!
//! "Sorts strictly ahead of" has to mean exactly what the `WITHIN GROUP ORDER BY` means,
//! including `DESC` and the placement of nulls, and it has to compare the hypothetical value
//! against the column's values under that same rule. Arrow's row format is precisely that
//! rule made comparable: [`RowConverter`] built from the clause's [`SortOptions`] encodes
//! each value into bytes whose ordering *is* the sort order and whose equality is value
//! equality. So the whole of the ordering question — descending, nulls, and the tie that
//! `dense_rank` de-duplicates — is one `cmp` per row against one encoded hypothetical row,
//! with no per-type comparison logic of this module's own to be wrong about. An ordered
//! column whose type the format cannot encode is refused naming the type, rather than
//! answered under some other order.
//!
//! ## Why these four are registered under another name
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
//! the two namespaces the planner conflates.

use std::collections::HashSet;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{SortOptions, cast};
use arrow::datatypes::{DataType, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{Result, ScalarValue, exec_datafusion_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

use super::clause;
use super::group::{self, Grouped};
use crate::error::tagged_message;
use crate::proto::vairedb::v1::VdbErrorCode;

/// How the direct argument is named in a refusal, and so what a client is told to fix.
const HYPOTHETICAL: &str = "hypothetical value";

/// The four aggregates this file owns, for [`super::register_within_group_aggregates`].
pub(super) fn udafs() -> Vec<Arc<AggregateUDF>> {
    Hypothetical::ALL
        .into_iter()
        .map(|kind| Arc::new(AggregateUDF::from(HypotheticalSet::new(kind))))
        .collect()
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
struct HypotheticalSet {
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
        group::state_fields(self.name(), &args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // The client-facing name for every message: what it wrote is `rank`, whatever the
        // read path renamed the call to.
        let spelled = self.kind.postgres_name();
        let ordered = clause::ordered_column(spelled, &args)?;
        let options = clause::sort_options(spelled, &args)?;
        let hypothetical = clause::literal_direct_argument(args.exprs, 1, spelled, HYPOTHETICAL)?;
        Ok(Box::new(HypotheticalAccumulator {
            kind: self.kind,
            hypothetical,
            options,
            group: Grouped::new(ordered, spelled),
        }))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
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
        self.group.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.group.merge_batch(states)
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
    // Both refusals below are tagged, because both can only be reached on an executor: see
    // [`super::clause`] for what a `DataFusionError` loses on the way back through the
    // scheduler, and why `XX000 internal_error` is the wrong thing to tell a client who wrote
    // a statement no retry will fix.
    let compare_at = comparison_coercion(values.data_type(), &hypothetical.data_type())
        .ok_or_else(|| {
            exec_datafusion_err!(
                "{}",
                tagged_message(
                    VdbErrorCode::TypeMismatch,
                    format!(
                        "{function} cannot compare a hypothetical {} against an ordered column \
                         of type {}",
                        hypothetical.data_type(),
                        values.data_type()
                    )
                )
            )
        })?;
    let values = cast(values, &compare_at)?;
    let hypothetical = hypothetical.cast_to(&compare_at)?.to_array()?;

    // The row encoding *is* the sort order the clause asked for: byte order equals value
    // order under these options, and equal bytes mean equal values. So one `cmp` per row
    // answers all three counts, for every type the format supports.
    let field = SortField::new_with_options(compare_at.clone(), options);
    let converter = RowConverter::new(vec![field]).map_err(|e| {
        exec_datafusion_err!(
            "{}",
            tagged_message(
                VdbErrorCode::FeatureNotSupported,
                format!("{function} cannot order a column of type {compare_at}: {e}")
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};

    use crate::error::code_of_tagged_message;
    use Hypothetical::{CumeDist, DenseRank, PercentRank, Rank};

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn one_to(last: i64) -> ArrayRef {
        ints(&(1..=last).map(Some).collect::<Vec<_>>())
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

    fn counts(
        values: &ArrayRef,
        hypothetical: impl Into<ScalarValue>,
        options: SortOptions,
    ) -> Counts {
        counts_against(values, &hypothetical.into(), options, "rank").unwrap()
    }

    fn answer(
        kind: Hypothetical,
        values: &ArrayRef,
        hypothetical: impl Into<ScalarValue>,
        options: SortOptions,
    ) -> ScalarValue {
        kind.answer(counts(values, hypothetical, options))
    }

    /// `SELECT rank(5) WITHIN GROUP (ORDER BY n) FROM (1 … 10)` — PostgreSQL answers 5,
    /// because four rows sort ahead of the hypothetical one.
    #[test]
    fn the_rank_counts_the_rows_ahead_of_the_hypothetical_one() {
        let values = one_to(10);
        assert_eq!(
            answer(Rank, &values, 5_i64, ascending()),
            ScalarValue::Int64(Some(5))
        );
        // Ahead of everything, and behind everything.
        assert_eq!(
            answer(Rank, &values, 0_i64, ascending()),
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(
            answer(Rank, &values, 99_i64, ascending()),
            ScalarValue::Int64(Some(11))
        );
    }

    /// A value equal to some rows does not count them: `rank` is 1 + the rows *strictly*
    /// ahead, so the hypothetical row ties with its peers rather than following them.
    #[test]
    fn peers_do_not_count_towards_the_rank() {
        let values = ints(&[Some(1), Some(5), Some(5), Some(5), Some(9)]);
        let c = counts(&values, 5_i64, ascending());
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
            Rank.answer(c),
            ScalarValue::Int64(Some(2)),
            "one row ahead, so rank 2"
        );
    }

    /// `dense_rank` counts distinct values ahead, so repeats collapse.
    #[test]
    fn the_dense_rank_counts_distinct_values_ahead() {
        let values = ints(&[Some(1), Some(1), Some(1), Some(2), Some(9)]);
        assert_eq!(
            answer(DenseRank, &values, 5_i64, ascending()),
            ScalarValue::Int64(Some(3)),
            "1 and 2 are ahead, so the third dense rank"
        );
        assert_eq!(
            answer(Rank, &values, 5_i64, ascending()),
            ScalarValue::Int64(Some(5)),
            "four rows are ahead, so the fifth rank"
        );
    }

    /// `percent_rank` divides by the real rows and `cume_dist` by them plus the hypothetical
    /// one, which is what the two window functions do to a group of `n + 1` rows.
    #[test]
    fn the_two_fractions_use_the_denominators_postgresql_uses() {
        let values = one_to(4);
        // 5 sorts behind all four rows: rank 5 of 5, so percent_rank 1.
        assert_eq!(
            answer(PercentRank, &values, 5_i64, ascending()),
            ScalarValue::Float64(Some(1.0))
        );
        assert_eq!(
            answer(CumeDist, &values, 5_i64, ascending()),
            ScalarValue::Float64(Some(1.0))
        );
        // 3 has two rows ahead of it and one peer, so of the five rows it and its peer are
        // the third and fourth: 2/4, and (2 + 1 + 1)/5.
        assert_eq!(
            answer(PercentRank, &values, 3_i64, ascending()),
            ScalarValue::Float64(Some(0.5))
        );
        assert_eq!(
            answer(CumeDist, &values, 3_i64, ascending()),
            ScalarValue::Float64(Some(0.8))
        );
    }

    /// An empty group: the hypothetical row is the only row, so it is first, alone, and its
    /// cumulative distribution is the whole of it. `percent_rank` must not divide by zero.
    #[test]
    fn an_empty_group_is_the_hypothetical_row_alone() {
        let values = ints(&[]);
        for (kind, expected) in [
            (Rank, ScalarValue::Int64(Some(1))),
            (DenseRank, ScalarValue::Int64(Some(1))),
            (PercentRank, ScalarValue::Float64(Some(0.0))),
            (CumeDist, ScalarValue::Float64(Some(1.0))),
        ] {
            assert_eq!(
                answer(kind, &values, 5_i64, ascending()),
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
        assert_eq!(
            answer(Rank, &one_to(10), 5_i64, descending()),
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
            counts(&values, 3_i64, ascending()),
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
            counts(&values, 3_i64, descending()),
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
        assert_eq!(
            answer(Rank, &one_to(3), ScalarValue::Int64(None), ascending()),
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
            answer(Rank, &values, 2_i64, ascending()),
            ScalarValue::Int64(Some(2)),
            "only 1.5 is below 2"
        );
    }

    /// Text orders too, and `dense_rank` de-duplicates it.
    #[test]
    fn a_text_column_is_ordered_as_text() {
        let values = text(&["a", "b", "b", "z"]);
        assert_eq!(
            answer(Rank, &values, "c", ascending()),
            ScalarValue::Int64(Some(4))
        );
        assert_eq!(
            answer(DenseRank, &values, "c", ascending()),
            ScalarValue::Int64(Some(3))
        );
    }

    /// Two types `=` cannot reconcile — `rank(5) WITHIN GROUP (ORDER BY a_boolean)` — are
    /// refused naming both, rather than compared under some encoding that would answer a
    /// number the client would believe. The refusal runs on an executor, so it carries its own
    /// SQLSTATE: untagged it would arrive as the `XX000` that says a retry might help.
    #[test]
    fn a_hypothetical_value_the_column_cannot_be_compared_to_is_refused() {
        let values: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let err = counts_against(&values, &ScalarValue::from(5_i64), ascending(), "rank")
            .expect_err("a boolean column has no ordering against an integer");
        assert_eq!(
            code_of_tagged_message(&err.to_string()),
            Some(VdbErrorCode::TypeMismatch)
        );
        assert!(
            err.to_string().contains(
                "rank cannot compare a hypothetical Int64 against an ordered column of type \
                 Boolean"
            ),
            "got: {err}"
        );
    }

    /// The four are registered under VaireDB's own names and not PostgreSQL's, which is what
    /// leaves `rank() OVER (…)` resolving to the window function — see the module doc. The
    /// rename is case-insensitive because SQL identifiers are, and it answers for these four
    /// names only.
    #[test]
    fn the_postgresql_names_map_to_the_udafs_that_answer_them() {
        assert_eq!(
            hypothetical_set_udaf("rank"),
            Some("vaire_hypothetical_rank")
        );
        assert_eq!(
            hypothetical_set_udaf("DENSE_RANK"),
            Some("vaire_hypothetical_dense_rank")
        );
        assert_eq!(
            hypothetical_set_udaf("percent_rank"),
            Some("vaire_hypothetical_percent_rank")
        );
        assert_eq!(
            hypothetical_set_udaf("cume_dist"),
            Some("vaire_hypothetical_cume_dist")
        );
        assert_eq!(
            hypothetical_set_udaf("mode"),
            None,
            "mode has no window function of its name to protect, so it is not renamed"
        );
        assert_eq!(hypothetical_set_udaf("ntile"), None);
        let udafs = udafs();
        let registered: Vec<&str> = udafs.iter().map(|udaf| udaf.name()).collect();
        assert_eq!(
            registered,
            vec![
                "vaire_hypothetical_rank",
                "vaire_hypothetical_dense_rank",
                "vaire_hypothetical_percent_rank",
                "vaire_hypothetical_cume_dist"
            ],
            "every name the rename can produce has to be a name that is registered"
        );
    }
}
