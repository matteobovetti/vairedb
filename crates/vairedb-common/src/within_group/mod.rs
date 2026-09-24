//! PostgreSQL's `WITHIN GROUP` aggregates: `percentile_cont`, `percentile_disc`, `mode()`,
//! and the hypothetical-set family `rank`, `dense_rank`, `percent_rank` and `cume_dist`.
//!
//! ```text
//! t(n) = 1 … 10
//!
//! SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY n) FROM t
//! SELECT mode() WITHIN GROUP (ORDER BY g) FROM t
//! SELECT rank(5) WITHIN GROUP (ORDER BY n) FROM t
//!
//! PostgreSQL  9.1                            answers
//! VaireDB     9.099999…                      -- DataFusion's, before this module
//!             ERROR:  Invalid function 'mode'. Did you mean 'md5'?
//!             ERROR:  Invalid function 'rank'. Did you mean 'rand'?
//! ```
//!
//! These are one feature — the clause is what supplies their input — and they were two
//! modules that had grown the same mechanism twice. What a client sees is seven functions
//! with seven distributions; what the code has to get right is one contract, and two files
//! hold the whole of it:
//!
//! * `clause` — what a *call* says: which column is ordered, which way, what `DISTINCT` and a
//!   missing `WITHIN GROUP` are refused with, and the rule that a direct argument is a
//!   literal.
//! * `group` — what the *group* is: the state a partial and a final aggregate agree on, and
//!   the ranked non-null values the answers by position are read off.
//!
//! Each family's own file then holds only its own distribution, which is the part a client can
//! see:
//!
//! * `percentile` — the two percentiles, and why `percentile_cont` shadows DataFusion's, with
//!   `fractions` for the direct argument they share.
//! * `mode` — the most frequent value, and why the clause is its tie-break.
//! * `hypothetical` — the four that place a hypothetical row, and why they are renamed.
//!
//! ## Why they are in this crate
//!
//! An aggregate crosses the Ballista wire as **a name**: a stage names it and the executor
//! resolves that name in its own registry. One implementation in the crate both nodes share
//! is what keeps the two sides from answering the same query differently, which is why
//! [`register_within_group_aggregates`] has to run on every context that plans **or**
//! executes a read — and why it is reached through [`crate::distributed_functions`] rather
//! than called by hand on each node.
//!
//! ## What is refused, and why each one is
//!
//! * **More than one ordered column.** `rank(a, b) WITHIN GROUP (ORDER BY x, y)` is legal
//!   PostgreSQL and is refused *upstream of this module*: datafusion-sql answers
//!   `Only a single ordering expression is permitted in a WITHIN GROUP clause` before any
//!   UDAF is consulted, so there is no signature this crate could offer that would be
//!   reached. The one-column form is the whole of what is implementable here.
//! * **A non-literal direct argument.** PostgreSQL evaluates the direct arguments once per
//!   group, so a grouped column is a legal fraction or hypothetical value there. Here the
//!   value is read off the physical literal, and anything else is refused rather than
//!   silently read from the first row.
//! * **`DISTINCT`.** PostgreSQL has no `DISTINCT` in an ordered-set aggregate, and
//!   de-duplicating the group would change the group every answer describes.
//! * **No `WITHIN GROUP` at all.** `mode(x)` and `rank(x, y)` parse as plain aggregates in
//!   DataFusion, but without the clause there is no sort order. Refused, naming the spelling.
//! * An ordered column whose type Arrow's row format cannot encode, naming the type rather
//!   than answering under some other order.
//!
//! ## State, and the wire
//!
//! Exact answers need the whole group, so the intermediate state is the values themselves as
//! a list — which is what lets the final aggregate merge the partials one per shard. The
//! direct argument is not part of the state because it is a literal every partial already
//! has.
//!
//! Every refusal that can only be reached on an executor is wrapped in
//! [`crate::error::tagged_message`], so its SQLSTATE survives the Ballista scheduler
//! rendering the error to text; without it a client is told `XX000` and that a retry might
//! help, and none of these will ever succeed on a retry.

mod clause;
mod fractions;
mod group;
mod hypothetical;
mod mode;
mod percentile;

/// The rename the coordinator has to apply to reach the hypothetical-set aggregates; every
/// other item here is an implementation detail of the seven functions
/// [`register_within_group_aggregates`] registers.
pub use hypothetical::hypothetical_set_udaf;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;

/// Register all seven `WITHIN GROUP` aggregates on `registry`.
///
/// `percentile_cont` replaces DataFusion's own (and keeps its `quantile_cont` alias), and the
/// four hypothetical-set aggregates go in under `vaire_hypothetical_<name>` rather than
/// PostgreSQL's names so that `rank() OVER (…)` keeps resolving to the window function — see
/// `hypothetical`, and [`hypothetical_set_udaf`] for the rename that makes the aggregate
/// reachable.
///
/// Each family says which aggregates it owns, so adding one is a change in that family's file
/// and nowhere else.
pub fn register_within_group_aggregates(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let udafs = percentile::udafs()
        .into_iter()
        .chain(mode::udafs())
        .chain(hypothetical::udafs());
    for udaf in udafs {
        registry.register_udaf(udaf)?;
    }
    Ok(())
}
