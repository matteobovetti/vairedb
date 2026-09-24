//! The one list of the functions a distributed read resolves by name, and the one seam
//! that installs them.
//!
//! ## The invariant this module owns
//!
//! A function crosses the Ballista wire as **a name and nothing else** — the reason each
//! of the modules below exists in `vairedb-common` rather than in the coordinator. The
//! consequence is a single rule: *every context that plans or executes a read holds the
//! identical set*. A name registered on the planner alone plans fine and then fails on the
//! node that runs the stage, after the query was accepted; a *shadowing* name registered
//! on the planner alone is worse, because the other node silently resolves DataFusion's
//! version and returns a wrong answer no client can detect.
//!
//! Before this module the rule was stated in a comment and enforced by hand: a
//! `register_*` call per family written out on the scheduler in `vairedb-coordinator` and
//! again on the executor in `vairedb-core`. Adding a function meant editing both, and
//! forgetting one produced exactly the failure above — so the crate that owns the
//! functions now owns the invariant too, and a node asks for the set instead of listing
//! it.
//!
//! ## What is not here
//!
//! `datafusion_pg_functions::register_all` stays at the call sites. It is upstream's own
//! single seam over its own set, so it cannot drift the way a hand-written list can, and
//! pulling it in would give this crate a dependency it has no other use for.
//!
//! The per-module `register_*` functions stay public: a test that exercises one family
//! wants that family and not every other, and the [`error_code_of_message`] wording tests
//! live beside the code that writes the message. What no longer has a second copy is the
//! *set*.

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;

use crate::proto::vairedb::v1::VdbErrorCode;

/// How a family installs itself on a registry.
type Register = fn(&mut dyn FunctionRegistry) -> Result<()>;

/// How a family recognizes one of its own failures in a message that lost its type
/// crossing the Ballista boundary.
type Classify = fn(&str) -> Option<VdbErrorCode>;

/// One family of shared functions.
///
/// `classify` is `None` for a family that raises nothing a client should see a specific
/// SQLSTATE for; see [`error_code_of_message`].
struct Family {
    /// What a failed registration is reported as, and nothing else.
    what: &'static str,
    register: Register,
    classify: Option<Classify>,
}

/// The set, in the order the two nodes used to spell out by hand.
///
/// Each entry's own module doc says why that family has to be here rather than in the
/// coordinator; this list says only that it is.
const FAMILIES: &[Family] = &[
    Family {
        what: "the pg_catalog scalar functions",
        register: crate::pg_udf::register_pg_catalog_scalar_functions,
        classify: None,
    },
    Family {
        what: "the WITHIN GROUP aggregates",
        register: crate::within_group::register_within_group_aggregates,
        classify: None,
    },
    Family {
        what: "the checked float division",
        register: crate::float_div::register_float_division,
        classify: None,
    },
    Family {
        what: "the list-valued NOT IN",
        register: crate::not_in::register_not_in,
        classify: None,
    },
    Family {
        what: "the bytea input conversion",
        register: crate::bytea_in::register_bytea_in,
        classify: Some(crate::bytea_in::error_code_of_message),
    },
    Family {
        what: "the json functions",
        register: crate::json_pg::register_json_functions,
        classify: None,
    },
    Family {
        what: "the json aggregates",
        register: crate::json_agg::register_json_aggregates,
        classify: None,
    },
    Family {
        what: "the uuid input conversion",
        register: crate::uuid_in::register_uuid_in,
        classify: None,
    },
    Family {
        what: "the checked nth_value",
        register: crate::nth_value::register_nth_value,
        classify: Some(crate::nth_value::error_code_of_message),
    },
    Family {
        what: "the exact statistics aggregates",
        register: crate::stats_udaf::register_statistics_aggregates,
        classify: None,
    },
    Family {
        what: "the int4 ntile",
        register: crate::ntile::register_ntile,
        classify: None,
    },
    Family {
        what: "the exact integer average",
        register: crate::avg_udaf::register_exact_average,
        classify: None,
    },
    Family {
        what: "pg_typeof",
        register: crate::pg_typeof::register_pg_typeof,
        classify: None,
    },
    Family {
        what: "the format functions",
        register: crate::pg_format::register_format_functions,
        classify: None,
    },
    Family {
        what: "the datetime functions",
        register: crate::pg_datetime::register_datetime_functions,
        classify: None,
    },
];

/// Register every shared function on `registry`.
///
/// Call this on each context that plans **or** executes a read — the client context and
/// the scheduler's state in `vairedb-coordinator`, the executor's state in `vairedb-core`
/// — and on nothing else. Registering twice is harmless: each family re-registers under
/// the same names, which is how a context that also ran `setup_pg_catalog` stays correct.
///
/// The error names the family that failed, so a registry that rejects one of them says
/// which one rather than which line of which node.
pub fn register(registry: &mut dyn FunctionRegistry) -> Result<()> {
    for family in FAMILIES {
        (family.register)(registry)
            .map_err(|e| e.context(format!("failed to register {}", family.what)))?;
    }
    Ok(())
}

/// The [`VdbErrorCode`] for a failure raised by one of these functions and recognized by
/// its *message*, or `None` if no family claims it.
///
/// A function runs inside a Ballista executor, and an error raised there reaches the
/// coordinator as text with its type gone: the scheduler renders the whole failure into a
/// string. Reading the message back is the only way for such a failure to carry the
/// SQLSTATE the same refusal carries when the write path catches it at parse time.
///
/// The coordinator's classifier asks this once instead of asking each family in turn, so a
/// family that gains a classifiable refusal is reachable from the classifier the moment its
/// entry in this module's list names the classifier.
pub fn error_code_of_message(msg: &str) -> Option<VdbErrorCode> {
    FAMILIES
        .iter()
        .filter_map(|family| family.classify)
        .find_map(|classify| classify(msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;
    use std::collections::HashSet;

    /// Every name each family registers on its own, gathered from a fresh context so the
    /// families cannot borrow each other's registrations.
    fn names_registered_family_by_family() -> HashSet<String> {
        let mut names = HashSet::new();
        for family in FAMILIES {
            let mut ctx = SessionContext::new();
            let before = all_names(&ctx);
            (family.register)(&mut ctx).expect("a family failed to register on its own");
            names.extend(all_names(&ctx).difference(&before).cloned());
        }
        names
    }

    fn all_names(ctx: &SessionContext) -> HashSet<String> {
        let mut names = ctx.udfs();
        names.extend(ctx.udafs());
        names.extend(ctx.udwfs());
        names
    }

    /// The whole point of the module: one call is the set. If a family is added to
    /// `FAMILIES` but [`register`] stops reaching it, the names it owns go missing here
    /// rather than on whichever node was not edited.
    #[test]
    fn one_call_registers_every_name_the_families_own() {
        let mut ctx = SessionContext::new();
        register(&mut ctx).expect("the set failed to register");
        let registered = all_names(&ctx);

        let mut missing: Vec<String> = names_registered_family_by_family()
            .difference(&registered)
            .cloned()
            .collect();
        missing.sort();
        assert!(
            missing.is_empty(),
            "the seam did not register: {}",
            missing.join(", ")
        );
    }

    /// The set is not empty, so the assertion above is about something.
    #[test]
    fn the_set_registers_the_names_a_client_would_name() {
        let mut ctx = SessionContext::new();
        register(&mut ctx).expect("the set failed to register");
        for name in [
            "format_type",
            "pg_typeof",
            "format",
            "age",
            "percentile_disc",
        ] {
            assert!(
                all_names(&ctx).contains(name),
                "{name} did not resolve after one call"
            );
        }
    }

    /// What the coordinator's catalog contexts do, since `setup_pg_catalog` registers some
    /// of these names too.
    #[test]
    fn registering_twice_is_idempotent() {
        let mut ctx = SessionContext::new();
        register(&mut ctx).expect("first registration failed");
        let after_first = all_names(&ctx);
        register(&mut ctx).expect("second registration failed");
        assert_eq!(after_first, all_names(&ctx));
    }

    #[test]
    fn a_bytea_refusal_is_classified_through_the_seam() {
        assert_eq!(
            error_code_of_message("Execution error: invalid hexadecimal digit: \"z\""),
            Some(VdbErrorCode::InvalidParameterValue)
        );
    }

    #[test]
    fn an_nth_value_refusal_is_classified_through_the_seam() {
        assert_eq!(
            error_code_of_message(&format!(
                "Execution error: {}",
                crate::nth_value::NON_POSITIVE_OFFSET_MESSAGE
            )),
            Some(VdbErrorCode::InvalidArgumentForNthValue)
        );
    }

    /// A failure none of the families raises stays unclassified, so the caller keeps
    /// whatever code it had rather than being handed a plausible wrong one.
    #[test]
    fn an_unrelated_failure_is_not_claimed() {
        assert_eq!(error_code_of_message("division by zero"), None);
        assert_eq!(
            error_code_of_message("Job abc failed: stage 1 failed"),
            None
        );
    }
}
