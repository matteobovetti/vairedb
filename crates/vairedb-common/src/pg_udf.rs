//! PostgreSQL's `pg_catalog` scalar functions, registered on every node that plans or
//! executes a read.
//!
//! `datafusion-pg-catalog`'s `setup_pg_catalog` registers these on a `SessionContext`
//! together with the `pg_catalog` **tables**, and the coordinator calls it for the two
//! contexts that answer catalog queries. That is enough for `\d`, and it is not enough
//! for a distributed read, which is what this module exists for.
//!
//! A scalar function crosses the Ballista wire as **a name and nothing else**: the
//! logical plan carries `ScalarUdfExprNode { fun_name, args }` with no definition
//! attached, and the side that decodes it looks the name up in its own registry
//! (`datafusion-proto`'s `ctx.udf(name).or_else(|_| codec.try_decode_udf(name, &[]))`).
//! So a function registered only where the query is *parsed* plans fine and then fails
//! on the two nodes that never heard of it — the scheduler, which decodes the logical
//! plan the client submits, and the executor, which decodes the physical stage. The
//! failure is late and unhelpful:
//!
//! ```text
//! SELECT format_type(oid, NULL) FROM (VALUES (23),(25)) t(oid);
//! ERROR:  XX000: Could not parse plan: … NotImplemented("LogicalExtensionCodec is not
//!         provided for scalar function format_type")
//! ```
//!
//! `psql`'s `\gdesc` is what found it, because it falls back to exactly that shape — a
//! `VALUES` list plus `pg_catalog.format_type`.
//!
//! ## Why registering the name is the fix, and a codec arm is not
//!
//! `LogicalExtensionCodec::try_decode_udf` is the *fallback* the message names, so adding
//! an arm to `VaireLogicalCodec` would also work — for the logical plan. It would then
//! have to be added again to the physical codec, and the physical codec on the executor
//! side lives in `vairedb-core` and would need these constructors anyway. Making the name
//! resolve is the same fix in one place, and it is the invariant the registration seam
//! already states: every context that plans or executes needs the identical set.
//!
//! ## Why four of `setup_pg_catalog`'s functions are left out
//!
//! All of these declare `Volatility::Immutable` or `Stable`, so DataFusion's simplifier
//! folds a call whose arguments are all literals into a constant *before* the plan is
//! serialized. A function that can **only** be called with no arguments therefore can
//! never cross the wire — measured, not assumed: `SELECT current_database(),
//! current_schema(), session_user, pg_backend_pid() FROM (VALUES (1)) t(x)` answers,
//! while `current_schemas(b)` over a column does not. Those four are also session-scoped,
//! and an executor is precisely the wrong place to answer them, so leaving them out is a
//! property worth keeping rather than an omission. (`current_schemas` is here because it
//! *also* has a one-argument form, and that form is the one that gets distributed.)

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;
use datafusion_pg_catalog::pg_catalog::{
    array_bounds_udf, create_current_schemas_udf, create_pg_encoding_to_char_udf,
    create_pg_get_constraintdef, create_pg_get_partition_ancestors_udf,
    create_pg_get_partkeydef_udf, create_pg_get_statisticsobjdef_columns_udf,
    create_pg_get_userbyid_udf, create_pg_relation_is_publishable_udf, create_pg_relation_size_udf,
    create_pg_stat_get_numscans, create_pg_table_is_visible, create_pg_total_relation_size_udf,
    format_type, has_privilege_udf, pg_get_expr_udf, quote_ident_udf,
};

/// Register the `pg_catalog` scalar functions that can appear in a serialized plan.
///
/// Call this on every context that plans **or** executes a read; see the module doc for
/// why the set is limited to functions that take an argument.
pub fn register_pg_catalog_scalar_functions(registry: &mut dyn FunctionRegistry) -> Result<()> {
    for udf in pg_catalog_scalar_functions() {
        // The previous registration under the same name, if any: the coordinator's
        // catalog contexts get these from `setup_pg_catalog` too, and re-registering the
        // same function is how that stays idempotent.
        registry.register_udf(Arc::new(udf))?;
    }
    Ok(())
}

/// The functions themselves, in the order `setup_pg_catalog` registers them.
fn pg_catalog_scalar_functions() -> Vec<ScalarUDF> {
    vec![
        create_current_schemas_udf(),
        create_pg_get_userbyid_udf(),
        has_privilege_udf::create_has_privilege_udf("has_table_privilege"),
        has_privilege_udf::create_has_privilege_udf("has_schema_privilege"),
        has_privilege_udf::create_has_privilege_udf("has_database_privilege"),
        has_privilege_udf::create_has_privilege_udf("has_any_column_privilege"),
        create_pg_table_is_visible(),
        format_type::create_format_type_udf(),
        pg_get_expr_udf::create_pg_get_expr_udf(),
        create_pg_get_partkeydef_udf(),
        create_pg_relation_is_publishable_udf(),
        create_pg_get_statisticsobjdef_columns_udf(),
        create_pg_encoding_to_char_udf(),
        create_pg_relation_size_udf(),
        create_pg_total_relation_size_udf(),
        create_pg_stat_get_numscans(),
        create_pg_get_constraintdef(),
        create_pg_get_partition_ancestors_udf(),
        quote_ident_udf::create_quote_ident_udf(),
        quote_ident_udf::create_parse_ident_udf(),
        array_bounds_udf::create_array_upper_udf(),
        array_bounds_udf::create_array_lower_udf(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    /// The one a client tool found, and the reason this module exists.
    #[test]
    fn format_type_resolves_by_name_after_registration() {
        let mut ctx = SessionContext::new();
        assert!(
            ctx.udf("format_type").is_err(),
            "a bare context should not have it, or this test proves nothing"
        );
        register_pg_catalog_scalar_functions(&mut ctx).expect("registration failed");
        assert!(ctx.udf("format_type").is_ok());
    }

    /// Every name in the set has to resolve, because the wire carries only the name.
    #[test]
    fn every_registered_function_resolves_by_its_own_name() {
        let mut ctx = SessionContext::new();
        register_pg_catalog_scalar_functions(&mut ctx).expect("registration failed");
        for udf in pg_catalog_scalar_functions() {
            assert!(
                ctx.udf(udf.name()).is_ok(),
                "{} did not resolve by name",
                udf.name()
            );
        }
    }

    /// The rule the module doc states, from the excluded side: a call with no arguments is
    /// folded to a literal before the plan is serialized, so a function with no other form
    /// cannot reach a node that would have to resolve it — and all four of these are
    /// session-scoped, which an executor could not answer correctly anyway.
    #[test]
    fn the_session_scoped_functions_are_left_out() {
        let names: Vec<String> = pg_catalog_scalar_functions()
            .iter()
            .map(|udf| udf.name().to_string())
            .collect();
        for excluded in [
            "current_database",
            "current_schema",
            "session_user",
            "pg_backend_pid",
        ] {
            assert!(
                !names.contains(&excluded.to_string()),
                "{excluded} takes no arguments, so it is always folded before serialization"
            );
        }
    }

    /// Registering twice is what the coordinator's catalog contexts do, since
    /// `setup_pg_catalog` registers the same names.
    #[test]
    fn registering_twice_is_idempotent() {
        let mut ctx = SessionContext::new();
        register_pg_catalog_scalar_functions(&mut ctx).expect("first registration failed");
        register_pg_catalog_scalar_functions(&mut ctx).expect("second registration failed");
        assert!(ctx.udf("format_type").is_ok());
    }
}
