//! One scratch `MetadataCatalog` per test, on a name no other test or run can
//! take, and no file left behind.
//!
//! ## The failure
//!
//! Four unit tests failed in a full `make test` and passed when run on their own:
//!
//! ```text
//! views::tests::a_created_view_is_stored_as_its_query_text
//!   ERROR, 42P07, relation "v" already exists
//! ddl::tests::a_column_list_alongside_as_select_is_refused
//!   assertion failed: !is_registered(&handler, "dst")
//! ```
//!
//! Both assert against a catalog the test believes is empty, and both were handed
//! one that already held a view named `v` and a table named `dst` — put there by
//! *the same test in an earlier run of the test binary*.
//!
//! ## Why
//!
//! Four modules each built their scratch catalog at
//! `temp_dir()/vairedb_test_<module>_<pid>_<counter>.redb` and never removed it.
//! The counter makes the name unique within one process, which is all it was
//! written for — redb takes an exclusive lock, so two live tests must not share a
//! file. It does nothing across processes: a pid is recycled, the counter restarts
//! at zero, and the next run's third `for_tests()` reopens the third catalog of
//! some earlier run, with that run's tables and views still in it. Nothing fails
//! until a recycled pid happens to land on a run that got far enough to write the
//! name the test asserts is absent, which is why this reads as a flake. The
//! evidence was in the temp directories: **17,000** leaked `.redb` files.
//!
//! ## The repair
//!
//! Two changes, and the second is what makes the first hold. The name now carries
//! a nanosecond timestamp as well as the pid and the counter, so it is unique
//! across runs and not merely within one; and the file is **unlinked as soon as
//! redb has opened it**, so a name cannot be inherited even in principle and a
//! test run leaves the temp directory as it found it. Unlinking an open file is
//! safe here because `Database::create` keeps the handle: redb never reopens by
//! path, so the storage stays valid for the life of the catalog and is reclaimed
//! when the last `Arc` to it drops. That also removes the need for a guard object
//! with a lifetime the callers cannot give it — `for_tests()` returns a handler by
//! value and `catalog_with()` returns an `Arc`, and neither has anywhere to keep
//! one.
//!
//! ## Why it sits at the crate root
//!
//! The leak was never specific to one module — every module whose tests need a
//! catalog can reproduce it, and one that could not reach this fixture did: the
//! failure detector's tests had rebuilt the pid-and-counter scheme verbatim. A
//! fixture scoped to `pgwire_handler` is an invitation to write the bug a second
//! time, so this is `pub(crate)` and is how a unit test in this crate gets a
//! catalog.

use crate::catalog::{ColumnDef, MetadataCatalog, ShardMeta, ShardStrategy, TableMeta};

/// A `MetadataCatalog` with nothing in it, for one test.
///
/// `tag` names the calling module and appears in the file name; it is a debugging
/// aid only, since the file is unlinked immediately and two callers passing the
/// same tag are still given different catalogs.
pub(crate) fn scratch_catalog(tag: &str) -> MetadataCatalog {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!(
        "vairedb_test_{tag}_{}_{}_{unique}.redb",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    ));

    let catalog = MetadataCatalog::open(path.to_str().expect("temp path is not UTF-8"))
        .unwrap_or_else(|e| panic!("scratch catalog at {}: {e}", path.display()));

    // Unlink while redb holds the handle: see the module doc. A failure here would
    // leak one file rather than break the test, so it is not worth a panic.
    let _ = std::fs::remove_file(&path);

    catalog
}

/// A sharded table for a test to put in a catalog: `columns` in the order given, each a
/// nullable `INTEGER`, hash-sharded on `shard_key`.
///
/// The literal this replaces was written 30 times across 12 modules, and the copies had
/// already drifted on the parts no test states an opinion about: three left `shard_count`
/// at `Default`, i.e. **zero**, and `..Default::default()` leaves `shard_strategy` at
/// `Unspecified`. Neither is a table `ddl::plan_create_table` can produce — it always
/// writes `Hash` and a `shard_count` of at least one. A fixture is only evidence about
/// production if it is shaped like production, so those two are pinned here, and the
/// column shape — the part a COPY or a DML test does assert on — stays the caller's.
pub(crate) fn table_meta(name: &str, columns: &[&str], shard_key: &str) -> TableMeta {
    TableMeta {
        table_name: name.to_string(),
        columns: columns
            .iter()
            .map(|c| ColumnDef {
                name: (*c).to_string(),
                data_type: "INTEGER".to_string(),
                nullable: true,
                ..Default::default()
            })
            .collect(),
        shard_strategy: ShardStrategy::Hash as i32,
        shard_key: shard_key.to_string(),
        shard_count: 2,
        replication_factor: 1,
        ..Default::default()
    }
}

/// One hash shard of `table`: bucket `bucket`, primary on `primary`, replicated to
/// `replicas`.
///
/// Named and shaped after what [`MetadataCatalog::assign_shards`] writes, which is the only
/// producer of shard records in production: a `logical_shard_id` of `shard<bucket>` and no
/// range bounds. Two of the five literals this replaces had drifted to a `shard_id` of
/// `<table>-<bucket>` — harmless while no test reads the field, and exactly the kind of
/// fixture that stops being evidence the moment one does.
pub(crate) fn shard_meta(table: &str, bucket: u32, primary: &str, replicas: &[&str]) -> ShardMeta {
    ShardMeta {
        shard_id: crate::util::logical_shard_id(bucket),
        table_name: table.to_string(),
        primary_node_id: primary.to_string(),
        replica_node_ids: replicas.iter().map(|r| (*r).to_string()).collect(),
        hash_bucket: bucket,
        range_lower: String::new(),
        range_upper: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ViewMeta;

    fn put_view(catalog: &MetadataCatalog, name: &str) {
        catalog
            .put_view(&ViewMeta {
                view_name: name.to_string(),
                definition: "SELECT 1".to_string(),
                columns: Vec::new(),
                created_at: None,
            })
            .unwrap();
    }

    #[test]
    fn a_scratch_catalog_starts_empty() {
        assert!(scratch_catalog("selftest").get_view("v").unwrap().is_none());
    }

    /// The property the four failing tests needed and did not have. Writing to one
    /// catalog must be invisible to the next, which is what an inherited file broke.
    #[test]
    fn two_scratch_catalogs_do_not_share_storage() {
        let first = scratch_catalog("selftest");
        put_view(&first, "v");

        let second = scratch_catalog("selftest");
        assert!(second.get_view("v").unwrap().is_none());
        assert!(first.get_view("v").unwrap().is_some());
    }

    /// redb must keep working after its file is unlinked, since that is what the
    /// repair relies on. Written as a read *and* a write after the unlink, because a
    /// deleted directory entry would only break the second.
    #[test]
    fn a_catalog_outlives_its_file() {
        let catalog = scratch_catalog("selftest");
        put_view(&catalog, "before");
        assert!(catalog.get_view("before").unwrap().is_some());

        put_view(&catalog, "after");
        assert!(catalog.get_view("after").unwrap().is_some());
    }

    /// No file is left behind — the reason 17,000 of them accumulated.
    ///
    /// Counted under a tag no other test uses. Sharing one would make this racy
    /// rather than wrong: a sibling test running in parallel is briefly visible in
    /// the temp directory, between redb opening its file and the unlink.
    #[test]
    fn a_scratch_catalog_leaves_no_file() {
        let _catalog = scratch_catalog("leakcheck");
        assert_eq!(files_tagged("leakcheck"), 0);
    }

    fn files_tagged(tag: &str) -> usize {
        let prefix = format!("vairedb_test_{tag}_");
        std::fs::read_dir(std::env::temp_dir())
            .expect("temp dir is readable")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
            .count()
    }
}
