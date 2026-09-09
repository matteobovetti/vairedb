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

use crate::catalog::MetadataCatalog;

/// A `MetadataCatalog` with nothing in it, for one test.
///
/// `tag` names the calling module and appears in the file name; it is a debugging
/// aid only, since the file is unlinked immediately and two callers passing the
/// same tag are still given different catalogs.
pub(super) fn scratch_catalog(tag: &str) -> MetadataCatalog {
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
