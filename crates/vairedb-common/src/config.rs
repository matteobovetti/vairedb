//! YAML configuration loading shared by both node binaries.
//!
//! The error is typed, and the type is the point. Both nodes read their whole configuration
//! from one file at startup and exit if they cannot, so this error is the *only* thing an
//! operator sees from a node that will not start — and there are two different things to do
//! about it. A file that cannot be read is a wrong `--config-file`, a relative path resolved
//! against an unexpected working directory, or a permission; a file that was read and does
//! not describe the configuration is a missing or misspelled field inside it. Handing both
//! back as one `Box<dyn Error>` left the operator to tell them apart from
//! `No such file or directory (os error 2)`, which does not even name the file it looked for.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

/// Why the configuration file could not be turned into a `T`.
///
/// Both variants name the path, because a node's own message is the only place the operator
/// learns which file was actually opened.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be read: it is not there, or not readable by this process.
    Unreadable {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The file was read, and its contents are not the configuration the node needs — a
    /// missing field, a value of the wrong type, or YAML that does not parse at all.
    Invalid {
        path: PathBuf,
        source: serde_yaml::Error,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // The `source` carries the reason; the path is what the operator has to act on.
            ConfigError::Unreadable { path, source } => {
                write!(
                    f,
                    "cannot read the configuration file {}: {source}",
                    path.display()
                )
            }
            // `serde_yaml` names the field and the line, which is the whole of the fix.
            ConfigError::Invalid { path, source } => write!(
                f,
                "{} is not a valid VaireDB configuration: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Unreadable { source, .. } => Some(source),
            ConfigError::Invalid { source, .. } => Some(source),
        }
    }
}

/// Read the file at `path` and deserialize its YAML contents into `T`.
pub fn from_file<T: DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Unreadable {
        path: path.to_path_buf(),
        source,
    })?;
    serde_yaml::from_str(&contents).map_err(|source| ConfigError::Invalid {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::error::Error as _;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug, Deserialize)]
    struct Settings {
        listen_addr: String,
    }

    /// A YAML file that lives only as long as the test that wrote it.
    ///
    /// [`from_file`] takes a path and opens it, so there is no testing it without a real
    /// file. Two things about the name are deliberate. It carries the caller's `stem`,
    /// because the messages under test are asserted to *name the file* and a stem that says
    /// which test wrote it is what makes that assertion readable when it fails. It also
    /// carries the process id and a counter, because a fixed name is shared state: two
    /// `cargo test` runs of this crate at once — one per worktree, say — would otherwise
    /// write, read and delete the same path and fail each other at random.
    ///
    /// The removal is in `Drop` rather than at the end of each test on purpose. A failing
    /// assertion panics, so a test that cleans up on its last line leaks the file exactly
    /// when it failed, and the *next* run of that test then reads the previous run's
    /// leftovers — turning one understandable failure into a confusing second one.
    struct TempYaml {
        path: PathBuf,
    }

    impl TempYaml {
        fn new(stem: &str, contents: &str) -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let unique = NEXT.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("{stem}-{}-{unique}.yml", std::process::id()));
            std::fs::write(&path, contents).expect("the temp directory is writable");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempYaml {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).ok();
        }
    }

    /// The path the other three are the failure of. Without this one, nothing here would
    /// notice a `from_file` that never returned `Ok` at all — every assertion below is about
    /// an error, and a function that only ever errors satisfies all of them.
    #[test]
    fn a_file_that_is_a_configuration_loads_the_values_it_gave() {
        let file = TempYaml::new("vairedb-config-valid", "listen_addr: \"127.0.0.1:5432\"\n");

        let settings: Settings = from_file(file.path()).expect("the YAML describes a Settings");

        assert_eq!(settings.listen_addr, "127.0.0.1:5432");
    }

    /// A file that is not there is the variant an operator fixes with a path, and the message
    /// has to say which path was tried — the `io::Error` alone does not.
    #[test]
    fn a_missing_file_is_unreadable_and_names_the_path() {
        let err = from_file::<Settings>(Path::new("/nonexistent/vairedb-test.yml"))
            .expect_err("a missing file cannot be read");
        assert!(
            matches!(err, ConfigError::Unreadable { .. }),
            "got: {err:?}"
        );
        assert!(
            err.to_string().contains("/nonexistent/vairedb-test.yml"),
            "got: {err}"
        );
    }

    /// A file that was read and does not match is the other variant, and the field the
    /// operator has to add survives in the message.
    #[test]
    fn a_file_missing_a_field_is_invalid_and_names_the_field() {
        let file = TempYaml::new("vairedb-config-missing-field", "other_key: 1\n");

        let err = from_file::<Settings>(file.path()).expect_err("the YAML does not match Settings");

        assert!(matches!(err, ConfigError::Invalid { .. }), "got: {err:?}");
        let message = err.to_string();
        assert!(message.contains("listen_addr"), "got: {message}");
        assert!(
            message.contains("vairedb-config-missing-field"),
            "got: {message}"
        );
    }

    /// YAML that does not parse at all is the same variant as YAML that parses into the wrong
    /// shape: in both, the file was read and is not a configuration.
    #[test]
    fn unparseable_yaml_is_invalid_rather_than_unreadable() {
        let file = TempYaml::new("vairedb-config-unparseable", "{{{{ not yaml");

        let err = from_file::<Settings>(file.path()).expect_err("that is not YAML");

        assert!(matches!(err, ConfigError::Invalid { .. }), "got: {err:?}");
    }

    /// The cause, not only the sentence. `Display` is what a node prints on the way out, but
    /// a caller that walks the chain — `{:#}` through an `anyhow`-style wrapper, or a log
    /// layer that records `source()` — has to reach the `io` or `serde_yaml` error underneath
    /// rather than a dead end, which is the whole reason `source` is implemented above.
    #[test]
    fn both_variants_keep_their_cause_reachable() {
        let unreadable = from_file::<Settings>(Path::new("/nonexistent/vairedb-test.yml"))
            .expect_err("a missing file cannot be read");
        assert!(unreadable.source().is_some(), "got: {unreadable:?}");

        let file = TempYaml::new("vairedb-config-cause-chain", "other_key: 1\n");
        let invalid =
            from_file::<Settings>(file.path()).expect_err("the YAML does not match Settings");
        assert!(invalid.source().is_some(), "got: {invalid:?}");
    }
}
