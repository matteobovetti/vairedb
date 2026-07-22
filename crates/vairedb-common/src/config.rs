//! YAML configuration loading shared by both node binaries.

use std::path::Path;

use serde::de::DeserializeOwned;

/// Read the file at `path` and deserialize its YAML contents into `T`.
///
/// Returns an error if the file cannot be read or the YAML does not match `T`.
pub fn from_file<T: DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let config: T = serde_yaml::from_str(&contents)?;
    Ok(config)
}
