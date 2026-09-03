//! Loads an operator "session" and replays it through the real Fabric
//! runtime pipeline.
//!
//! Fabric today has no persistent daemon or live-state service: the protocol
//! server (`facet-protocol`) acknowledges messages without retaining queryable
//! state. The honest, real capability that exists is the in-process analysis
//! pipeline in `fabric-runtime`: it ingests `FabricMessage` values, builds the
//! topology registry, workload profiles and latest-observation state, and can
//! run the optimizer over the result.
//!
//! This module reads a JSON array of `FabricMessage` values -- the exact
//! messages a Fabric node emits over the protocol -- and feeds each one through
//! `FabricRuntime::handle`, producing a fully populated runtime the CLI can
//! render. With no input file, the runtime is empty and every command reports
//! the empty state truthfully.

use std::io::Read;
use std::path::Path;

use fabric_protocol::FabricMessage;
use fabric_runtime::FabricRuntime;

/// Failure while loading or replaying a session.
#[derive(Debug)]
pub enum SessionError {
    Io { path: String, source: std::io::Error },
    Parse { path: String, source: serde_json::Error },
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "could not read session '{path}': {source}")
            }
            Self::Parse { path, source } => {
                write!(f, "invalid session '{path}': {source}")
            }
        }
    }
}

impl std::error::Error for SessionError {}

/// Build a runtime by replaying the messages at `path` (if any).
///
/// A `None` path yields a fresh, empty runtime. A path of `-` reads the
/// session from standard input, following the usual Unix convention.
pub fn load(path: Option<&Path>) -> Result<FabricRuntime, SessionError> {
    let mut runtime = FabricRuntime::new();

    let Some(path) = path else {
        return Ok(runtime);
    };

    let (label, bytes) = if path.as_os_str() == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .map_err(|source| SessionError::Io {
                path: "<stdin>".to_string(),
                source,
            })?;
        ("<stdin>".to_string(), buf)
    } else {
        let bytes = std::fs::read(path).map_err(|source| SessionError::Io {
            path: path.display().to_string(),
            source,
        })?;
        (path.display().to_string(), bytes)
    };

    let messages: Vec<FabricMessage> =
        serde_json::from_slice(&bytes).map_err(|source| SessionError::Parse {
            path: label,
            source,
        })?;

    for message in messages {
        // Return value is a protocol acknowledgement; the durable effect is the
        // mutation of runtime state, which is what the CLI renders.
        let _ = runtime.handle(message);
    }

    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    /// Load and expect failure. `FabricRuntime` is not `Debug`, so `unwrap_err`
    /// is unavailable; extract the error by hand.
    fn load_err(path: &Path) -> SessionError {
        match load(Some(path)) {
            Ok(_) => panic!("expected load to fail for {}", path.display()),
            Err(err) => err,
        }
    }

    /// Write `contents` to a unique scratch file and return its path.
    fn scratch(contents: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir()
            .join(format!("fabric-cli-session-{nanos}.json"));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn no_path_yields_empty_runtime() {
        let runtime = load(None).unwrap();
        assert_eq!(runtime.topology().len(), 0);
        assert_eq!(runtime.analyzer().len(), 0);
        assert_eq!(runtime.state().len(), 0);
    }

    #[test]
    fn empty_array_is_valid_and_empty() {
        let path = scratch("[]");
        let runtime = load(Some(&path)).unwrap();
        assert_eq!(runtime.topology().len(), 0);
        assert_eq!(runtime.analyzer().len(), 0);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn missing_file_is_io_error() {
        let path = std::env::temp_dir().join("fabric-cli-does-not-exist.json");
        let err = load_err(&path);
        assert!(matches!(err, SessionError::Io { .. }));
        assert!(err.to_string().contains("could not read session"));
    }

    #[test]
    fn malformed_json_is_parse_error() {
        let path = scratch("{ not valid json ");
        let err = load_err(&path);
        assert!(matches!(err, SessionError::Parse { .. }));
        assert!(err.to_string().contains("invalid session"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn unknown_message_variant_is_parse_error() {
        // Externally-tagged enum: an unrecognized variant name is rejected by
        // serde rather than silently dropped.
        let path = scratch(r#"[ { "Nonsense": { "node_id": "x" } } ]"#);
        let err = load_err(&path);
        assert!(matches!(err, SessionError::Parse { .. }));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn example_session_populates_runtime() {
        // The shipped example exercises Topology + Telemetry ingestion.
        let path = Path::new("examples/session.json");
        let runtime = load(Some(path)).unwrap();
        assert_eq!(runtime.topology().len(), 2);
        assert_eq!(runtime.analyzer().len(), 2);
    }
}
