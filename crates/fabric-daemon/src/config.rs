//! What an operator declares, and what the daemon refuses to start without.
//!
//! Two types, on purpose. [`ConfigFile`] is the JSON an operator writes:
//! strings, optional everywhere a default is honest, and
//! `deny_unknown_fields` so a misspelled key is a startup error rather than a
//! setting that silently did nothing. [`Settings`] is what the daemon runs on:
//! parsed addresses, a validated [`Keyspace`], a declared
//! [`TopologyRegistry`], and the secrets actually loaded.
//!
//! # Secrets are never in the file
//!
//! A backend's API token and the admin token are named by *environment
//! variable* in the configuration and read from the environment at startup.
//! That is the posture [`fabric_facetql::FacetqlEndpoint::from_env`] already
//! takes, and the reason is the same: a config file gets copied into a ticket,
//! a repository and a container image, and a token in it is a token in all
//! three. A missing variable reports the variable's *name*, never a value.
//!
//! # Everything checkable is checked before the first request
//!
//! A keyspace naming a cell no backend holds, a placement outside the grid, a
//! placement store on a backend with no credential: each is a configuration
//! fault whose only symptom at request time is a `421` or a `503` on some
//! subset of traffic. They are all decided here, at startup, where the whole
//! declaration is visible at once.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;

use fabric_controller::ControllerPolicy;
use fabric_core::{Coordinate, DbmsId, Shard};
use fabric_facetql::frontdoor::{Backend, FrontDoorConfig, Keyspace, KeyspaceRule};
use fabric_routing::{ReadPreference, RoutingKey};
use fabric_topology::TopologyRegistry;
use serde::Deserialize;

/// Default client-facing port. Nothing above the front door has to change to
/// adopt it: `FACET_DATABASE_URL` points here instead of at one FacetQL.
pub const DEFAULT_DATA_LISTEN: &str = "0.0.0.0:7710";

/// Default operator port, bound to loopback: the admin surface reports the
/// fleet's shape and accepts reports about work in flight, and neither belongs
/// on a public interface by default.
pub const DEFAULT_ADMIN_LISTEN: &str = "127.0.0.1:7711";

/// Environment variable holding the admin token, unless the file names another.
pub const DEFAULT_ADMIN_TOKEN_ENV: &str = "FABRIC_ADMIN_TOKEN";

/// Why the daemon would not start.
#[derive(Debug)]
pub enum ConfigError {
    Read { path: String, error: String },
    Parse { path: String, error: String },
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, error } => {
                write!(f, "could not read the configuration at {path}: {error}")
            }

            Self::Parse { path, error } => {
                write!(f, "could not parse the configuration at {path}: {error}")
            }

            Self::Invalid(reason) => write!(f, "{reason}"),
        }
    }
}

impl std::error::Error for ConfigError {}

fn invalid(reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(reason.into())
}

// ─────────────────────────────────────────────────────── the declared file

/// One grid cell, as an operator names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellRef {
    pub shard: u64,
    pub x: u8,
    pub y: u8,
}

impl CellRef {
    fn coordinate(self) -> Coordinate {
        Coordinate::new(self.x, self.y)
    }

    fn routing_key(self) -> Result<RoutingKey, ConfigError> {
        RoutingKey::new(self.shard, self.coordinate()).map_err(|error| {
            invalid(format!(
                "cell shard {} ({},{}) is not addressable: {error}",
                self.shard, self.x, self.y
            ))
        })
    }
}

impl std::fmt::Display for CellRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard {} ({},{})", self.shard, self.x, self.y)
    }
}

/// One FacetQL instance this daemon is responsible for.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendFile {
    pub id: String,
    pub url: String,

    #[serde(default = "default_region")]
    pub region: String,

    /// Environment variable holding this instance's control-plane API token.
    ///
    /// Optional, and its absence is not a failure: the data path forwards the
    /// *client's* credential and needs none of its own. Without it this
    /// instance produces no `/stats` telemetry and cannot hold the placement
    /// store, and the daemon says so at startup rather than reporting a
    /// silently unmonitored instance as healthy.
    #[serde(default)]
    pub token_env: Option<String>,

    /// The cells this instance holds, as declared by the operator.
    #[serde(default)]
    pub placements: Vec<CellRef>,
}

fn default_region() -> String {
    "default".to_string()
}

/// One operator-declared correspondence between FacetQL's namespace and a
/// Fabric cell. Both halves are mandatory; see
/// [`fabric_facetql::frontdoor::keyspace`] for why.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyspaceRuleFile {
    pub kind: String,
    pub address_prefix: String,
    pub shard: u64,
    pub x: u8,
    pub y: u8,
}

impl KeyspaceRuleFile {
    fn cell(&self) -> CellRef {
        CellRef {
            shard: self.shard,
            x: self.x,
            y: self.y,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyspaceFile {
    #[serde(default)]
    pub rules: Vec<KeyspaceRuleFile>,

    /// Where anything no rule covers goes. Without one, an unmapped request is
    /// refused rather than sent somewhere plausible.
    #[serde(default)]
    pub fallback: Option<CellRef>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CadenceFile {
    /// How often every backend is probed with `GET /`.
    #[serde(default = "default_probe_ms")]
    pub liveness_probe_ms: u64,

    /// How often `/stats` is sampled from every credentialed backend.
    #[serde(default = "default_telemetry_ms")]
    pub telemetry_poll_ms: u64,

    /// How often the control loop runs a full cycle.
    #[serde(default = "default_cycle_ms")]
    pub control_cycle_ms: u64,
}

fn default_probe_ms() -> u64 {
    5_000
}

fn default_telemetry_ms() -> u64 {
    15_000
}

fn default_cycle_ms() -> u64 {
    5_000
}

impl Default for CadenceFile {
    fn default() -> Self {
        Self {
            liveness_probe_ms: default_probe_ms(),
            telemetry_poll_ms: default_telemetry_ms(),
            control_cycle_ms: default_cycle_ms(),
        }
    }
}

/// The controller's thresholds. Every field is optional and falls back to
/// [`ControllerPolicy::default`], which is deliberately conservative.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyFile {
    pub max_decision_age_ms: Option<u64>,
    pub min_confidence: Option<f64>,
    pub min_replicas: Option<usize>,
    pub max_node_utilization: Option<f64>,
    pub max_concurrent_actions: Option<usize>,
    pub max_actions_per_node: Option<usize>,
    pub phase_timeout_ms: Option<u64>,
    pub measurement_settle_ms: Option<u64>,
    pub measurement_deadline_ms: Option<u64>,
    pub outcome_noise_floor: Option<f64>,
}

impl PolicyFile {
    fn resolve(&self) -> ControllerPolicy {
        let base = ControllerPolicy::default();

        ControllerPolicy {
            max_decision_age_ms: self.max_decision_age_ms.unwrap_or(base.max_decision_age_ms),
            min_confidence: self.min_confidence.unwrap_or(base.min_confidence),
            min_replicas: self.min_replicas.unwrap_or(base.min_replicas),
            max_node_utilization: self
                .max_node_utilization
                .unwrap_or(base.max_node_utilization),
            max_concurrent_actions: self
                .max_concurrent_actions
                .unwrap_or(base.max_concurrent_actions),
            max_actions_per_node: self
                .max_actions_per_node
                .unwrap_or(base.max_actions_per_node),
            phase_timeout_ms: self.phase_timeout_ms.unwrap_or(base.phase_timeout_ms),
            measurement_settle_ms: self
                .measurement_settle_ms
                .unwrap_or(base.measurement_settle_ms),
            measurement_deadline_ms: self
                .measurement_deadline_ms
                .unwrap_or(base.measurement_deadline_ms),
            outcome_noise_floor: self.outcome_noise_floor.unwrap_or(base.outcome_noise_floor),
        }
    }
}

/// Which copy a read may be served from.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadPreferenceFile {
    Primary,
    AnyFresh,
    PreferRegion { region: String },
    AnyCopy,
}

impl From<&ReadPreferenceFile> for ReadPreference {
    fn from(preference: &ReadPreferenceFile) -> Self {
        match preference {
            ReadPreferenceFile::Primary => Self::Primary,
            ReadPreferenceFile::AnyFresh => Self::AnyFresh,
            ReadPreferenceFile::PreferRegion { region } => Self::PreferRegion {
                region: region.clone(),
            },
            ReadPreferenceFile::AnyCopy => Self::AnyCopy,
        }
    }
}

/// The whole declaration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    #[serde(default = "default_data_listen")]
    pub data_listen: String,

    #[serde(default = "default_admin_listen")]
    pub admin_listen: String,

    #[serde(default = "default_admin_token_env")]
    pub admin_token_env: String,

    pub backends: Vec<BackendFile>,

    #[serde(default)]
    pub keyspace: KeyspaceFile,

    #[serde(default)]
    pub cadence: CadenceFile,

    #[serde(default)]
    pub policy: PolicyFile,

    /// How long a node may stay silent before routing stops sending traffic to
    /// it. Defaults to three probe intervals: a missed probe is common, three
    /// in a row is not.
    #[serde(default)]
    pub silence_budget_ms: Option<u64>,

    /// How long a liveness probe may take before the instance counts as
    /// unreachable for that round.
    #[serde(default = "default_probe_timeout_ms")]
    pub probe_timeout_ms: u64,

    /// Placements a node is provisioned for. No protocol message reports it,
    /// so it is an operator input.
    #[serde(default = "default_placement_capacity")]
    pub placement_capacity: usize,

    #[serde(default)]
    pub read_preference: Option<ReadPreferenceFile>,

    /// The backend holding Fabric's durable placement state, by id.
    ///
    /// Without one the placement map is whatever this file declares, and a
    /// cell that moved is forgotten at restart — so the daemon would route to
    /// the instance that used to hold it. With one, the map is read back at
    /// startup and every completed move is written through.
    #[serde(default)]
    pub placement_store: Option<String>,

    /// How long shutdown may spend draining before the daemon gives up and
    /// says what it could not finish.
    #[serde(default = "default_drain_ms")]
    pub drain_ms: u64,

    /// How long after an action concludes on a cell before this daemon will
    /// propose another one for the same cell.
    ///
    /// The controller decides whether an action is *valid*; how often to ask is
    /// this process's business, and without a dead band it would ask on every
    /// cycle. A cell does not necessarily stop being hot because it moved --
    /// the workload moved with it -- so a loop with no cooldown would move it
    /// again the instant the verdict landed, and again after that: a control
    /// plane oscillating instead of controlling. The default is deliberately
    /// long relative to the measurement settle window, because the observation
    /// immediately after a move is of a system that is still settling.
    #[serde(default = "default_cooldown_ms")]
    pub decision_cooldown_ms: u64,
}

fn default_data_listen() -> String {
    DEFAULT_DATA_LISTEN.to_string()
}

fn default_admin_listen() -> String {
    DEFAULT_ADMIN_LISTEN.to_string()
}

fn default_admin_token_env() -> String {
    DEFAULT_ADMIN_TOKEN_ENV.to_string()
}

fn default_probe_timeout_ms() -> u64 {
    2_000
}

fn default_placement_capacity() -> usize {
    fabric_runtime::DEFAULT_PLACEMENT_CAPACITY
}

fn default_drain_ms() -> u64 {
    30_000
}

fn default_cooldown_ms() -> u64 {
    300_000
}

// ─────────────────────────────────────────────────────────── what it becomes

/// One backend, resolved: an identity, an address, the cells it holds, and —
/// only where the operator supplied one — a control-plane credential.
///
/// [`Debug`] is hand-written to redact the token, for the reason
/// [`fabric_facetql::FacetqlEndpoint`] does the same: a derived one prints the
/// credential into every log line, panic message and `dbg!` that ever touches
/// a struct containing it, and this struct is inside [`Settings`], which is
/// inside the control plane.
#[derive(Clone)]
pub struct ResolvedBackend {
    pub id: DbmsId,
    pub url: String,
    pub region: String,
    pub token: Option<String>,
    pub placements: Vec<CellRef>,
}

impl ResolvedBackend {
    /// The data-path identity: an address and no credential of its own.
    pub fn front_door_backend(&self) -> Result<Backend, ConfigError> {
        Backend::new(self.id.clone(), self.url.clone())
            .map_err(|error| invalid(error.to_string()))
    }
}

/// The validated configuration the daemon runs on.
///
/// [`Debug`] redacts the admin token; see [`ResolvedBackend`].
#[derive(Clone)]
pub struct Settings {
    pub data_listen: SocketAddr,
    pub admin_listen: SocketAddr,
    pub admin_token: String,
    pub backends: Vec<ResolvedBackend>,
    pub keyspace: Keyspace,
    pub topology: TopologyRegistry,
    pub front_door: FrontDoorConfig,
    pub cadence: CadenceFile,
    pub policy: ControllerPolicy,
    pub silence_budget_ms: u64,
    pub probe_timeout_ms: u64,
    pub placement_capacity: usize,
    pub placement_store: Option<DbmsId>,
    pub drain_ms: u64,
    pub decision_cooldown_ms: u64,
}

impl Settings {
    /// Read a configuration file and resolve it against the environment.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|error| ConfigError::Read {
            path: path.display().to_string(),
            error: error.to_string(),
        })?;

        let file: ConfigFile =
            serde_json::from_str(&text).map_err(|error| ConfigError::Parse {
                path: path.display().to_string(),
                error: error.to_string(),
            })?;

        Self::resolve(file, &EnvVars)
    }

    /// Resolve a parsed declaration, reading every secret from `env`.
    ///
    /// The environment is a parameter so the whole of validation — including
    /// what happens when a token is missing — is testable without touching the
    /// process environment, which is global and shared with every other test.
    pub fn resolve(file: ConfigFile, env: &dyn Env) -> Result<Self, ConfigError> {
        let data_listen = parse_listen("data_listen", &file.data_listen)?;
        let admin_listen = parse_listen("admin_listen", &file.admin_listen)?;

        let admin_token = env.get(&file.admin_token_env).filter(|token| !token.is_empty()).ok_or_else(|| {
            invalid(format!(
                "environment variable {} is not set: the admin surface reports \
                 the fleet's shape and accepts reports about work in flight, so \
                 it is never served unauthenticated",
                file.admin_token_env
            ))
        })?;

        if file.backends.is_empty() {
            return Err(invalid(
                "no backends are declared: a front door with nothing behind it \
                 can serve nothing",
            ));
        }

        let mut seen_ids: BTreeSet<String> = BTreeSet::new();
        let mut backends: Vec<ResolvedBackend> = Vec::with_capacity(file.backends.len());
        let mut topology = TopologyRegistry::new();
        let mut holders: BTreeMap<(u64, usize), String> = BTreeMap::new();

        for backend in &file.backends {
            if backend.id.trim().is_empty() {
                return Err(invalid("a backend with an empty id cannot be routed to"));
            }

            if !seen_ids.insert(backend.id.clone()) {
                return Err(invalid(format!(
                    "backend '{}' is declared twice; two addresses for one \
                     identity is two different fleets",
                    backend.id
                )));
            }

            let id = DbmsId::new(backend.id.clone());

            let token = match &backend.token_env {
                None => None,

                Some(variable) => Some(
                    env.get(variable)
                        .filter(|token| !token.is_empty())
                        .ok_or_else(|| {
                            invalid(format!(
                                "environment variable {variable} is not set: no \
                                 API token for FacetQL instance '{}'",
                                backend.id
                            ))
                        })?,
                ),
            };

            for cell in &backend.placements {
                if !cell.coordinate().is_valid() {
                    return Err(invalid(format!(
                        "backend '{}' declares {cell}, which is outside the \
                         {}x{} grid",
                        backend.id,
                        fabric_core::GRID_WIDTH,
                        fabric_core::GRID_HEIGHT
                    )));
                }

                let slot = (cell.shard, cell.coordinate().index());

                if let Some(other) = holders.get(&slot) {
                    return Err(invalid(format!(
                        "{cell} is declared on both '{other}' and '{}'; a cell \
                         has exactly one holder, and moving it is a decision \
                         this daemon makes, not a thing declared twice",
                        backend.id
                    )));
                }

                holders.insert(slot, backend.id.clone());

                topology.place(
                    id.clone(),
                    &Shard::new(cell.shard, backend.region.clone()),
                    cell.coordinate(),
                    backend.region.clone(),
                );
            }

            let resolved = ResolvedBackend {
                id,
                url: backend.url.clone(),
                region: backend.region.clone(),
                token,
                placements: backend.placements.clone(),
            };

            // Rejects a URL that is not http(s) here rather than on the first
            // proxied request.
            resolved.front_door_backend()?;

            backends.push(resolved);
        }

        let keyspace = resolve_keyspace(&file.keyspace, &holders)?;

        let placement_store = match &file.placement_store {
            None => None,

            Some(name) => {
                let backend = backends
                    .iter()
                    .find(|backend| backend.id.0 == *name)
                    .ok_or_else(|| {
                        invalid(format!(
                            "placement_store names '{name}', which is not a \
                             declared backend"
                        ))
                    })?;

                if backend.token.is_none() {
                    return Err(invalid(format!(
                        "placement_store is '{name}', but that backend declares \
                         no token_env: Fabric's own control state is written \
                         with Fabric's own credential, never the client's"
                    )));
                }

                Some(backend.id.clone())
            }
        };

        let cadence = file.cadence.clone();

        for (name, value) in [
            ("liveness_probe_ms", cadence.liveness_probe_ms),
            ("telemetry_poll_ms", cadence.telemetry_poll_ms),
            ("control_cycle_ms", cadence.control_cycle_ms),
        ] {
            if value == 0 {
                return Err(invalid(format!("{name} must be greater than zero")));
            }
        }

        let silence_budget_ms = file
            .silence_budget_ms
            .unwrap_or_else(|| cadence.liveness_probe_ms.saturating_mul(3));

        if silence_budget_ms < cadence.liveness_probe_ms {
            return Err(invalid(format!(
                "silence_budget_ms ({silence_budget_ms}) is shorter than one \
                 probe interval ({}): every instance would be declared \
                 unreachable between probes",
                cadence.liveness_probe_ms
            )));
        }

        let front_door = FrontDoorConfig {
            read_preference: file
                .read_preference
                .as_ref()
                .map(ReadPreference::from)
                .unwrap_or(ReadPreference::Primary),
            ..FrontDoorConfig::default()
        };

        Ok(Self {
            data_listen,
            admin_listen,
            admin_token,
            backends,
            keyspace,
            topology,
            front_door,
            cadence,
            policy: file.policy.resolve(),
            silence_budget_ms,
            probe_timeout_ms: file.probe_timeout_ms.max(1),
            placement_capacity: file.placement_capacity.max(1),
            placement_store,
            drain_ms: file.drain_ms,
            decision_cooldown_ms: file.decision_cooldown_ms,
        })
    }

    /// The backends, as the front door addresses them.
    pub fn front_door_backends(&self) -> Result<Vec<Backend>, ConfigError> {
        self.backends
            .iter()
            .map(ResolvedBackend::front_door_backend)
            .collect()
    }

    pub fn backend(&self, id: &DbmsId) -> Option<&ResolvedBackend> {
        self.backends.iter().find(|backend| &backend.id == id)
    }
}

impl std::fmt::Debug for ResolvedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedBackend")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("region", &self.region)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("placements", &self.placements)
            .finish()
    }
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("data_listen", &self.data_listen)
            .field("admin_listen", &self.admin_listen)
            .field("admin_token", &"<redacted>")
            .field("backends", &self.backends)
            .field("keyspace", &self.keyspace)
            .field("topology", &self.topology)
            .field("front_door", &self.front_door)
            .field("cadence", &self.cadence)
            .field("policy", &self.policy)
            .field("silence_budget_ms", &self.silence_budget_ms)
            .field("probe_timeout_ms", &self.probe_timeout_ms)
            .field("placement_capacity", &self.placement_capacity)
            .field("placement_store", &self.placement_store)
            .field("drain_ms", &self.drain_ms)
            .field("decision_cooldown_ms", &self.decision_cooldown_ms)
            .finish()
    }
}

/// Every cell the keyspace can name must be a cell some backend holds.
///
/// Without this check the first request for an undeclared kind is answered
/// with a `421` — the honest status for "the keyspace names a place routing
/// has never heard of" — for as long as nobody notices. It is configuration,
/// it is decidable here, and a control plane that discovers it one failed
/// request at a time has already failed those requests.
fn resolve_keyspace(
    file: &KeyspaceFile,
    holders: &BTreeMap<(u64, usize), String>,
) -> Result<Keyspace, ConfigError> {
    let mut keyspace = Keyspace::new();

    let placed = |cell: CellRef| -> Result<(), ConfigError> {
        if holders.contains_key(&(cell.shard, cell.coordinate().index())) {
            return Ok(());
        }

        Err(invalid(format!(
            "the keyspace routes to {cell}, which no backend declares a \
             placement for: every request that resolved there would be \
             misdirected"
        )))
    };

    for rule in &file.rules {
        placed(rule.cell())?;

        let key = rule.cell().routing_key()?;

        keyspace = keyspace
            .with_rule(
                KeyspaceRule::new(rule.kind.clone(), rule.address_prefix.clone(), key)
                    .map_err(|error| invalid(error.to_string()))?,
            )
            .map_err(|error| invalid(error.to_string()))?;
    }

    if let Some(fallback) = file.fallback {
        placed(fallback)?;
        keyspace = keyspace.with_fallback(fallback.routing_key()?);
    }

    if file.rules.is_empty() && file.fallback.is_none() {
        return Err(invalid(
            "the keyspace declares no rules and no fallback, so no request has \
             a route: declare a fallback for the single-instance case",
        ));
    }

    Ok(keyspace)
}

fn parse_listen(field: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse()
        .map_err(|error| invalid(format!("{field} '{value}' is not an address: {error}")))
}

/// Where secrets are read from.
pub trait Env {
    fn get(&self, name: &str) -> Option<String>;
}

/// The process environment.
pub struct EnvVars;

impl Env for EnvVars {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// A fixed map, for tests and for resolving a declaration without touching the
/// process environment.
pub struct MapEnv(pub BTreeMap<String, String>);

impl MapEnv {
    pub fn of(pairs: &[(&str, &str)]) -> Self {
        Self(
            pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        )
    }
}

impl Env for MapEnv {
    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"{
        "backends": [
            {
                "id": "db-a",
                "url": "http://db-a:8892",
                "region": "us-east",
                "placements": [{ "shard": 1, "x": 0, "y": 0 }]
            }
        ],
        "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } }
    }"#;

    fn env() -> MapEnv {
        MapEnv::of(&[("FABRIC_ADMIN_TOKEN", "admin-secret")])
    }

    fn parse(text: &str) -> ConfigFile {
        serde_json::from_str(text).expect("valid configuration")
    }

    fn settings(text: &str) -> Settings {
        Settings::resolve(parse(text), &env()).expect("a valid declaration")
    }

    fn refusal(text: &str) -> String {
        Settings::resolve(parse(text), &env())
            .expect_err("expected a refusal")
            .to_string()
    }

    #[test]
    fn a_minimal_declaration_resolves_to_a_servable_fleet() {
        let settings = settings(MINIMAL);

        assert_eq!(settings.data_listen.to_string(), DEFAULT_DATA_LISTEN);
        assert_eq!(settings.admin_listen.to_string(), DEFAULT_ADMIN_LISTEN);
        assert_eq!(settings.admin_token, "admin-secret");
        assert_eq!(settings.backends.len(), 1);
        assert!(settings.backends[0].token.is_none());
        assert_eq!(settings.topology.len(), 1);
        assert_eq!(
            settings.keyspace.spanning_key(),
            Some(RoutingKey::new(1, Coordinate::new(0, 0)).unwrap())
        );

        // The silence budget follows the probe interval rather than a constant
        // that has to be remembered when the interval changes.
        assert_eq!(
            settings.silence_budget_ms,
            settings.cadence.liveness_probe_ms * 3
        );
    }

    /// The whole point of resolving the keyspace against the declared
    /// placements: a rule pointing at a cell nobody holds is answerable only
    /// with a 421, and it is decidable before the first request.
    #[test]
    fn a_keyspace_rule_for_an_unheld_cell_is_refused_at_startup() {
        let message = refusal(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://db-a:8892",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": {
                    "rules": [
                        { "kind": "Post", "address_prefix": "Post:",
                          "shard": 9, "x": 4, "y": 4 }
                    ],
                    "fallback": { "shard": 1, "x": 0, "y": 0 }
                }
            }"#,
        );

        assert!(message.contains("shard 9 (4,4)"), "{message}");
        assert!(message.contains("no backend declares"), "{message}");
    }

    #[test]
    fn one_cell_may_not_be_declared_on_two_instances() {
        let message = refusal(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] },
                    { "id": "db-b", "url": "http://b:1",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } }
            }"#,
        );

        assert!(message.contains("exactly one holder"), "{message}");
    }

    #[test]
    fn a_missing_secret_names_the_variable_and_never_a_value() {
        let file = parse(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "token_env": "FABRIC_DB_A_TOKEN",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } }
            }"#,
        );

        let message = Settings::resolve(file, &env()).unwrap_err().to_string();

        assert!(message.contains("FABRIC_DB_A_TOKEN"), "{message}");
        assert!(message.contains("db-a"), "{message}");
    }

    #[test]
    fn the_admin_surface_is_never_served_without_a_token() {
        let message = Settings::resolve(parse(MINIMAL), &MapEnv::of(&[]))
            .unwrap_err()
            .to_string();

        assert!(message.contains("FABRIC_ADMIN_TOKEN"), "{message}");
        assert!(message.contains("unauthenticated"), "{message}");
    }

    #[test]
    fn a_placement_store_without_a_credential_is_refused() {
        let message = refusal(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } },
                "placement_store": "db-a"
            }"#,
        );

        assert!(message.contains("token_env"), "{message}");
    }

    #[test]
    fn a_silence_budget_shorter_than_a_probe_interval_is_refused() {
        let message = refusal(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } },
                "cadence": { "liveness_probe_ms": 5000 },
                "silence_budget_ms": 1000
            }"#,
        );

        assert!(message.contains("between probes"), "{message}");
    }

    /// A misspelled key that is silently ignored is a setting an operator
    /// believes is in force and is not.
    #[test]
    fn an_unknown_key_is_a_startup_error() {
        let error = serde_json::from_str::<ConfigFile>(
            r#"{ "backends": [], "keysapce": {} }"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("keysapce"), "{error}");
    }

    /// The configuration the repository ships and the parser that reads it
    /// have to be the same thing. A shipped example that no longer resolves is
    /// how an operator's first `docker compose up` fails.
    #[test]
    fn the_shipped_deployment_configuration_still_resolves() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/fabric.json");

        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        let file: ConfigFile = serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        let settings = Settings::resolve(file, &env())
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        assert_eq!(settings.backends.len(), 2);

        // Both instances are addressable and probed; the namespace lives in
        // one place, which is what keeps the door transparent before anything
        // has been split.
        assert!(settings.keyspace.spanning_key().is_some());

        // Neither declares a credential, so neither produces telemetry. The
        // daemon says so on the admin port rather than showing an unmonitored
        // instance as a healthy one.
        assert!(settings.backends.iter().all(|backend| backend.token.is_none()));
    }

    /// A derived `Debug` on a struct holding a bearer token puts that token in
    /// every panic message and every `dbg!` anybody ever writes near it.
    #[test]
    fn debug_never_prints_a_secret() {
        let file = parse(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "token_env": "FABRIC_DB_A_TOKEN",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } }
            }"#,
        );

        let settings = Settings::resolve(
            file,
            &MapEnv::of(&[
                ("FABRIC_ADMIN_TOKEN", "admin-s3cret"),
                ("FABRIC_DB_A_TOKEN", "backend-s3cret"),
            ]),
        )
        .expect("a valid declaration");

        let rendered = format!("{settings:?}");

        assert!(!rendered.contains("admin-s3cret"), "{rendered}");
        assert!(!rendered.contains("backend-s3cret"), "{rendered}");
        assert!(rendered.contains("<redacted>"));

        // The token is still there to be used; it is only unprintable.
        assert_eq!(settings.admin_token, "admin-s3cret");
        assert_eq!(settings.backends[0].token.as_deref(), Some("backend-s3cret"));
    }

    #[test]
    fn policy_thresholds_come_from_the_file_and_default_conservatively() {
        let settings = settings(
            r#"{
                "backends": [
                    { "id": "db-a", "url": "http://a:1",
                      "placements": [{ "shard": 1, "x": 0, "y": 0 }] }
                ],
                "keyspace": { "fallback": { "shard": 1, "x": 0, "y": 0 } },
                "policy": { "phase_timeout_ms": 9000 }
            }"#,
        );

        assert_eq!(settings.policy.phase_timeout_ms, 9_000);
        assert_eq!(
            settings.policy.measurement_settle_ms,
            ControllerPolicy::default().measurement_settle_ms
        );
    }
}
