//! The declared map from FacetQL's namespace to Fabric's routing keys.
//!
//! Routing is a lookup against declared state, never a computation over an
//! identity (`fabric_routing`'s first doc line, README §2). The front door
//! therefore does **not** hash an address into a grid cell, and it does not
//! infer a node's kind from its address: it is *told*, once, by the operator,
//! and every request is answered out of that table.
//!
//! # Why a rule carries both a kind and an address prefix
//!
//! FacetQL's request surface addresses the same data two incompatible ways:
//!
//! * by **kind** — `GET /nodes?kind=`, `POST /nodes/query`, `/nodes/count`,
//!   `/nodes/count_by`, and the `clear_kind` / `delete_where` transaction ops;
//! * by **address** — `GET|PUT|DELETE /node/:address`, `/claim`, `/history`,
//!   `/owned`, `/edges/*`, `/nodes/multiget`, and the `delete_node` /
//!   `set_if` / `insert_edge` / `delete_edge` ops.
//!
//! An address does **not** carry its kind, and none of the address-addressed
//! endpoints takes a kind parameter (see `facetql/src/api/routes.rs`). So a
//! router cannot learn from FacetQL which kind `Post:1` is without reading the
//! node — and reading it requires already knowing which backend holds it. The
//! stack's addresses happen to be `kind:id`-shaped today, but FacetQL's own
//! `DeleteEdgeRequest` doc says plainly that "nothing enforces that", and
//! building routing on an unenforced convention would be inventing semantics
//! adapter-side, which the integration plan forbids (Finding A).
//!
//! The honest resolution is to make the correspondence an **operator
//! assertion** rather than a guess. A [`KeyspaceRule`] states: *kind `K` lives
//! at addresses beginning `P`, and both resolve to routing key `X`.* Both
//! halves are mandatory, because a rule with only one half would answer one of
//! the two addressing modes and silently fall through to the fallback for the
//! other — writing a node where no read of it will ever look.
//!
//! [`Keyspace::resolve`] then enforces the assertion on every request that
//! carries both: a `POST /node` whose `kind` and `address` disagree about
//! where it belongs is refused at the door instead of being written somewhere
//! its own kind's queries will never see.

use fabric_routing::RoutingKey;

/// One operator-declared correspondence between FacetQL's namespace and a
/// Fabric routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceRule {
    kind: String,
    address_prefix: String,
    key: RoutingKey,
}

impl KeyspaceRule {
    /// Declare that `kind`, and every address beginning `address_prefix`,
    /// route to `key`.
    ///
    /// Both halves are required and neither may be empty — an empty prefix
    /// matches every address, which is what [`Keyspace::with_fallback`] is
    /// for, and a rule that silently became the fallback would shadow every
    /// other rule depending on declaration order.
    pub fn new(
        kind: impl Into<String>,
        address_prefix: impl Into<String>,
        key: RoutingKey,
    ) -> Result<Self, KeyspaceError> {
        let kind = kind.into();
        let address_prefix = address_prefix.into();

        if kind.is_empty() {
            return Err(KeyspaceError::EmptyKind);
        }

        if address_prefix.is_empty() {
            return Err(KeyspaceError::EmptyAddressPrefix { kind });
        }

        Ok(Self {
            kind,
            address_prefix,
            key,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn address_prefix(&self) -> &str {
        &self.address_prefix
    }

    pub fn key(&self) -> RoutingKey {
        self.key
    }
}

/// What is wrong with a declared keyspace.
///
/// Every variant is a startup-time configuration fault, never a runtime
/// condition: a keyspace that cannot answer unambiguously must be refused
/// before it is serving traffic, not discovered one misrouted write at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyspaceError {
    EmptyKind,
    EmptyAddressPrefix { kind: String },
    DuplicateKind { kind: String },
    DuplicateAddressPrefix { address_prefix: String },
}

impl std::fmt::Display for KeyspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyKind => write!(f, "a keyspace rule must name a kind"),

            Self::EmptyAddressPrefix { kind } => write!(
                f,
                "the keyspace rule for kind '{kind}' must name a non-empty \
                 address prefix; use a fallback for 'everything else'"
            ),

            Self::DuplicateKind { kind } => write!(
                f,
                "kind '{kind}' is declared by two keyspace rules"
            ),

            Self::DuplicateAddressPrefix { address_prefix } => write!(
                f,
                "address prefix '{address_prefix}' is declared by two keyspace rules"
            ),
        }
    }
}

impl std::error::Error for KeyspaceError {}

/// Why a request could not be turned into one routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyspaceMiss {
    /// Nothing in the keyspace covers this kind or address, and there is no
    /// fallback. The front door does not guess and does not fan out: a fan-out
    /// read would return a partial answer that looks complete, and a fan-out
    /// write would put the node on every backend.
    Unmapped { kind: Option<String>, address: Option<String> },

    /// The request named both a kind and an address, and the keyspace says
    /// they live in different places. The operator's own assertion is being
    /// violated by this request; writing it would put the node where its
    /// kind's queries will never look.
    Inconsistent {
        kind: String,
        address: String,
        by_kind: RoutingKey,
        by_address: RoutingKey,
    },
}

impl std::fmt::Display for KeyspaceMiss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unmapped { kind, address } => {
                write!(f, "no keyspace rule covers ")?;

                match (kind, address) {
                    (Some(kind), Some(address)) => {
                        write!(f, "kind '{kind}' or address '{address}'")
                    }
                    (Some(kind), None) => write!(f, "kind '{kind}'"),
                    (None, Some(address)) => write!(f, "address '{address}'"),
                    (None, None) => write!(f, "this request, which names neither a kind nor an address"),
                }?;

                write!(f, ", and no fallback is declared")
            }

            Self::Inconsistent {
                kind,
                address,
                by_kind,
                by_address,
            } => write!(
                f,
                "address '{address}' routes to {by_address} but its kind \
                 '{kind}' routes to {by_kind}; the keyspace says these are \
                 different places, so this request cannot be served without \
                 putting the node where its own kind's queries will not look"
            ),
        }
    }
}

/// The declared namespace map. Immutable once built.
#[derive(Debug, Clone, Default)]
pub struct Keyspace {
    rules: Vec<KeyspaceRule>,
    fallback: Option<RoutingKey>,
}

impl Keyspace {
    pub fn new() -> Self {
        Self::default()
    }

    /// A keyspace with no rules at all, where everything routes to one key.
    ///
    /// This is the honest shape of the single-shard deployment — one FacetQL
    /// behind the front door — and it is what makes the front door adoptable
    /// before anything has been split.
    pub fn single(key: RoutingKey) -> Self {
        Self {
            rules: Vec::new(),
            fallback: Some(key),
        }
    }

    /// Add a rule, refusing one that would make a lookup ambiguous.
    pub fn with_rule(mut self, rule: KeyspaceRule) -> Result<Self, KeyspaceError> {
        if self.rules.iter().any(|existing| existing.kind == rule.kind) {
            return Err(KeyspaceError::DuplicateKind { kind: rule.kind });
        }

        if self
            .rules
            .iter()
            .any(|existing| existing.address_prefix == rule.address_prefix)
        {
            return Err(KeyspaceError::DuplicateAddressPrefix {
                address_prefix: rule.address_prefix,
            });
        }

        self.rules.push(rule);
        Ok(self)
    }

    /// Where anything no rule covers goes. Without one, an unmapped request is
    /// refused rather than sent somewhere plausible.
    pub fn with_fallback(mut self, key: RoutingKey) -> Self {
        self.fallback = Some(key);
        self
    }

    pub fn rules(&self) -> &[KeyspaceRule] {
        &self.rules
    }

    pub fn fallback(&self) -> Option<RoutingKey> {
        self.fallback
    }

    /// The one key that answers for the *whole* namespace, when there is one.
    ///
    /// Some FacetQL requests are not scoped to a kind or an address at all —
    /// `GET /nodes` with no `kind`, `POST /nodes/query` with no `kind`,
    /// `GET /stats`, `POST /admin/users`. Split across several backends none
    /// of them has a truthful single answer, and answering from one backend
    /// anyway would return a partial result that looks complete.
    ///
    /// But a keyspace whose every rule and whose fallback all name the same
    /// key — the ordinary "one FacetQL behind the front door" deployment, and
    /// any fleet that has not actually been split yet — *does* have one
    /// truthful answer, and refusing it would make the front door a
    /// regression rather than a transparent stand-in. So the question is asked
    /// here, exactly, rather than assumed either way.
    ///
    /// A fallback is required: without one an address matching no rule has no
    /// home, so the namespace is not covered by any single key.
    pub fn spanning_key(&self) -> Option<RoutingKey> {
        let fallback = self.fallback?;

        self.rules
            .iter()
            .all(|rule| rule.key == fallback)
            .then_some(fallback)
    }

    /// The key a kind is declared at.
    pub fn for_kind(&self, kind: &str) -> Option<RoutingKey> {
        self.rules
            .iter()
            .find(|rule| rule.kind == kind)
            .map(|rule| rule.key)
    }

    /// The key an address is declared at: longest declared prefix wins, so a
    /// rule for `__fabric_placement:` beats one for `__` without the operator
    /// having to think about declaration order.
    pub fn for_address(&self, address: &str) -> Option<RoutingKey> {
        self.rules
            .iter()
            .filter(|rule| address.starts_with(&rule.address_prefix))
            .max_by_key(|rule| rule.address_prefix.len())
            .map(|rule| rule.key)
    }

    /// The one lookup. Every request class funnels through here.
    ///
    /// A request that names both a kind and an address must agree with itself;
    /// see the module docs for why that check is the point of the rule shape
    /// rather than a nicety.
    pub fn resolve(
        &self,
        kind: Option<&str>,
        address: Option<&str>,
    ) -> Result<RoutingKey, KeyspaceMiss> {
        let by_kind = kind.and_then(|kind| self.for_kind(kind));
        let by_address = address.and_then(|address| self.for_address(address));

        match (by_kind, by_address) {
            (Some(by_kind), Some(by_address)) if by_kind != by_address => {
                Err(KeyspaceMiss::Inconsistent {
                    kind: kind.unwrap_or_default().to_string(),
                    address: address.unwrap_or_default().to_string(),
                    by_kind,
                    by_address,
                })
            }

            (Some(key), _) | (None, Some(key)) => Ok(key),

            (None, None) => self.fallback.ok_or_else(|| KeyspaceMiss::Unmapped {
                kind: kind.map(str::to_string),
                address: address.map(str::to_string),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabric_core::Coordinate;

    fn key(shard_id: u64) -> RoutingKey {
        RoutingKey::new(shard_id, Coordinate::new(0, 0)).unwrap()
    }

    fn keyspace() -> Keyspace {
        Keyspace::new()
            .with_rule(KeyspaceRule::new("Post", "Post:", key(1)).unwrap())
            .unwrap()
            .with_rule(KeyspaceRule::new("__session", "__session:", key(2)).unwrap())
            .unwrap()
    }

    #[test]
    fn a_kind_and_its_addresses_resolve_to_the_same_key() {
        let keyspace = keyspace();

        assert_eq!(keyspace.resolve(Some("Post"), None), Ok(key(1)));
        assert_eq!(keyspace.resolve(None, Some("Post:17")), Ok(key(1)));
        assert_eq!(keyspace.resolve(Some("Post"), Some("Post:17")), Ok(key(1)));
    }

    /// The invariant the two-halved rule exists to enforce: a write whose
    /// address contradicts its kind is refused, not filed where its kind's
    /// queries will never look.
    #[test]
    fn a_request_that_contradicts_itself_is_refused() {
        let miss = keyspace()
            .resolve(Some("Post"), Some("__session:abc"))
            .unwrap_err();

        assert!(matches!(miss, KeyspaceMiss::Inconsistent { .. }));
        assert!(miss.to_string().contains("will not look"));
    }

    #[test]
    fn an_unmapped_request_is_refused_rather_than_guessed() {
        let miss = keyspace().resolve(Some("Unknown"), None).unwrap_err();

        assert_eq!(
            miss,
            KeyspaceMiss::Unmapped {
                kind: Some("Unknown".to_string()),
                address: None
            }
        );
    }

    #[test]
    fn a_fallback_catches_what_no_rule_covers_and_shadows_nothing() {
        let keyspace = keyspace().with_fallback(key(9));

        assert_eq!(keyspace.resolve(Some("Unknown"), None), Ok(key(9)));
        assert_eq!(keyspace.resolve(None, Some("Other:1")), Ok(key(9)));
        // Declared rules still win.
        assert_eq!(keyspace.resolve(Some("Post"), None), Ok(key(1)));
    }

    #[test]
    fn the_longest_declared_prefix_wins() {
        let keyspace = Keyspace::new()
            .with_rule(KeyspaceRule::new("A", "__", key(1)).unwrap())
            .unwrap()
            .with_rule(KeyspaceRule::new("B", "__fabric_placement:", key(2)).unwrap())
            .unwrap();

        assert_eq!(
            keyspace.resolve(None, Some("__fabric_placement:1:0:0")),
            Ok(key(2))
        );
        assert_eq!(keyspace.resolve(None, Some("__session:x")), Ok(key(1)));
    }

    #[test]
    fn an_ambiguous_keyspace_is_refused_at_construction() {
        let error = keyspace()
            .with_rule(KeyspaceRule::new("Post", "Other:", key(3)).unwrap())
            .unwrap_err();
        assert_eq!(
            error,
            KeyspaceError::DuplicateKind {
                kind: "Post".to_string()
            }
        );

        let error = keyspace()
            .with_rule(KeyspaceRule::new("Other", "Post:", key(3)).unwrap())
            .unwrap_err();
        assert_eq!(
            error,
            KeyspaceError::DuplicateAddressPrefix {
                address_prefix: "Post:".to_string()
            }
        );
    }

    #[test]
    fn a_namespace_wide_question_has_an_answer_only_when_everything_is_one_place() {
        // Split: no single truthful answer.
        assert_eq!(keyspace().with_fallback(key(9)).spanning_key(), None);
        // Undeclared fallback: an address matching no rule has no home.
        assert_eq!(keyspace().spanning_key(), None);
        // One FacetQL behind the door: fully transparent.
        assert_eq!(Keyspace::single(key(1)).spanning_key(), Some(key(1)));
        // Rules that all point at the same place are still one place.
        assert_eq!(
            Keyspace::new()
                .with_rule(KeyspaceRule::new("Post", "Post:", key(1)).unwrap())
                .unwrap()
                .with_fallback(key(1))
                .spanning_key(),
            Some(key(1))
        );
    }

    #[test]
    fn a_half_declared_rule_is_refused() {
        assert_eq!(
            KeyspaceRule::new("", "Post:", key(1)).unwrap_err(),
            KeyspaceError::EmptyKind
        );
        assert_eq!(
            KeyspaceRule::new("Post", "", key(1)).unwrap_err(),
            KeyspaceError::EmptyAddressPrefix {
                kind: "Post".to_string()
            }
        );
    }
}
