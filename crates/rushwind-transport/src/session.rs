//! Session-handshake middleware contracts.
//!
//! Session-family transports (websocket now; QUIC/http3/webtransport and
//! the MQTT consumer bridge in P2) accept long-lived connections whose
//! handshake happens **before** any authentication context exists. The
//! contracts here give every such transport a uniform, ordered gate chain
//! plus a listener-scoped admission policy — closing the gap documented in
//! the threat model: HTTP-family middleware (tower) never sees a session
//! handshake, so the gate chain is the only authn/authz point for these
//! transports.
//!
//! Design rationale, the per-transport enforcement matrix, and the
//! session-shutdown bus live in `docs/session-middleware.md` in the
//! repository root.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A transport-neutral snapshot of one session handshake, as visible to
/// gate middleware **before** any session state is allocated.
///
/// Fields carry only what the concrete transport can reveal:
/// HTTP-family transports fill `headers` and no `remote`; raw-socket
/// transports (P2) will fill `remote` and no `headers`. Gates must treat
/// absent evidence as absence — never guess.
#[derive(Debug, Clone, Default)]
pub struct Handshake {
    /// Header pairs as presented by the client, when the transport is
    /// HTTP-family. Empty otherwise.
    pub headers: Vec<(String, String)>,
    /// The peer address, when the transport is a raw socket. `None`
    /// otherwise; HTTP-family listeners do not expose it until
    /// connect-info plumbing lands.
    pub remote: Option<SocketAddr>,
}

/// A rejection handed back by a gate: an HTTP-family transport maps
/// `status` to an HTTP status code; raw transports map it to their
/// protocol's refusal.
#[derive(Debug, Clone)]
pub struct Rejection {
    /// Status code, interpreted per transport.
    pub status: u16,
    /// Human-readable reason, for diagnostics.
    pub reason: String,
}

/// The verdict of one gate on one handshake.
#[derive(Debug, Clone)]
pub enum GateVerdict {
    /// No objection; proceed to the next gate.
    Continue,
    /// Refuse the handshake. First rejection wins and the chain stops.
    Reject(Rejection),
}

/// One gate in a session-handshake chain.
///
/// Gates are **synchronous and CPU-bound by contract**: a gate verifies
/// local evidence (signatures, allowlists, header shape) and must never
/// perform IO. Verdicts that need remote evidence (token introspection,
/// revocation checks) belong behind an in-process cache the gate reads —
/// or in a future async gate tier.
pub trait HandshakeGate: Send + Sync {
    /// The gate's name, for diagnostics and rejection records.
    fn name(&self) -> String;
    /// Inspect one handshake snapshot and return a verdict.
    fn inspect(&self, handshake: &Handshake) -> GateVerdict;
}

/// An ordered chain of gates, evaluated in registration order.
///
/// Built via [`GateChain::gate`] on the route builder of the concrete
/// transport; evaluation semantics are transport-independent.
pub struct GateChain {
    gates: Vec<Arc<dyn HandshakeGate>>,
}

impl Default for GateChain {
    fn default() -> Self {
        Self::new()
    }
}

impl GateChain {
    /// Creates an empty chain. An empty chain accepts every handshake —
    /// mount gates explicitly on auth-sensitive routes.
    pub fn new() -> Self {
        Self { gates: Vec::new() }
    }

    /// Appends a gate. Registration order is evaluation order.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates.push(gate);
        self
    }

    /// Evaluates the chain in registration order. The first rejection
    /// wins and short-circuits; an empty chain accepts everything.
    pub fn evaluate(&self, handshake: &Handshake) -> Result<(), Rejection> {
        for gate in &self.gates {
            if let GateVerdict::Reject(rej) = gate.inspect(handshake) {
                return Err(rej);
            }
        }
        Ok(())
    }
}

/// Listener-scoped admission policy for a session route.
///
/// This is the pre-auth budget of the threat model: quotas that bound
/// resource consumption **before and during** the handshake, deliberately
/// separate from any post-auth application-level limiting. Fields exist
/// only for the controls a concrete transport can actually enforce; see
/// `docs/session-middleware.md` for the per-transport enforcement matrix.
#[derive(Default)]
pub struct SessionPolicy {
    max_concurrent_sessions: Option<usize>,
    max_sessions_per_remote: Option<usize>,
    handshake_timeout: Option<Duration>,
}

impl SessionPolicy {
    /// Creates a policy with no limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps the number of simultaneously live sessions on this route.
    /// Excess handshakes are refused at the pre-admission check and, for
    /// those that raced past it, closed immediately at session start.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.max_concurrent_sessions = Some(n);
        self
    }

    /// The configured simultaneous-session cap, if any.
    pub fn get_max_concurrent_sessions(&self) -> Option<usize> {
        self.max_concurrent_sessions
    }

    /// Caps the number of simultaneously live sessions **per peer IP**
    /// (the port varies per connection; the count is per address). One
    /// peer cannot crowd the route's whole budget. Enforceable only
    /// where the transport exposes the peer address — raw transports
    /// (QUIC today) enforce it, HTTP-family listeners cannot until
    /// connect-info plumbing lands, and a `None` remote makes the cap
    /// inert there.
    pub fn max_sessions_per_remote(mut self, n: usize) -> Self {
        self.max_sessions_per_remote = Some(n);
        self
    }

    /// The configured per-peer cap, if any.
    pub fn get_max_sessions_per_remote(&self) -> Option<usize> {
        self.max_sessions_per_remote
    }

    /// Bounds the wall-clock budget a single handshake may consume.
    /// Transports with an observable handshake phase enforce this by
    /// racing the handshake future against the deadline and dropping it
    /// on expiry.
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    /// The configured handshake deadline, if any.
    pub fn get_handshake_timeout(&self) -> Option<Duration> {
        self.handshake_timeout
    }
}

/// The shared counter behind every session cap, owned by one listener.
///
/// Admission is a single atomic step: the counter increments and the cap
/// is checked together, so racing handshakes cannot both claim the last
/// slot. Guards decrement on drop — including drop by task abort — so a
/// cap can never leak slots.
pub struct SessionCounter {
    live: Arc<AtomicUsize>,
    /// The per-peer live counts, keyed by IP (the port varies per
    /// connection). Shared with the emitted guards so their drop can
    /// remove idle entries; the lock guards only the count swap.
    per_remote: Arc<Mutex<BTreeMap<IpAddr, Arc<AtomicUsize>>>>,
}

impl Default for SessionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionCounter {
    /// Creates a counter at zero.
    pub fn new() -> Self {
        Self {
            live: Arc::new(AtomicUsize::new(0)),
            per_remote: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Advisory cap check without admission. Transport pre-checks use
    /// this where a cheap early refusal is possible; the authoritative
    /// check is [`SessionCounter::admit`].
    pub fn at_cap(&self, cap: Option<usize>) -> bool {
        cap.is_some_and(|cap| self.live.load(Ordering::Relaxed) >= cap)
    }

    /// Attempts admission under both caps at once: the route-wide
    /// `total_cap` and the per-peer-IP `per_remote_cap`. The remote's
    /// IP drives the per-peer count; `None` (HTTP-family transports
    /// before connect-info plumbing) makes that cap inert. Returns the
    /// total guard and, when the per-peer cap applies, its guard —
    /// both decrement on drop. `None` total means uncapped: the
    /// per-peer cap may still refuse a peer that crowded the route.
    pub fn admit_with_remote(
        &self,
        total_cap: Option<usize>,
        per_remote_cap: Option<usize>,
        remote: Option<SocketAddr>,
    ) -> Option<(SessionCounterGuard, Option<PerRemoteGuard>)> {
        let total = self.admit(total_cap)?;
        let per_remote = match (per_remote_cap, remote) {
            (Some(cap), Some(addr)) => {
                let ip = addr.ip();
                let counter = {
                    let mut peers = self.per_remote.lock().expect("per-remote map poisoned");
                    Arc::clone(
                        peers
                            .entry(ip)
                            .or_insert_with(|| Arc::new(AtomicUsize::new(0))),
                    )
                };
                let prev = counter.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    counter.fetch_sub(1, Ordering::Relaxed);
                    drop(total);
                    return None;
                }
                Some(PerRemoteGuard {
                    peers: Arc::clone(&self.per_remote),
                    ip,
                    live: Some(Arc::clone(&counter)),
                })
            }
            _ => None,
        };
        Some((total, per_remote))
    }

    /// Attempts admission under `cap`, returning a guard that decrements
    /// on drop, or `None` when the cap is reached. `None` for `cap` means
    /// uncapped: admission always succeeds and the guard is inert — an
    /// uncapped route never decrements a counter it never incremented.
    pub fn admit(&self, cap: Option<usize>) -> Option<SessionCounterGuard> {
        let live = match cap {
            None => None,
            Some(cap) => {
                let prev = self.live.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    self.live.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
                Some(Arc::clone(&self.live))
            }
        };
        Some(SessionCounterGuard { live })
    }
}

/// The admission guard returned by [`SessionCounter::admit`]. The counted
/// variant decrements the counter on drop; the uncapped variant is inert.
pub struct SessionCounterGuard {
    live: Option<Arc<AtomicUsize>>,
}

/// The per-peer-IP half of a per-remote admission: decrements the
/// peer's live count on drop and drops the map entry once a peer is
/// fully idle (the map cannot grow with the peer population).
pub struct PerRemoteGuard {
    peers: Arc<Mutex<BTreeMap<IpAddr, Arc<AtomicUsize>>>>,
    ip: IpAddr,
    live: Option<Arc<AtomicUsize>>,
}

impl Drop for PerRemoteGuard {
    fn drop(&mut self) {
        if let Some(live) = &self.live {
            let remaining = live.fetch_sub(1, Ordering::Relaxed);
            if remaining <= 1 {
                // Idle peer: drop the entry so the map stays bounded.
                self.peers
                    .lock()
                    .expect("per-remote map poisoned")
                    .remove(&self.ip);
            }
        }
    }
}

impl Drop for SessionCounterGuard {
    fn drop(&mut self) {
        if let Some(live) = &self.live {
            live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Recorder {
        label: &'static str,
        verdict: GateVerdict,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl HandshakeGate for Recorder {
        fn name(&self) -> String {
            self.label.to_string()
        }
        fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
            self.log
                .lock()
                .expect("log poisoned")
                .push(self.label.to_string());
            self.verdict.clone()
        }
    }

    fn recorder(
        label: &'static str,
        verdict: GateVerdict,
        log: &Arc<Mutex<Vec<String>>>,
    ) -> Arc<Recorder> {
        Arc::new(Recorder {
            label,
            verdict,
            log: Arc::clone(log),
        })
    }

    #[test]
    fn empty_chain_accepts_everything() {
        let chain = GateChain::new();
        assert!(chain.evaluate(&Handshake::default()).is_ok());
    }

    #[test]
    fn first_rejection_short_circuits_in_registration_order() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let chain = GateChain::new()
            .gate(recorder("a", GateVerdict::Continue, &log))
            .gate(recorder(
                "b",
                GateVerdict::Reject(Rejection {
                    status: 403,
                    reason: "denied".to_string(),
                }),
                &log,
            ))
            .gate(recorder("c", GateVerdict::Continue, &log));
        let verdict = chain.evaluate(&Handshake::default());
        assert!(verdict.is_err());
        assert_eq!(verdict.unwrap_err().status, 403);
        assert_eq!(*log.lock().expect("log poisoned"), vec!["a", "b"]);
    }

    #[test]
    fn all_continue_accepts_and_every_gate_runs() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let chain = GateChain::new()
            .gate(recorder("a", GateVerdict::Continue, &log))
            .gate(recorder("b", GateVerdict::Continue, &log))
            .gate(recorder("c", GateVerdict::Continue, &log));
        assert!(chain.evaluate(&Handshake::default()).is_ok());
        assert_eq!(*log.lock().expect("log poisoned"), vec!["a", "b", "c"]);
    }

    #[test]
    fn counter_admits_to_cap_then_refuses() {
        let counter = SessionCounter::new();
        // Held for the whole test: while the guard lives, the cap holds.
        let _held = counter
            .admit(Some(1))
            .expect("first admission must succeed");
        assert!(counter.admit(Some(1)).is_none());
        assert!(counter.at_cap(Some(1)));
    }

    #[test]
    fn counter_guard_drop_frees_slot() {
        let counter = SessionCounter::new();
        {
            let _guard = counter.admit(Some(1));
            assert!(_guard.is_some());
        }
        assert!(!counter.at_cap(Some(1)));
        assert!(counter.admit(Some(1)).is_some());
    }

    #[test]
    fn counter_uncapped_never_counts() {
        let counter = SessionCounter::new();
        for _ in 0..8 {
            assert!(counter.admit(None).is_some());
        }
        assert!(!counter.at_cap(None));
    }

    #[test]
    fn per_remote_cap_refuses_one_peer_but_not_another() {
        let counter = SessionCounter::new();
        let peer_a: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let peer_a2: std::net::SocketAddr = "10.0.0.1:5001".parse().unwrap();
        let peer_b: std::net::SocketAddr = "10.0.0.2:5000".parse().unwrap();

        // Peer A fills its per-IP budget (2).
        let guard_a1 = counter
            .admit_with_remote(None, Some(2), Some(peer_a))
            .expect("peer a slot 1");
        let _guard_a2 = counter
            .admit_with_remote(None, Some(2), Some(peer_a2))
            .expect("peer a slot 2 (same IP, other port)");
        assert!(
            counter
                .admit_with_remote(None, Some(2), Some(peer_a))
                .is_none(),
            "peer a is at its per-IP cap"
        );
        // Peer B is unaffected — the cap is per IP.
        assert!(
            counter
                .admit_with_remote(None, Some(2), Some(peer_b))
                .is_some(),
            "peer b admits freely"
        );
        drop(guard_a1);
        // Peer A's released slot admits again.
        assert!(counter
            .admit_with_remote(None, Some(2), Some(peer_a))
            .is_some());
    }

    #[test]
    fn per_remote_cap_composes_with_the_total_cap() {
        let counter = SessionCounter::new();
        let peer_a: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let peer_b: std::net::SocketAddr = "10.0.0.2:5000".parse().unwrap();

        let _a = counter
            .admit_with_remote(Some(2), Some(1), Some(peer_a))
            .expect("peer a");
        let _b = counter
            .admit_with_remote(Some(2), Some(1), Some(peer_b))
            .expect("peer b — the total cap has room");
        // The total cap is now full: a third peer refuses even though
        // its own per-IP budget is untouched.
        let peer_c: std::net::SocketAddr = "10.0.0.3:5000".parse().unwrap();
        assert!(counter
            .admit_with_remote(Some(2), Some(1), Some(peer_c))
            .is_none());
    }

    #[test]
    fn a_none_remote_makes_the_per_remote_cap_inert() {
        let counter = SessionCounter::new();
        // HTTP-family transports before connect-info plumbing: the cap
        // cannot apply, admission succeeds repeatedly.
        for _ in 0..5 {
            assert!(counter.admit_with_remote(None, Some(1), None).is_some());
        }
    }
}
