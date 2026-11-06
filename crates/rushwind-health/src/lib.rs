//! Health-check contract for RushWind: liveness and readiness
//! probes for Kubernetes, load balancers, and orchestration
//! platforms.
//!
//! # The model
//!
//! - [`Status`] — the health state of a component (up / down /
//!   unknown).
//! - [`Checker`] — a named health-check function returning a
//!   [`Result`].
//! - [`Result`] — the outcome of one check (status + message +
//!   details).
//! - [`Health`] — an aggregator that runs every registered checker
//!   concurrently under a per-check timeout and produces a combined
//!   [`Result`] with per-check breakdowns.
//!
//! The aggregation rules: any `down` checker
//! makes the aggregate `down`; otherwise any `unknown` makes it
//! `unknown`; otherwise the aggregate is `up`. Each checker runs
//! concurrently and is bounded by the configured timeout — a timed-out
//! checker reports `down` with a timeout message.
//!
//! # Built-in checkers
//!
//! - [`ping`] — adapt any fallible async closure.
//! - [`tcp`] — TCP port dial checks.
//! - [`http`] — HTTP GET availability checks (2xx–3xx = up).
//! - [`all`] — combinators that short-circuit on the first failure.
//! - [`any`] — combinators that succeed on the first pass.
//!
//! # The HTTP edge
//!
//! [`readiness_handler`] serves the aggregate as a JSON endpoint —
//! 200 when up or unknown, 503 when down. [`liveness_handler`]
//! answers a constant 200: if the process can serve HTTP, it is
//! alive.
//!
//! # Testing
//!
//! The engine is in-process; the conformance suite (aggregation,
//! timeouts, combinators, handlers) runs as ordinary unit tests.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use serde::Serialize;
use tokio::sync::RwLock;

/// The health state of a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    /// Not yet checked, or the outcome is inconclusive (the
    /// default).
    #[default]
    Unknown,
    /// The component runs.
    Up,
    /// The component is unavailable.
    Down,
}

impl Status {
    /// The readable string form.
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Up => "up",
            Status::Down => "down",
            Status::Unknown => "unknown",
        }
    }
}

/// One check's details, serialized into the aggregate — one
/// per-check breakdown entry.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CheckDetail {
    /// The readable status string.
    pub status: String,
    /// The optional human-readable message.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// The outcome of a single check. The aggregate
/// carries one [`CheckDetail`] per checker under
/// [`AggregateResult::checks`].
#[derive(Debug, Clone)]
pub struct Result {
    /// The check's status.
    pub status: Status,
    /// An optional human-readable message (e.g. the error
    /// description).
    pub message: String,
    /// Optional key-value details for extra check information.
    pub details: BTreeMap<String, String>,
}

impl Result {
    /// An `up` result with no message.
    pub fn up() -> Self {
        Self {
            status: Status::Up,
            message: String::new(),
            details: BTreeMap::new(),
        }
    }

    /// A `down` result carrying a message.
    pub fn down(message: impl Into<String>) -> Self {
        Self {
            status: Status::Down,
            message: message.into(),
            details: BTreeMap::new(),
        }
    }

    /// An `unknown` result carrying a message.
    pub fn unknown(message: impl Into<String>) -> Self {
        Self {
            status: Status::Unknown,
            message: message.into(),
            details: BTreeMap::new(),
        }
    }
}

/// The checker — an async health check returning a [`Result`].
/// Cancellation rides the future.
pub type Checker = Arc<dyn Fn() -> BoxFuture<'static, Result> + Send + Sync>;

/// Future type used by checkers.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The std result type, disambiguated from the contract's
/// [`Result`] check outcome.
pub type StdResult<T, E> = std::result::Result<T, E>;

use std::pin::Pin;

/// The aggregator: named checkers registered at
/// startup, every check run concurrently under one timeout.
pub struct Health {
    checkers: RwLock<Vec<(String, Checker)>>,
    timeout: Duration,
}

impl Default for Health {
    fn default() -> Self {
        Self::new(HealthOptions::default())
    }
}

/// The aggregator's settings.
#[derive(Debug, Clone)]
pub struct HealthOptions {
    /// The per-check timeout; a checker exceeding it reports `down`.
    /// Default: 5 s.
    pub timeout: Duration,
}

impl Default for HealthOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
        }
    }
}

impl Health {
    /// Builds an aggregator with the default options.
    pub fn new(options: HealthOptions) -> Self {
        Self {
            checkers: RwLock::new(Vec::new()),
            timeout: options.timeout,
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `timeout_ms` is optional.
    pub fn from_settings(settings: serde_json::Value) -> StdResult<Self, serde_json::Error> {
        #[derive(serde::Deserialize)]
        struct HealthSettings {
            timeout_ms: Option<u64>,
        }
        let settings: HealthSettings = serde_json::from_value(settings)?;
        Ok(Self::new(HealthOptions {
            timeout: Duration::from_millis(settings.timeout_ms.unwrap_or(5_000)),
        }))
    }

    /// Registers or replaces a named checker.
    pub async fn register(&self, name: impl Into<String>, checker: Checker) {
        let name = name.into();
        let mut checkers = self.checkers.write().await;
        if let Some(slot) = checkers.iter_mut().find(|(existing, _)| *existing == name) {
            slot.1 = checker;
        } else {
            checkers.push((name, checker));
        }
    }

    /// Registers a closure-shaped checker: `Ok(())` is up,
    /// `Err(message)` is down.
    pub async fn register_ping<F, Fut>(&self, name: impl Into<String>, checker: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = StdResult<(), String>> + Send + 'static,
    {
        let checker = Arc::new(checker);
        self.register(
            name,
            Arc::new(move || {
                let checker = Arc::clone(&checker);
                Box::pin(async move {
                    match checker().await {
                        Ok(()) => Result::up(),
                        Err(message) => Result::down(message),
                    }
                }) as BoxFuture<'static, Result>
            }),
        )
        .await;
    }

    /// Removes a named checker; a no-op when absent.
    pub async fn deregister(&self, name: &str) {
        self.checkers
            .write()
            .await
            .retain(|(existing, _)| existing != name);
    }

    /// The registered checker names.
    pub async fn names(&self) -> Vec<String> {
        self.checkers
            .read()
            .await
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Runs every registered checker concurrently and aggregates the
    /// outcomes. With no checkers registered the aggregate is
    /// `up` with a "no checkers registered" message.
    pub async fn check(&self) -> AggregateResult {
        let snapshot = self.checkers.read().await.clone();
        if snapshot.is_empty() {
            return AggregateResult {
                status: Status::Up.as_str().to_string(),
                message: "no checkers registered".to_string(),
                checks: BTreeMap::new(),
            };
        }

        let mut joins = Vec::with_capacity(snapshot.len());
        for (name, checker) in snapshot {
            let timeout = self.timeout;
            let task_name = name.clone();
            joins.push((
                task_name,
                tokio::spawn(async move {
                    let result = tokio::time::timeout(timeout, checker())
                        .await
                        .unwrap_or_else(|_| Result::down(format!("checker {name:?} timed out")));
                    (name, result)
                }),
            ));
        }

        let mut overall = Status::Up;
        let mut checks = BTreeMap::new();
        for (name, join) in joins {
            // A panicked checker is a failed component, not a silent
            // absence: it reports down, or the aggregate would keep
            // claiming readiness while the component is gone.
            let (_, result) = match join.await {
                Ok(pair) => pair,
                Err(_) => (
                    name.clone(),
                    Result::down(format!("checker {name:?} panicked")),
                ),
            };
            checks.insert(
                name.clone(),
                CheckDetail {
                    status: result.status.as_str().to_string(),
                    message: result.message.clone(),
                },
            );
            if result.status == Status::Down {
                overall = Status::Down;
            } else if result.status == Status::Unknown && overall != Status::Down {
                overall = Status::Unknown;
            }
        }

        AggregateResult {
            status: overall.as_str().to_string(),
            message: String::new(),
            checks,
        }
    }
}

/// The aggregated outcome — one
/// [`CheckDetail`] per checker.
#[derive(Debug, Clone, Serialize)]
pub struct AggregateResult {
    /// The readable aggregate status.
    pub status: String,
    /// An optional aggregate message (e.g. "no checkers registered").
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// One detail entry per checker.
    pub checks: BTreeMap<String, CheckDetail>,
}

/// Adapts any fallible async closure into a [`Checker`].
pub fn ping<F, Fut>(checker: F) -> Checker
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = StdResult<(), String>> + Send + 'static,
{
    let checker = Arc::new(checker);
    Arc::new(move || {
        let checker = Arc::clone(&checker);
        Box::pin(async move {
            match checker().await {
                Ok(()) => Result::up(),
                Err(message) => Result::down(message),
            }
        }) as BoxFuture<'static, Result>
    })
}

/// A TCP dial checker: the target is up when the
/// connection establishes. `timeout` defaults to 3 s when `None`.
pub fn tcp(addr: impl Into<String>, timeout: Option<Duration>) -> Checker {
    let addr = addr.into();
    let timeout = timeout.unwrap_or(Duration::from_secs(3));
    Arc::new(move || {
        let addr = addr.clone();
        Box::pin(async move {
            match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
                Err(_) => Result::down(format!("tcp dial {addr}: timed out")),
                Ok(Err(message)) => Result::down(format!("tcp dial {addr}: {message}")),
                Ok(Ok(_)) => Result::up(),
            }
        }) as BoxFuture<'static, Result>
    })
}

/// An HTTP GET availability checker: 2xx–3xx is up,
/// anything else (or any error) is down. `timeout` defaults to 3 s
/// when `None`.
pub fn http(url: impl Into<String>, timeout: Option<Duration>) -> Checker {
    let url = url.into();
    let timeout = timeout.unwrap_or(Duration::from_secs(3));
    Arc::new(move || {
        let url = url.clone();
        let timeout = timeout;
        Box::pin(async move {
            let client = reqwest::Client::new();
            let response = tokio::time::timeout(timeout, client.get(&url).send()).await;
            match response {
                Err(_) => Result::down(format!("http {url}: timed out")),
                Ok(Err(e)) => Result::down(format!("http request {url}: {e}")),
                Ok(Ok(response)) => {
                    let status = response.status().as_u16();
                    if (200..400).contains(&status) {
                        Result::up()
                    } else {
                        Result::down(format!("http {url} returned status {status}"))
                    }
                }
            }
        }) as BoxFuture<'static, Result>
    })
}

/// Composes checkers requiring every child to pass, short-circuiting
/// on the first failure.
pub fn all(checkers: Vec<Checker>) -> Checker {
    Arc::new(move || {
        let checkers = checkers.clone();
        Box::pin(async move {
            for (index, checker) in checkers.iter().enumerate() {
                let result = checker().await;
                if result.status == Status::Down {
                    return Result::down(format!("checker[{index}] failed: {}", result.message));
                }
            }
            Result::up()
        }) as BoxFuture<'static, Result>
    })
}

/// Composes checkers passing when any child passes. Reports the last
/// failure's message when all fail.
pub fn any(checkers: Vec<Checker>) -> Checker {
    Arc::new(move || {
        let checkers = checkers.clone();
        Box::pin(async move {
            let mut last_error = String::new();
            for checker in &checkers {
                let result = checker().await;
                if result.status == Status::Up {
                    return Result::up();
                }
                last_error = result.message;
            }
            Result::down(format!("all checkers failed, last error: {last_error}"))
        }) as BoxFuture<'static, Result>
    })
}

/// The readiness HTTP handler. Runs the
/// aggregate; 200 when up or unknown, 503 when down. The JSON body
/// carries the per-check breakdown.
pub async fn readiness_handler(
    health: Arc<Health>,
) -> (axum::http::StatusCode, Json<HandlerResponse>) {
    let result = health.check().await;
    let status_code = if result.status == Status::Down.as_str() {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    } else {
        axum::http::StatusCode::OK
    };
    (
        status_code,
        Json(HandlerResponse {
            status: result.status.as_str().to_string(),
            message: if result.message.is_empty() {
                None
            } else {
                Some(result.message)
            },
            checks: if result.checks.is_empty() {
                None
            } else {
                Some(result.checks)
            },
        }),
    )
}

/// The liveness HTTP handler: a
/// constant 200 with `{"status":"up"}`; if the process serves HTTP,
/// it is alive.
pub async fn liveness_handler() -> (axum::http::StatusCode, Json<HandlerResponse>) {
    (
        axum::http::StatusCode::OK,
        Json(HandlerResponse {
            status: Status::Up.as_str().to_string(),
            message: None,
            checks: None,
        }),
    )
}

/// The HTTP endpoint's JSON response.
#[derive(Debug, Serialize)]
pub struct HandlerResponse {
    /// The readable aggregate status.
    pub status: String,
    /// The optional aggregate message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The optional per-check breakdown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks: Option<BTreeMap<String, CheckDetail>>,
}
