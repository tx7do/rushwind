//! DTM engine for the RushWind [`transaction`] contract — a
//! hand-rolled client for DTM's HTTP protocol over reqwest,
//! covering the initiator side of all four transaction patterns:
//! saga (forward + compensate), TCC (try-confirm-cancel), 2-phase
//! message, and XA.
//!
//! [`transaction`]: rushwind_transaction
//!
//! # The wire protocol
//!
//! The engine speaks DTM's wire directly, as the reference
//! `github.com/dtm-labs/client/dtmcli` client does — every operation is a
//! JSON POST to `{server}/{submit|prepare|abort|registerBranch}`
//! carrying the trans base (`gid`, `trans_type`, parallel `steps`
//! and `payloads`, `concurrent`, `protocol`, ...), with the
//! client's success rule: a non-success status or a body containing
//! `FAILURE` is an error. Branch invocations POST/GET the business
//! URL with `gid`/`branch_id`/`trans_type`/`op` query parameters
//! (plus `phase2_url` for XA), and map 425/ONGOING to
//! [`TransactionError::Ongoing`], 409/FAILURE to
//! [`TransactionError::Failure`], anything else non-200 to
//! [`TransactionError::Dtm`].
//!
//! Flow fidelity, per `dtmcli`: TCC/XA globals run prepare →
//! business → submit, aborting (TCC with a `rollback_reason`) when
//! the business closure errors; saga steps are parallel
//! `steps`/`payloads` arrays whose payloads are JSON-encoded
//! strings; msg `AddTopic` prefixes `topic://`; custom options
//! (`{"orders":...,"concurrent":true}` for a concurrent saga,
//! `{"delay":N}` for a delayed msg) ride `custom_data`; the
//! branch-id generator emits zero-padded two-digit suffixes ("01",
//! "02", ...) with overflow reported as typed errors. The
//! participant-side, database-coupled helpers of the reference
//! client (`BarrierFromQuery`, `XaLocalTransaction`, `DoAndSubmitDB` — all
//! take a `*sql.DB`) are not provided: the barrier machinery is the
//! caller's business, like the orchestration it serves.
//! `Msg::do_and_submit` keeps the pure-HTTP shape: prepare,
//! business, then query-prepared (GET, branch id "00", op "msg")
//! on a non-failure business error, then submit — or abort on
//! FAILURE.
//!
//! The client is stateless — one fresh HTTP exchange per call — so
//! [`TransactionClient::close`] is a no-op.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rushwind_transaction::{BoxFuture, TransactionClient, TransactionError};

/// The DTM server address when settings name none.
const DEFAULT_SERVER: &str = "http://localhost:36789/api/dtmsvr";

/// The per-request HTTP timeout for DTM calls. Without one, a stalled
/// DTM server hangs every transaction call chain forever.
const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The sub-branch count ceiling; registering beyond it is an error.
const BRANCH_LIMIT: u32 = 99;

/// The msg topic URL prefix.
const MSG_TOPIC_PREFIX: &str = "topic://";

struct Inner {
    http: reqwest::Client,
    /// The DTM server root, e.g. `http://localhost:36789/api/dtmsvr`.
    server: String,
}

/// One branch invocation, bundling the DTM `TransRequestBranch`
/// arguments.
struct BranchRequest<'a> {
    gid: &'a str,
    trans_type: &'a str,
    method: reqwest::Method,
    body: Option<serde_json::Value>,
    branch_id: &'a str,
    op: &'a str,
    url: &'a str,
    phase2: bool,
}

impl Inner {
    /// POSTs the trans base to `{server}/{operation}` — the DTM
    /// `TransCallDtm`: a non-success status or a body containing
    /// FAILURE is [`TransactionError::Dtm`].
    async fn call_dtm(
        &self,
        base: &TransBaseWire<'_>,
        operation: &str,
    ) -> Result<(), TransactionError> {
        let body = serde_json::to_vec(base)
            .map_err(|e| TransactionError::Encode(format!("trans base: {e}")))?;
        let response = self
            .http
            .post(format!("{}/{operation}", self.server))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| TransactionError::Request(e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<unreadable body: {e}>"));
        if !status.is_success() || text.contains("FAILURE") {
            return Err(TransactionError::Dtm(text));
        }
        Ok(())
    }

    /// Registers a branch — the DTM `TransRegisterBranch`: a flat
    /// string map POSTed to `{server}/registerBranch`.
    async fn register_branch(
        &self,
        gid: &str,
        trans_type: &str,
        added: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), TransactionError> {
        let mut body = serde_json::Map::new();
        body.insert(
            "gid".to_string(),
            serde_json::Value::String(gid.to_string()),
        );
        body.insert(
            "trans_type".to_string(),
            serde_json::Value::String(trans_type.to_string()),
        );
        body.extend(added);
        let body = serde_json::to_vec(&serde_json::Value::Object(body))
            .map_err(|e| TransactionError::Encode(format!("branch: {e}")))?;
        let response = self
            .http
            .post(format!("{}/registerBranch", self.server))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| TransactionError::Request(e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<unreadable body: {e}>"));
        if !status.is_success() || text.contains("FAILURE") {
            return Err(TransactionError::Dtm(text));
        }
        Ok(())
    }

    /// Invokes a business branch endpoint — the DTM
    /// `TransRequestBranch` + `HTTPResp2DtmError`.
    async fn request_branch(&self, branch: BranchRequest<'_>) -> Result<(), TransactionError> {
        let mut request = self.http.request(branch.method, branch.url).query(&[
            ("dtm", self.server.as_str()),
            ("gid", branch.gid),
            ("branch_id", branch.branch_id),
            ("trans_type", branch.trans_type),
            ("op", branch.op),
        ]);
        if branch.phase2 {
            request = request.query(&[("phase2_url", branch.url)]);
        }
        if let Some(body) = branch.body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(
                    serde_json::to_vec(&body)
                        .map_err(|e| TransactionError::Encode(format!("branch: {e}")))?,
                );
        }
        let response = request
            .send()
            .await
            .map_err(|e| TransactionError::Request(e.to_string()))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<unreadable body: {e}>"));
        resp_to_dtm_error(status, text)
    }
}

/// A DTM (Distributed Transaction Manager) client.
pub struct DtmClient {
    inner: Arc<Inner>,
}

/// The wire shape every DTM operation POSTs — the DTM `TransBase`.
/// `concurrent` and `protocol` serialize unconditionally, matching
/// the reference struct's tags (`concurrent` has no omitempty; `protocol`
/// is always the empty string for the plain HTTP protocol).
#[derive(serde::Serialize)]
struct TransBaseWire<'a> {
    gid: &'a str,
    trans_type: &'a str,
    #[serde(skip_serializing_if = "String::is_empty")]
    custom_data: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    steps: Vec<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    payloads: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    query_prepared: String,
    concurrent: bool,
    protocol: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    rollback_reason: String,
}

impl<'a> TransBaseWire<'a> {
    fn new(gid: &'a str, trans_type: &'a str) -> Self {
        Self {
            gid,
            trans_type,
            custom_data: String::new(),
            steps: Vec::new(),
            payloads: Vec::new(),
            query_prepared: String::new(),
            concurrent: false,
            protocol: String::new(),
            rollback_reason: String::new(),
        }
    }
}

/// Serializes a business payload to its wire value — the DTM
/// `MustMarshalString`, with the panic as
/// [`TransactionError::Encode`].
fn marshal(payload: &impl serde::Serialize) -> Result<serde_json::Value, TransactionError> {
    serde_json::to_value(payload).map_err(|e| TransactionError::Encode(format!("payload: {e}")))
}

/// Maps a branch/endpoint response — the DTM `HTTPResp2DtmError`.
fn resp_to_dtm_error(status: u16, body: String) -> Result<(), TransactionError> {
    if status == 425 || body.contains("ONGOING") {
        Err(TransactionError::Ongoing(body))
    } else if status == 409 || body.contains("FAILURE") {
        Err(TransactionError::Failure(body))
    } else if status != 200 {
        Err(TransactionError::Dtm(body))
    } else {
        Ok(())
    }
}

/// The zero-padded sub-branch id generator — the DTM `BranchIDGen`,
/// panics surfaced as typed errors. Ids are "01", "02", ... within a
/// global transaction.
struct BranchIdGen {
    next: u32,
}

impl BranchIdGen {
    fn new() -> Self {
        Self { next: 0 }
    }

    fn new_sub_branch_id(&mut self) -> Result<String, TransactionError> {
        if self.next >= BRANCH_LIMIT {
            return Err(TransactionError::TooManyBranches);
        }
        self.next += 1;
        Ok(format!("{:02}", self.next))
    }
}

impl DtmClient {
    /// Creates a client for the default DTM server.
    pub fn new() -> Self {
        Self::with_server(DEFAULT_SERVER)
    }

    /// Creates a client for an explicit DTM server.
    pub fn with_server(server: impl Into<String>) -> Self {
        Self::with_server_and_timeout(server, DEFAULT_HTTP_TIMEOUT)
    }

    /// Creates a client with an explicit per-request timeout. The
    /// default client carries a total-request timeout ([`DEFAULT_HTTP_TIMEOUT`]):
    /// without one, a DTM server that stalls hangs every saga/tcc/xa
    /// call chain forever.
    pub fn with_server_and_timeout(server: impl Into<String>, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
                server: server.into(),
            }),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape;
    /// an absent or empty `server` selects the default, an absent
    /// `timeout_ms` the default request timeout.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, TransactionError> {
        let server = settings
            .get("server")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_SERVER);
        let timeout = settings
            .get("timeout_ms")
            .and_then(|value| value.as_u64())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_HTTP_TIMEOUT);
        Ok(Self::with_server_and_timeout(server, timeout))
    }

    /// The DTM server address.
    pub fn server(&self) -> &str {
        &self.inner.server
    }

    /// Starts a saga transaction builder — the DTM `NewSaga`.
    pub fn saga(&self, gid: impl Into<String>) -> Saga {
        Saga {
            inner: self.inner.clone(),
            gid: gid.into(),
            steps: Vec::new(),
            payloads: Vec::new(),
            orders: BTreeMap::new(),
            concurrent: false,
        }
    }

    /// Starts a 2-phase message transaction builder — the DTM
    /// `NewMsg`.
    pub fn msg(&self, gid: impl Into<String>) -> Msg {
        Msg {
            inner: self.inner.clone(),
            gid: gid.into(),
            steps: Vec::new(),
            payloads: Vec::new(),
            delay: 0,
        }
    }

    /// Runs a TCC global transaction — the DTM
    /// `TccGlobalTransaction`. The closure receives the branch
    /// handle; an `Ok` outcome submits (confirm), an error aborts
    /// (cancel) with the error text as the rollback reason, with the
    /// abort's own error swallowed.
    pub async fn tcc_global_transaction<F, Fut>(
        &self,
        gid: impl Into<String>,
        tcc_func: F,
    ) -> Result<(), TransactionError>
    where
        F: FnOnce(Tcc) -> Fut,
        Fut: Future<Output = Result<(), TransactionError>>,
    {
        let gid = gid.into();
        let mut base = TransBaseWire::new(&gid, "tcc");
        let tcc = Tcc {
            inner: self.inner.clone(),
            gid: gid.clone(),
            branches: BranchIdGen::new(),
        };
        self.inner.call_dtm(&base, "prepare").await?;
        match tcc_func(tcc).await {
            Ok(()) => self.inner.call_dtm(&base, "submit").await,
            Err(error) => {
                base.rollback_reason = error.to_string();
                let _ = self.inner.call_dtm(&base, "abort").await;
                Err(error)
            }
        }
    }

    /// Runs an XA global transaction — the DTM `XaGlobalTransaction`.
    /// The closure registers branches through the [`Xa`] handle; an
    /// `Ok` outcome submits, an error aborts.
    pub async fn xa_global_transaction<F, Fut>(
        &self,
        gid: impl Into<String>,
        xa_func: F,
    ) -> Result<(), TransactionError>
    where
        F: FnOnce(Xa) -> Fut,
        Fut: Future<Output = Result<(), TransactionError>>,
    {
        let gid = gid.into();
        let base = TransBaseWire::new(&gid, "xa");
        let xa = Xa {
            inner: self.inner.clone(),
            gid: gid.clone(),
            branches: BranchIdGen::new(),
        };
        self.inner.call_dtm(&base, "prepare").await?;
        match xa_func(xa).await {
            Ok(()) => self.inner.call_dtm(&base, "submit").await,
            Err(error) => {
                let _ = self.inner.call_dtm(&base, "abort").await;
                Err(error)
            }
        }
    }
}

impl Default for DtmClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionClient for DtmClient {
    fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), TransactionError>> {
        Box::pin(async move { Ok(()) })
    }
}

/// A saga transaction builder — the DTM `Saga`. Steps run in order;
/// a failing step compensates all completed ones in reverse.
pub struct Saga {
    inner: Arc<Inner>,
    gid: String,
    steps: Vec<BTreeMap<String, String>>,
    payloads: Vec<String>,
    orders: BTreeMap<String, Vec<usize>>,
    concurrent: bool,
}

impl Saga {
    /// Adds a step: the forward `action` URL, the `compensate` URL,
    /// and the payload sent to both — the DTM `Saga.Add`.
    pub fn add(
        self,
        action: impl Into<String>,
        compensate: impl Into<String>,
        payload: &impl serde::Serialize,
    ) -> Result<Self, TransactionError> {
        let mut step = BTreeMap::new();
        step.insert("action".to_string(), action.into());
        step.insert("compensate".to_string(), compensate.into());
        let payload = marshal(payload)?;
        Ok(Self {
            steps: {
                let mut steps = self.steps;
                steps.push(step);
                steps
            },
            payloads: {
                let mut payloads = self.payloads;
                payloads.push(payload.to_string());
                payloads
            },
            ..self
        })
    }

    /// Orders `branch` to run only after `pre_branches` — the DTM
    /// `AddBranchOrder`. Indices are zero-based step numbers.
    pub fn add_branch_order(mut self, branch: usize, pre_branches: Vec<usize>) -> Self {
        self.orders.insert(branch.to_string(), pre_branches);
        self
    }

    /// Enables concurrent branch execution — the DTM `SetConcurrent`.
    pub fn set_concurrent(mut self) -> Self {
        self.concurrent = true;
        self
    }

    /// Submits the saga to the DTM server — the DTM `Saga.Submit`.
    pub async fn submit(self) -> Result<(), TransactionError> {
        let mut base = TransBaseWire::new(&self.gid, "saga");
        base.steps = self.steps;
        base.payloads = self.payloads;
        if self.concurrent {
            base.concurrent = true;
            base.custom_data = serde_json::to_string(&serde_json::json!({
                "orders": self.orders,
                "concurrent": self.concurrent,
            }))
            .map_err(|e| TransactionError::Encode(format!("custom data: {e}")))?;
        }
        self.inner.call_dtm(&base, "submit").await
    }
}

/// A 2-phase message transaction builder — the DTM `Msg`.
pub struct Msg {
    inner: Arc<Inner>,
    gid: String,
    steps: Vec<BTreeMap<String, String>>,
    payloads: Vec<String>,
    delay: u64,
}

impl Msg {
    /// Adds a step: the `action` URL invoked on commit, with its
    /// payload — the DTM `Msg.Add`.
    pub fn add(
        self,
        action: impl Into<String>,
        payload: &impl serde::Serialize,
    ) -> Result<Self, TransactionError> {
        let mut step = BTreeMap::new();
        step.insert("action".to_string(), action.into());
        let payload = marshal(payload)?;
        Ok(Self {
            steps: {
                let mut steps = self.steps;
                steps.push(step);
                steps
            },
            payloads: {
                let mut payloads = self.payloads;
                payloads.push(payload.to_string());
                payloads
            },
            ..self
        })
    }

    /// Adds a topic-based step for message-queue integration — the
    /// DTM `AddTopic` (`topic://` prefix).
    pub fn add_topic(
        self,
        topic: impl Into<String>,
        payload: &impl serde::Serialize,
    ) -> Result<Self, TransactionError> {
        self.add(format!("{MSG_TOPIC_PREFIX}{}", topic.into()), payload)
    }

    /// Delays the action invocation — the DTM `SetDelay`, seconds.
    pub fn set_delay(mut self, delay: u64) -> Self {
        self.delay = delay;
        self
    }

    /// Prepares the message; DTM later calls `query_prepared` to
    /// decide whether to proceed — the DTM `Msg.Prepare`.
    pub async fn prepare(&self, query_prepared: impl Into<String>) -> Result<(), TransactionError> {
        let mut base = TransBaseWire::new(&self.gid, "msg");
        base.steps = self.steps.clone();
        base.payloads = self.payloads.clone();
        base.query_prepared = query_prepared.into();
        self.inner.call_dtm(&base, "prepare").await
    }

    /// Submits the message — the DTM `Msg.Submit`.
    pub async fn submit(&self) -> Result<(), TransactionError> {
        let mut base = TransBaseWire::new(&self.gid, "msg");
        base.steps = self.steps.clone();
        base.payloads = self.payloads.clone();
        if self.delay > 0 {
            base.custom_data =
                serde_json::to_string(&serde_json::json!({ "delay": self.delay }))
                    .map_err(|e| TransactionError::Encode(format!("custom data: {e}")))?;
        }
        self.inner.call_dtm(&base, "submit").await
    }

    /// Runs prepare → business → submit in one call — the DTM
    /// `DoAndSubmit`. A business [`TransactionError::Failure`] aborts
    /// directly; any other business error queries `query_prepared`
    /// (GET, branch id "00", op "msg") to learn the outcome and then
    /// submits — or aborts when the outcome is FAILURE. The
    /// DB-coupled barrier handle of the reference client
    /// (`DoAndSubmitDB`)
    /// is not provided; the business error takes precedence over
    /// any flow error.
    pub async fn do_and_submit<F, Fut>(
        self,
        query_prepared: impl Into<String>,
        busi_call: F,
    ) -> Result<(), TransactionError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), TransactionError>>,
    {
        let query_prepared = query_prepared.into();
        self.prepare(query_prepared.clone()).await?;
        let busi_error: Option<TransactionError> = busi_call().await.err();
        let flow_error: Option<TransactionError> = match &busi_error {
            // Business succeeded: straight to submit.
            None => self.submit().await.err(),
            // Business reported FAILURE: abort, return the business
            // error — the ErrFailure path.
            Some(error @ TransactionError::Failure(_)) => {
                let _ = self.abort(&query_prepared).await;
                Some(error.clone())
            }
            // Any other business error: DTM learns the outcome from
            // the query-prepared endpoint, then submits — or aborts
            // when the outcome is FAILURE.
            Some(_) => match self.query_prepared_outcome(&query_prepared).await {
                Ok(()) => self.submit().await.err(),
                Err(failure @ TransactionError::Failure(_)) => {
                    let _ = self.abort(&query_prepared).await;
                    Some(failure)
                }
                Err(other) => Some(other),
            },
        };
        match (busi_error, flow_error) {
            (Some(error), _) | (None, Some(error)) => Err(error),
            (None, None) => Ok(()),
        }
    }

    /// Aborts the message — `TransCallDtm(base, "abort")`
    /// with the base as Prepare left it.
    async fn abort(&self, query_prepared: &str) -> Result<(), TransactionError> {
        let mut base = TransBaseWire::new(&self.gid, "msg");
        base.steps = self.steps.clone();
        base.payloads = self.payloads.clone();
        base.query_prepared = query_prepared.to_string();
        self.inner.call_dtm(&base, "abort").await
    }

    /// Asks the query-prepared endpoint for the business outcome —
    /// requestBranch(GET, branch "00", op "msg").
    async fn query_prepared_outcome(&self, query_prepared: &str) -> Result<(), TransactionError> {
        self.inner
            .request_branch(BranchRequest {
                gid: &self.gid,
                trans_type: "msg",
                method: reqwest::Method::GET,
                body: None,
                branch_id: "00",
                op: "msg",
                url: query_prepared,
                phase2: false,
            })
            .await
    }
}

/// A TCC branch handle handed to the
/// [`DtmClient::tcc_global_transaction`] closure — the DTM `Tcc`.
pub struct Tcc {
    inner: Arc<Inner>,
    gid: String,
    branches: BranchIdGen,
}

impl Tcc {
    /// Registers and invokes a TCC branch — the DTM `CallBranch`:
    /// register the confirm/cancel URLs with DTM, then POST the
    /// payload to `try_url` with the branch query parameters.
    pub async fn call_branch(
        &mut self,
        payload: &impl serde::Serialize,
        try_url: impl Into<String>,
        confirm_url: impl Into<String>,
        cancel_url: impl Into<String>,
    ) -> Result<(), TransactionError> {
        let branch_id = self.branches.new_sub_branch_id()?;
        let payload = marshal(payload)?;
        self.inner
            .register_branch(
                &self.gid,
                "tcc",
                serde_json::Map::from_iter([
                    (
                        "data".to_string(),
                        serde_json::Value::String(payload.to_string()),
                    ),
                    (
                        "branch_id".to_string(),
                        serde_json::Value::String(branch_id.clone()),
                    ),
                    (
                        "confirm".to_string(),
                        serde_json::Value::String(confirm_url.into()),
                    ),
                    (
                        "cancel".to_string(),
                        serde_json::Value::String(cancel_url.into()),
                    ),
                ]),
            )
            .await?;
        self.inner
            .request_branch(BranchRequest {
                gid: &self.gid,
                trans_type: "tcc",
                method: reqwest::Method::POST,
                body: Some(payload),
                branch_id: &branch_id,
                op: "try",
                url: &try_url.into(),
                phase2: false,
            })
            .await
    }
}

/// An XA branch handle handed to the
/// [`DtmClient::xa_global_transaction`] closure — the DTM `Xa`.
pub struct Xa {
    inner: Arc<Inner>,
    gid: String,
    branches: BranchIdGen,
}

impl Xa {
    /// Invokes an XA branch — the DTM `Xa.CallBranch`: POST the
    /// payload to `branch_url` with the branch query parameters and
    /// `phase2_url`.
    pub async fn call_branch(
        &mut self,
        payload: &impl serde::Serialize,
        branch_url: impl Into<String>,
    ) -> Result<(), TransactionError> {
        let branch_id = self.branches.new_sub_branch_id()?;
        let payload = marshal(payload)?;
        self.inner
            .request_branch(BranchRequest {
                gid: &self.gid,
                trans_type: "xa",
                method: reqwest::Method::POST,
                body: Some(payload),
                branch_id: &branch_id,
                op: "action",
                url: &branch_url.into(),
                phase2: true,
            })
            .await
    }
}

/// Unit pins for the protocol's pure pieces.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_ids_are_zero_padded_and_bounded() {
        let mut gen = BranchIdGen::new();
        assert_eq!(gen.new_sub_branch_id().unwrap(), "01");
        assert_eq!(gen.new_sub_branch_id().unwrap(), "02");
        for _ in 2..BRANCH_LIMIT {
            gen.new_sub_branch_id().unwrap();
        }
        assert!(matches!(
            gen.new_sub_branch_id(),
            Err(TransactionError::TooManyBranches)
        ));
    }

    #[test]
    fn branch_responses_map_like_go() {
        assert!(resp_to_dtm_error(200, r#"{"dtm_result":"SUCCESS"}"#.into()).is_ok());
        assert!(matches!(
            resp_to_dtm_error(409, "FAILURE".into()),
            Err(TransactionError::Failure(_))
        ));
        assert!(matches!(
            resp_to_dtm_error(200, r#"{"dtm_result":"FAILURE"}"#.into()),
            Err(TransactionError::Failure(_))
        ));
        assert!(matches!(
            resp_to_dtm_error(428, "ONGOING".into()),
            Err(TransactionError::Ongoing(_))
        ));
        assert!(matches!(
            resp_to_dtm_error(500, "boom".into()),
            Err(TransactionError::Dtm(_))
        ));
    }

    #[test]
    fn trans_base_serializes_the_go_tags() {
        let base = TransBaseWire::new("g1", "saga");
        let value = serde_json::to_value(&base).unwrap();
        assert_eq!(value["gid"], "g1");
        assert_eq!(value["trans_type"], "saga");
        assert_eq!(value["concurrent"], false);
        assert_eq!(value["protocol"], "");
        assert!(value.get("steps").is_none());
        assert!(value.get("custom_data").is_none());
        assert!(value.get("rollback_reason").is_none());
    }

    #[test]
    fn settings_select_server_with_the_go_default() {
        let client = DtmClient::from_settings(serde_json::json!({})).unwrap();
        assert_eq!(client.server(), DEFAULT_SERVER);
        let client = DtmClient::from_settings(serde_json::json!({
            "server": "http://10.0.0.9:36789/api/dtmsvr"
        }))
        .unwrap();
        assert_eq!(client.server(), "http://10.0.0.9:36789/api/dtmsvr");
    }
}
