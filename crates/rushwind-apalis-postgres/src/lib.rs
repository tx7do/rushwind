//! Postgres storage backend for the [apalis](https://docs.rs/apalis-core/0.7)
//! task queue — a hand-rolled `SKIP LOCKED` transport for RushWind.
//!
//! apalis backends are pluggable: anything implementing the `Storage` +
//! `Backend` traits from `apalis-core` slots into `WorkerBuilder::backend`.
//! This crate is such a backend, backed by a single Postgres table and
//! built around one idea — **reliability comes from the storage semantics,
//! not from the queue library**: transactional claims via `FOR UPDATE
//! SKIP LOCKED`, a visibility timeout on every claim, orphan recovery by
//! lock expiry, exponential-backoff retries and a `killed` dead-letter
//! state that keeps the corpse around for inspection.
//!
//! # Schema
//!
//! [`PostgresStorage::setup`] is idempotent and creates one table:
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS rushwind_apalis_jobs (
//!     task_id      TEXT PRIMARY KEY,
//!     queue        TEXT NOT NULL,
//!     payload      TEXT NOT NULL,          -- JsonCodec<String> encoding of the job
//!     status       TEXT NOT NULL DEFAULT 'pending',  -- pending|running|done|killed
//!     attempts     INTEGER NOT NULL DEFAULT 0,       -- incremented on every claim
//!     max_attempts INTEGER NOT NULL DEFAULT 10,
//!     run_at       TIMESTAMPTZ NOT NULL DEFAULT now(), -- due time (schedule + retry backoff)
//!     lock_at      TIMESTAMPTZ,            -- visibility deadline while running
//!     lock_by      TEXT,                   -- claiming worker id
//!     done_at      TIMESTAMPTZ,            -- terminal transition time (drives vacuum)
//!     last_error   TEXT,
//!     created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! ```
//!
//! One storage instance serves exactly one queue ([`PostgresStorage::queue`]);
//! multiple queues are multiple storages over the same pool and table.
//!
//! # Delivery semantics
//!
//! - **Claiming** is a single `UPDATE ... WHERE task_id IN (SELECT ...
//!   FOR UPDATE SKIP LOCKED) RETURNING` statement: under concurrent
//!   workers no row is handed out twice, and the claim stamps `lock_by`
//!   plus `lock_at = now() + lock timeout` (five keep-alive intervals).
//! - **Acknowledgements** flow through apalis's `AckLayer`: on every
//!   task outcome the storage receives the `Response` and hands it to
//!   the heartbeat loop, which batch-writes it. Success → `done`;
//!   failure with attempts left → `pending` again with
//!   `run_at = now() + retry_backoff(attempt)` (1s, 2s, 4s … capped at
//!   1h); failure with attempts exhausted, or an `Error::Abort` →
//!   `killed`, the dead-letter state. The batch update only touches
//!   rows still in `running` **under the acking worker's claim**
//!   (`lock_by` match), so a late ack after orphan recovery — or after
//!   another worker re-claimed the task — is dropped instead of
//!   corrupting the requeued row.
//! - **Crash recovery**: the heartbeat refreshes `lock_at` for this
//!   worker's running tasks each keep-alive, and re-queues any task
//!   whose `lock_at` expired — a dead worker's in-flight work returns
//!   to the pool automatically. Claim time both counts the attempt and
//!   enforces the budget: a job whose `attempts` have reached its
//!   `max_attempts` is dead-lettered at the next claim instead of
//!   cycling through crash → reclaim → crash forever, and never
//!   re-claimed past the budget. Delivery is therefore
//!   at-least-once; handlers should be idempotent.
//! - **Scheduled jobs** are plain rows with a future `run_at`; the
//!   claim query simply will not see them before then.
//! - [`Storage::vacuum`] deletes terminal rows (`done`/`killed`) older
//!   than the configured retention.
//!
//! # Divergences from `apalis-sql`
//!
//! The upstream `apalis-sql::postgres` storage is the reference this
//! design consciously departs from:
//!
//! - **No `NOTIFY`/`LISTEN`** — polling only (250ms default). One less
//!   dedicated connection and one less failure mode; latency trades
//!   for simplicity.
//! - **No `apalis.workers` registry, no plpgsql functions** — orphan
//!   recovery keys off the row's own `lock_at` expiry instead of a
//!   workers table's `last_seen`, and all SQL lives inline here.
//! - **`attempts` increments at claim time**, not at ack time, so a
//!   claimed-but-crashed run still counts towards `max_attempts`.
//! - **Retry backoff** — failures wait `retry_backoff` before becoming
//!   visible again; upstream re-offers failed rows immediately.
//! - **`JsonCodec<String>` only, one queue per storage** — no codec
//!   generic, no priority column, no per-queue stats surface. The
//!   payload column is TEXT, not JSONB, so the codec stays swappable
//!   in principle.
//! - **Duplicate push is a no-op** (`ON CONFLICT DO NOTHING`), not an
//!   error.
//!
//! # Testing
//!
//! Live conformance tests run against a real Postgres via the `live`
//! feature (`APALIS_PG_DATABASE_URL`); there is no embedded Postgres
//! for CI's unit lanes. Pure logic (backoff curve, settings defaults)
//! has inline unit tests that run everywhere.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{convert::TryFrom, fmt, marker::PhantomData, str::FromStr, time::Duration};

use apalis_core::{
    backend::Backend,
    codec::{json::JsonCodec, Codec},
    error::Error,
    interval::interval,
    layers::{Ack, AckLayer},
    notify::Notify,
    poller::{controller::Controller, stream::BackendStream, Poller},
    request::{Parts, Request, RequestStream, State},
    response::Response,
    storage::Storage,
    task::{attempt::Attempt, namespace::Namespace, task_id::TaskId},
    worker::{Context as WorkerContext, Event, Worker, WorkerId},
};
use chrono::{DateTime, Utc};
use futures::{select, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

/// How many claimed tasks may sit in flight between the poll loop and
/// the worker before the channel applies backpressure.
const BUFFER_SIZE: usize = 10;

/// The default cap on execution attempts for a task.
const DEFAULT_MAX_ATTEMPTS: u32 = 10;

/// One acknowledged outcome waiting for the heartbeat to persist:
/// `(task_id, next status, next run_at, last_error)`.
/// One acknowledged outcome: the claiming worker id, the task id, the
/// outcome state, the retry schedule (when the task goes back to
/// pending), and the recorded error. The worker id rides every row so
/// the batch write can refuse to touch a task another worker has since
/// re-claimed.
type AckRow = (String, String, State, Option<DateTime<Utc>>, Option<String>);

/// The error surface of the storage. Detail rides in the message, per
/// the workspace error taxonomy (`RegistryError`/`BrokerError` style).
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// The storage could not complete the operation.
    Failed(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "apalis postgres storage failed: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {}

/// The per-task context the storage hands to apalis — the job's
/// `max_attempts`, resolved from the row at claim time, plus the
/// claiming worker's id stamped at claim time (the ack path compares it
/// against the row's `lock_by` so a stale worker cannot overwrite
/// another worker's claim). A task pushed with the default context
/// takes the storage-wide setting; build a custom [`Request`] with a
/// specific [`PgContext`] to override per task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PgContext {
    max_attempts: i32,
    #[serde(default)]
    worker_id: String,
}

impl Default for PgContext {
    fn default() -> Self {
        Self {
            max_attempts: i32::try_from(DEFAULT_MAX_ATTEMPTS)
                .expect("DEFAULT_MAX_ATTEMPTS fits in i32"),
            worker_id: String::new(),
        }
    }
}

impl PgContext {
    /// Builds a context with an explicit attempt budget (at least one).
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1) as i32,
            worker_id: String::new(),
        }
    }

    /// The attempt budget for the task.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts.max(1) as u32
    }
}

/// The bootstrap factory's settings wire shape for
/// [`PostgresStorage::from_settings`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSettings {
    /// The Postgres connection URL, e.g.
    /// `postgres://rushwind:rushwind@127.0.0.1:5432/rushwind`.
    pub url: String,
    /// The queue this storage serves. Defaults to `"default"`.
    #[serde(default = "default_queue")]
    pub queue: String,
    /// How often a worker polls the queue for due tasks, in
    /// milliseconds. Defaults to 250.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// How often a worker refreshes the visibility deadline of its
    /// running tasks, in seconds. The visibility deadline itself is
    /// five times this — see the crate docs. Defaults to 30.
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
    /// The default attempt budget for pushed tasks that carry the
    /// default [`PgContext`] (per-task [`PgContext`]s override it).
    /// Defaults to 10.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// How long `done`/`killed` rows are kept before
    /// [`Storage::vacuum`] removes them, in seconds. Defaults to
    /// seven days.
    #[serde(default = "default_retention_secs")]
    pub retention_secs: u64,
}

fn default_queue() -> String {
    "default".to_string()
}

fn default_poll_interval_ms() -> u64 {
    250
}

fn default_keep_alive_secs() -> u64 {
    30
}

fn default_max_attempts() -> u32 {
    DEFAULT_MAX_ATTEMPTS
}

fn default_retention_secs() -> u64 {
    7 * 24 * 3600
}

#[derive(Debug, Clone)]
struct StorageConfig {
    poll_interval: Duration,
    keep_alive: Duration,
    max_attempts: i32,
    retention: Duration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(default_poll_interval_ms()),
            keep_alive: Duration::from_secs(default_keep_alive_secs()),
            max_attempts: PgContext::default().max_attempts,
            retention: Duration::from_secs(default_retention_secs()),
        }
    }
}

impl StorageConfig {
    /// The visibility deadline stamped on every claim: five keep-alive
    /// intervals, so a worker must miss five consecutive heartbeats
    /// before its task is considered orphaned.
    fn lock_timeout(&self) -> Duration {
        self.keep_alive * 5
    }
}

impl TryFrom<PostgresSettings> for StorageConfig {
    type Error = StorageError;

    fn try_from(settings: PostgresSettings) -> Result<Self, Self::Error> {
        if settings.queue.is_empty() {
            return Err(StorageError::Failed("queue must not be empty".into()));
        }
        if settings.poll_interval_ms == 0 {
            return Err(StorageError::Failed("poll_interval_ms must be > 0".into()));
        }
        if settings.keep_alive_secs == 0 {
            return Err(StorageError::Failed("keep_alive_secs must be > 0".into()));
        }
        if settings.max_attempts == 0 {
            return Err(StorageError::Failed("max_attempts must be >= 1".into()));
        }
        Ok(Self {
            poll_interval: Duration::from_millis(settings.poll_interval_ms),
            keep_alive: Duration::from_secs(settings.keep_alive_secs),
            max_attempts: i32::try_from(settings.max_attempts.max(1))
                .map_err(|e| StorageError::Failed(format!("max_attempts: {e}")))?,
            retention: Duration::from_secs(settings.retention_secs),
        })
    }
}

/// A Postgres-backed apalis storage for tasks of type `T`. Cheap to
/// clone; every clone shares one poll controller and ack channel, and
/// claims one queue.
pub struct PostgresStorage<T> {
    pool: PgPool,
    queue: String,
    config: StorageConfig,
    controller: Controller,
    ack_notify: Notify<AckRow>,
    _job: PhantomData<T>,
}

impl<T> Clone for PostgresStorage<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            queue: self.queue.clone(),
            config: self.config.clone(),
            controller: self.controller.clone(),
            ack_notify: self.ack_notify.clone(),
            _job: PhantomData,
        }
    }
}

impl<T> fmt::Debug for PostgresStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("pool", &self.pool)
            .field("queue", &self.queue)
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresStorage<()> {
    /// Creates the jobs table and indexes if they do not exist.
    /// Idempotent; call once per pool at startup (the live test suite
    /// and the demo example both do).
    pub async fn setup(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS rushwind_apalis_jobs (
                task_id      TEXT PRIMARY KEY,
                queue        TEXT NOT NULL,
                payload      TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                attempts     INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 10,
                run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                lock_at      TIMESTAMPTZ,
                lock_by      TEXT,
                done_at      TIMESTAMPTZ,
                last_error   TEXT,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS rushwind_apalis_jobs_claim_idx
                ON rushwind_apalis_jobs (queue, status, run_at);
            CREATE INDEX IF NOT EXISTS rushwind_apalis_jobs_done_idx
                ON rushwind_apalis_jobs (queue, status, done_at);",
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

impl<T> PostgresStorage<T> {
    /// Resolves the attempt budget for a pushed task: an explicit
    /// [`PgContext`] wins; the default context inherits the
    /// storage-wide setting.
    fn resolve_max_attempts(&self, ctx: &PgContext) -> i32 {
        if *ctx == PgContext::default() {
            self.config.max_attempts
        } else {
            ctx.max_attempts
        }
    }

    /// Connects to Postgres at `url` and serves `queue`. Performs no
    /// migrations — run [`PostgresStorage::setup`] first.
    pub async fn connect(url: &str, queue: &str) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .connect(url)
            .await
            .map_err(|e| StorageError::Failed(format!("postgres connect: {e}")))?;
        Ok(Self::new(pool, queue))
    }

    /// Wraps an existing pool. Performs no migrations — run
    /// [`PostgresStorage::setup`] first.
    pub fn new(pool: PgPool, queue: &str) -> Self {
        Self::with_config(pool, queue, StorageConfig::default())
    }

    /// Constructs from the bootstrap factory's settings wire shape.
    /// The pool is created lazily, so this never touches the network —
    /// the factory can build storages before Postgres is reachable.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, StorageError> {
        let settings: PostgresSettings = serde_json::from_value(settings)
            .map_err(|e| StorageError::Failed(format!("settings parse: {e}")))?;
        let config = StorageConfig::try_from(settings.clone())?;
        let pool = PgPoolOptions::new()
            .connect_lazy(&settings.url)
            .map_err(|e| StorageError::Failed(format!("postgres pool: {e}")))?;
        Ok(Self::with_config(pool, &settings.queue, config))
    }

    fn with_config(pool: PgPool, queue: &str, config: StorageConfig) -> Self {
        Self {
            pool,
            queue: queue.to_string(),
            config,
            controller: Controller::new(),
            ack_notify: Notify::new(),
            _job: PhantomData,
        }
    }

    /// The queue this storage claims from and pushes to.
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The underlying pool, for migrations and tests.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Atomically claims up to `BUFFER_SIZE` due tasks for `worker_id`:
    /// flips them to `running`, stamps the claim (`lock_by`, `lock_at`)
    /// and counts the attempt. The visibility deadline is five
    /// keep-alive intervals from now. The returned vec is ordered by
    /// `run_at`, then `task_id` — task ids are ULIDs, so ties on
    /// `run_at` resolve to creation order (FIFO).
    pub async fn fetch_next(
        &mut self,
        worker_id: &WorkerId,
    ) -> Result<Vec<Request<T, PgContext>>, StorageError>
    where
        T: DeserializeOwned,
    {
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.config.lock_timeout())
                .map_err(|e| StorageError::Failed(format!("lock timeout: {e}")))?;
        let rows = sqlx::query(
            "WITH dead AS (
                 UPDATE rushwind_apalis_jobs
                 SET status = 'killed', lock_by = NULL, lock_at = NULL,
                     done_at = now(), updated_at = now()
                 WHERE queue = $3 AND status = 'pending' AND attempts >= max_attempts
                 RETURNING task_id
             ),
             claimed AS (
                 UPDATE rushwind_apalis_jobs
                 SET status = 'running', lock_by = $1, lock_at = $2,
                     attempts = attempts + 1, updated_at = now()
                 WHERE task_id IN (
                     SELECT task_id FROM rushwind_apalis_jobs
                     WHERE queue = $3 AND status = 'pending' AND run_at <= now()
                           AND attempts < max_attempts
                     ORDER BY run_at ASC, task_id ASC
                     LIMIT $4
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING task_id, payload, attempts, max_attempts, run_at
             )
             SELECT task_id, payload, attempts, max_attempts FROM claimed
             ORDER BY run_at ASC, task_id ASC",
        )
        .bind(worker_id.to_string())
        .bind(deadline)
        .bind(&self.queue)
        .bind(i64::try_from(BUFFER_SIZE).expect("BUFFER_SIZE fits in i64"))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("claim: {e}")))?;
        rows.iter()
            .map(|row| self.request_from_row(row, &worker_id.to_string()))
            .collect()
    }

    fn request_from_row(
        &self,
        row: &sqlx::postgres::PgRow,
        worker_id: &str,
    ) -> Result<Request<T, PgContext>, StorageError>
    where
        T: DeserializeOwned,
    {
        let task_id: String = row
            .try_get("task_id")
            .map_err(|e| StorageError::Failed(format!("row task_id: {e}")))?;
        let payload: String = row
            .try_get("payload")
            .map_err(|e| StorageError::Failed(format!("row payload: {e}")))?;
        let attempts: i32 = row
            .try_get("attempts")
            .map_err(|e| StorageError::Failed(format!("row attempts: {e}")))?;
        let max_attempts: i32 = row
            .try_get("max_attempts")
            .map_err(|e| StorageError::Failed(format!("row max_attempts: {e}")))?;
        let args: T = JsonCodec::<String>::decode(payload)
            .map_err(|e| StorageError::Failed(format!("payload decode: {e}")))?;
        let mut parts = Parts::<PgContext>::default();
        parts.task_id = TaskId::from_str(&task_id)
            .map_err(|e| StorageError::Failed(format!("task_id parse: {e}")))?;
        parts.attempt = Attempt::new_with_value(
            usize::try_from(attempts.max(0))
                .map_err(|e| StorageError::Failed(format!("attempts: {e}")))?,
        );
        parts.context = PgContext {
            max_attempts,
            worker_id: worker_id.to_string(),
        };
        parts.namespace = Some(Namespace(self.queue.clone()));
        Ok(Request::new_with_parts(args, parts))
    }

    /// Re-arms the visibility deadline of every task this worker
    /// currently holds, so long-running handlers are not orphaned.
    async fn refresh_locks(&self, worker_id: &WorkerId) -> Result<(), StorageError> {
        let deadline = Utc::now()
            + chrono::Duration::from_std(self.config.lock_timeout())
                .map_err(|e| StorageError::Failed(format!("lock timeout: {e}")))?;
        sqlx::query(
            "UPDATE rushwind_apalis_jobs
             SET lock_at = $1, updated_at = now()
             WHERE queue = $2 AND status = 'running' AND lock_by = $3",
        )
        .bind(deadline)
        .bind(&self.queue)
        .bind(worker_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("keep-alive: {e}")))?;
        Ok(())
    }

    /// Returns tasks whose visibility deadline expired — their claiming
    /// worker died or stalled — to the pending pool. Safe to run from
    /// any worker: a live worker keeps its rows' deadlines in the
    /// future via [`Self::refresh_locks`].
    async fn sweep_orphans(&self) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE rushwind_apalis_jobs
             SET status = 'pending', lock_by = NULL, lock_at = NULL,
                 last_error = COALESCE(last_error, 'abandoned: lock expired'),
                 updated_at = now()
             WHERE queue = $1 AND status = 'running' AND lock_at < now()",
        )
        .bind(&self.queue)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("orphan sweep: {e}")))?;
        Ok(())
    }

    /// Batch-persists acknowledged outcomes. Only rows still in
    /// `running` **under this worker's claim** are touched: if orphan
    /// recovery already re-queued a task, or another worker re-claimed
    /// it while this worker sat stalled past the lock deadline, the
    /// stale ack is dropped (`lock_by` must match the acking worker).
    async fn write_acks(&self, acks: Vec<AckRow>) -> Result<(), StorageError> {
        if acks.is_empty() {
            return Ok(());
        }
        let rows: Vec<serde_json::Value> = acks
            .iter()
            .map(|(worker_id, task_id, status, run_at, last_error)| {
                serde_json::Value::Array(vec![
                    serde_json::Value::String(worker_id.clone()),
                    serde_json::Value::String(task_id.clone()),
                    serde_json::Value::String(status_to_sql(status).to_string()),
                    run_at
                        .map(|t| serde_json::Value::String(t.to_rfc3339()))
                        .unwrap_or(serde_json::Value::Null),
                    last_error
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                ])
            })
            .collect();
        let encoded = serde_json::to_string(&rows)
            .map_err(|e| StorageError::Failed(format!("ack encode: {e}")))?;
        sqlx::query(
            "UPDATE rushwind_apalis_jobs j
             SET status = q.status,
                 run_at = COALESCE(q.run_at, j.run_at),
                 last_error = q.last_error,
                 lock_by = NULL,
                 lock_at = NULL,
                 done_at = CASE WHEN q.status IN ('done', 'killed')
                                THEN now() ELSE NULL END,
                 updated_at = now()
             FROM (
                 SELECT (value->>0)::text AS worker_id,
                        (value->>1)::text AS task_id,
                        (value->>2)::text AS status,
                        (value->>3)::timestamptz AS run_at,
                        (value->>4)::text AS last_error
                 FROM json_array_elements($1::json)
             ) q
             WHERE j.task_id = q.task_id AND j.queue = $2
                   AND j.status = 'running' AND j.lock_by = q.worker_id",
        )
        .bind(encoded)
        .bind(&self.queue)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("ack write: {e}")))?;
        Ok(())
    }
}

/// Maps an apalis outcome state onto the SQL status vocabulary.
fn status_to_sql(status: &State) -> &'static str {
    match status {
        State::Pending => "pending",
        State::Done => "done",
        State::Killed => "killed",
        _ => "pending",
    }
}

/// The wait before a failed task becomes visible again: exponential,
/// `1s * 2^(attempt - 1)`, capped at one hour. `attempt` is the 1-based
/// number of the run that just failed.
pub fn retry_backoff(attempt: usize) -> chrono::Duration {
    let attempt = attempt.max(1);
    let shift = (attempt - 1).min(16) as u32;
    let secs = 1u64.checked_shl(shift).unwrap_or(3600).min(3600);
    chrono::Duration::seconds(secs as i64)
}

impl<T, Res> Ack<T, Res, JsonCodec<String>> for PostgresStorage<T>
where
    T: Send + Sync,
    Res: Sync,
{
    type Context = PgContext;
    type AckError = StorageError;

    async fn ack(&mut self, ctx: &PgContext, res: &Response<Res>) -> Result<(), StorageError> {
        let (status, run_at) = match &res.inner {
            Ok(_) => (State::Done, None),
            Err(Error::Abort(_)) => (State::Killed, None),
            Err(_) if res.attempt.current() >= ctx.max_attempts() as usize => (State::Killed, None),
            Err(_) => (
                State::Pending,
                Some(Utc::now() + retry_backoff(res.attempt.current())),
            ),
        };
        let last_error = res.inner.as_ref().err().map(|e| e.to_string());
        self.ack_notify
            .notify((
                ctx.worker_id.clone(),
                res.task_id.to_string(),
                status,
                run_at,
                last_error,
            ))
            .map_err(|e| StorageError::Failed(format!("ack channel: {e}")))?;
        Ok(())
    }
}

impl<T> Backend<Request<T, PgContext>> for PostgresStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    type Stream = BackendStream<RequestStream<Request<T, PgContext>>>;

    type Layer = AckLayer<PostgresStorage<T>, T, PgContext, JsonCodec<String>>;

    type Codec = JsonCodec<String>;

    fn poll(mut self, worker: &Worker<WorkerContext>) -> Poller<Self::Stream, Self::Layer> {
        let layer = AckLayer::new(self.clone());
        let config = self.config.clone();
        let controller = self.controller.clone();
        let ack_notify = self.ack_notify.clone();
        let (mut tx, rx) = futures::channel::mpsc::channel::<
            Result<Option<Request<T, PgContext>>, Error>,
        >(BUFFER_SIZE);
        let worker = worker.clone();

        let heartbeat = async move {
            // Start from a clean slate: anything a dead worker instance left
            // running goes back to the pool before we claim anything.
            if let Err(e) = self.sweep_orphans().await {
                worker.emit(Event::Error(Box::new(e)));
            }

            let mut keep_alive = interval(config.keep_alive).fuse();
            let mut poll_tick = interval(config.poll_interval).fuse();
            let mut ack_stream = ack_notify.ready_chunks(BUFFER_SIZE).fuse();

            loop {
                select! {
                    _ = keep_alive.next() => {
                        if let Err(e) = self.refresh_locks(worker.id()).await {
                            worker.emit(Event::Error(Box::new(e)));
                        }
                        if let Err(e) = self.sweep_orphans().await {
                            worker.emit(Event::Error(Box::new(e)));
                        }
                    }
                    acks = ack_stream.next() => {
                        if let Some(acks) = acks {
                            if let Err(e) = self.write_acks(acks).await {
                                worker.emit(Event::Error(Box::new(e)));
                            }
                        }
                    }
                    _ = poll_tick.next() => {
                        if worker.is_ready() {
                            match self.fetch_next(worker.id()).await {
                                Ok(jobs) => {
                                    for job in jobs {
                                        if tx.send(Ok(Some(job))).await.is_err() {
                                            // The worker end is gone; stop the loop.
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    worker.emit(Event::Error(Box::new(e)));
                                }
                            }
                        }
                    }
                }
            }
        };

        Poller::new_with_layer(BackendStream::new(rx.boxed(), controller), heartbeat, layer)
    }
}

impl<T> Storage for PostgresStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    type Job = T;

    type Error = StorageError;

    type Context = PgContext;

    type Compact = String;

    async fn push_request(
        &mut self,
        req: Request<Self::Job, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let payload = JsonCodec::<String>::encode(&req.args)
            .map_err(|e| StorageError::Failed(format!("payload encode: {e}")))?;
        sqlx::query(
            "INSERT INTO rushwind_apalis_jobs
                 (task_id, queue, payload, status, attempts, max_attempts, run_at)
             VALUES ($1, $2, $3, 'pending', 0, $4, now())
             ON CONFLICT (task_id) DO NOTHING",
        )
        .bind(req.parts.task_id.to_string())
        .bind(&self.queue)
        .bind(payload)
        .bind(self.resolve_max_attempts(&req.parts.context))
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("push: {e}")))?;
        Ok(req.parts)
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        sqlx::query(
            "INSERT INTO rushwind_apalis_jobs
                 (task_id, queue, payload, status, attempts, max_attempts, run_at)
             VALUES ($1, $2, $3, 'pending', 0, $4, now())
             ON CONFLICT (task_id) DO NOTHING",
        )
        .bind(req.parts.task_id.to_string())
        .bind(&self.queue)
        .bind(req.args)
        .bind(self.resolve_max_attempts(&req.parts.context))
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("push raw: {e}")))?;
        Ok(req.parts)
    }

    async fn schedule_request(
        &mut self,
        request: Request<Self::Job, Self::Context>,
        on: i64,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let run_at = DateTime::from_timestamp(on, 0)
            .ok_or_else(|| StorageError::Failed(format!("invalid schedule timestamp: {on}")))?;
        let payload = JsonCodec::<String>::encode(&request.args)
            .map_err(|e| StorageError::Failed(format!("payload encode: {e}")))?;
        sqlx::query(
            "INSERT INTO rushwind_apalis_jobs
                 (task_id, queue, payload, status, attempts, max_attempts, run_at)
             VALUES ($1, $2, $3, 'pending', 0, $4, $5)
             ON CONFLICT (task_id) DO NOTHING",
        )
        .bind(request.parts.task_id.to_string())
        .bind(&self.queue)
        .bind(payload)
        .bind(self.resolve_max_attempts(&request.parts.context))
        .bind(run_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("schedule: {e}")))?;
        Ok(request.parts)
    }

    async fn len(&mut self) -> Result<i64, Self::Error> {
        let row = sqlx::query(
            "SELECT count(*) AS n FROM rushwind_apalis_jobs
             WHERE queue = $1 AND status = 'pending'",
        )
        .bind(&self.queue)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("len: {e}")))?;
        row.try_get("n")
            .map_err(|e| StorageError::Failed(format!("len row: {e}")))
    }

    async fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job, Self::Context>>, Self::Error> {
        let row = sqlx::query(
            "SELECT task_id, payload, attempts, max_attempts FROM rushwind_apalis_jobs
             WHERE task_id = $1 AND queue = $2",
        )
        .bind(job_id.to_string())
        .bind(&self.queue)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("fetch_by_id: {e}")))?;
        match row {
            None => Ok(None),
            // The maintenance fetch carries no claim: an empty worker id
            // means the ack's lock_by guard would refuse to finalize it.
            Some(row) => self.request_from_row(&row, "").map(Some),
        }
    }

    async fn update(&mut self, job: Request<Self::Job, Self::Context>) -> Result<(), Self::Error> {
        // The worker's outcome path is the ack, not `update`; this is a
        // maintenance surface for attempt budgets and counters.
        let attempts: i32 = i32::try_from(job.parts.attempt.current())
            .map_err(|e| StorageError::Failed(format!("attempts: {e}")))?;
        sqlx::query(
            "UPDATE rushwind_apalis_jobs
             SET max_attempts = $1, attempts = $2, updated_at = now()
             WHERE task_id = $3 AND queue = $4",
        )
        .bind(job.parts.context.max_attempts)
        .bind(attempts)
        .bind(job.parts.task_id.to_string())
        .bind(&self.queue)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("update: {e}")))?;
        Ok(())
    }

    async fn reschedule(
        &mut self,
        job: Request<Self::Job, Self::Context>,
        wait: Duration,
    ) -> Result<(), Self::Error> {
        let run_at = Utc::now()
            + chrono::Duration::from_std(wait)
                .map_err(|e| StorageError::Failed(format!("reschedule wait: {e}")))?;
        sqlx::query(
            "UPDATE rushwind_apalis_jobs
             SET status = 'pending', run_at = $1, lock_by = NULL, lock_at = NULL,
                 updated_at = now()
             WHERE task_id = $2 AND queue = $3",
        )
        .bind(run_at)
        .bind(job.parts.task_id.to_string())
        .bind(&self.queue)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("reschedule: {e}")))?;
        Ok(())
    }

    async fn is_empty(&mut self) -> Result<bool, Self::Error> {
        Ok(self.len().await? == 0)
    }

    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(self.config.retention)
                .map_err(|e| StorageError::Failed(format!("retention: {e}")))?;
        let result = sqlx::query(
            "DELETE FROM rushwind_apalis_jobs
             WHERE queue = $1 AND status IN ('done', 'killed') AND done_at < $2",
        )
        .bind(&self.queue)
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Failed(format!("vacuum: {e}")))?;
        Ok(usize::try_from(result.rows_affected()).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps_at_an_hour() {
        assert_eq!(retry_backoff(1), chrono::Duration::seconds(1));
        assert_eq!(retry_backoff(2), chrono::Duration::seconds(2));
        assert_eq!(retry_backoff(3), chrono::Duration::seconds(4));
        assert_eq!(retry_backoff(13), chrono::Duration::seconds(3600));
        assert_eq!(retry_backoff(100), chrono::Duration::seconds(3600));
        // A zero attempt (a task claimed but never acked) backstops to
        // the first step rather than underflowing.
        assert_eq!(retry_backoff(0), chrono::Duration::seconds(1));
    }

    #[test]
    fn settings_default_everything_but_the_url() {
        let settings: PostgresSettings =
            serde_json::from_value(serde_json::json!({ "url": "postgres://x" })).unwrap();
        assert_eq!(settings.queue, "default");
        assert_eq!(settings.poll_interval_ms, 250);
        assert_eq!(settings.keep_alive_secs, 30);
        assert_eq!(settings.max_attempts, 10);
        assert_eq!(settings.retention_secs, 7 * 24 * 3600);
    }

    #[test]
    fn settings_reject_zero_attempt_budgets() {
        let settings: PostgresSettings = serde_json::from_value(serde_json::json!({
            "url": "postgres://x", "max_attempts": 0
        }))
        .unwrap();
        assert!(StorageConfig::try_from(settings).is_err());
    }

    #[test]
    fn context_enforces_a_one_attempt_floor() {
        assert_eq!(PgContext::new(0).max_attempts(), 1);
        assert_eq!(PgContext::new(7).max_attempts(), 7);
        assert_eq!(PgContext::default().max_attempts(), 10);
    }
}
