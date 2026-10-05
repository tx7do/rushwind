//! Live conformance suite for the Postgres apalis storage.
//!
//! Runs against a real Postgres — there is no embedded Postgres for
//! CI's unit lanes. Gated behind the `live` feature:
//!
//! ```bash
//! cargo test -p rushwind-apalis-postgres --features live
//! ```
//!
//! The server address comes from `APALIS_PG_DATABASE_URL`
//! (default `postgres://rushwind:rushwind@127.0.0.1:5432/rushwind`).
//! The suite creates the schema itself and isolates every test on its
//! own queue name, so it can run against a shared database.
//!
//! The ack path needs the storage's heartbeat running (acks reach the
//! database through it), so tests that assert outcomes spawn a poller
//! via [`drive`] — exactly the wiring `Monitor` uses in production —
//! and route claimed tasks into a channel the test consumes.

#![cfg(feature = "live")]

use std::{sync::Arc, time::Duration};

use apalis_core::{
    backend::Backend,
    error::{BoxDynError, Error as ApalisError},
    layers::Ack,
    request::{Parts, Request, State},
    response::Response,
    storage::Storage,
    task::task_id::TaskId,
    worker::{Context as WorkerContext, Worker, WorkerId},
};
use futures::StreamExt;
use rushwind_apalis_postgres::{retry_backoff, PgContext, PostgresStorage};
use serde::{Deserialize, Serialize};
use sqlx::Row;

const URL_ENV: &str = "APALIS_PG_DATABASE_URL";
const DEFAULT_URL: &str = "postgres://rushwind:rushwind@127.0.0.1:5432/rushwind";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Email {
    subject: String,
    to: String,
}

fn example_email() -> Email {
    Email {
        subject: "Test Subject".to_string(),
        to: "example@rushwind".to_string(),
    }
}

fn database_url() -> String {
    std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// Every test gets its own queue, so the suite can share one database.
fn unique_queue() -> String {
    format!("rw-apalis-test-{}", TaskId::new())
}

async fn setup(queue: &str) -> PostgresStorage<Email> {
    let storage = PostgresStorage::<Email>::connect(&database_url(), queue)
        .await
        .expect("connect");
    PostgresStorage::<()>::setup(storage.pool())
        .await
        .expect("setup");
    purge(&storage).await;
    storage
}

async fn purge(storage: &PostgresStorage<Email>) {
    sqlx::query("DELETE FROM rushwind_apalis_jobs WHERE queue = $1")
        .bind(storage.queue())
        .execute(storage.pool())
        .await
        .expect("purge");
}

/// Runs the storage's poller like `Monitor` does: heartbeat spawned,
/// claimed tasks forwarded to the test. Returns the worker handle and
/// the receiving end.
fn drive(
    storage: &PostgresStorage<Email>,
    worker_name: &str,
) -> (
    Worker<WorkerContext>,
    tokio::sync::mpsc::Receiver<Request<Email, PgContext>>,
) {
    let worker = Worker::new(WorkerId::new(worker_name), WorkerContext::default());
    // In production the ReadinessLayer flips this as the service drains;
    // a bare worker starts not-ready, which would stall the heartbeat's
    // claim branch forever.
    worker.start();
    let poller = storage.clone().poll(&worker);
    tokio::spawn(poller.heartbeat);
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut stream = poller.stream;
        while let Some(item) = stream.next().await {
            match item {
                Ok(Some(req)) => {
                    if tx.send(req).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("poll stream error: {e}"),
            }
        }
    });
    (worker, rx)
}

fn success_response(req: &Request<Email, PgContext>) -> Response<()> {
    Response::success((), req.parts.task_id.clone(), req.parts.attempt.clone())
}

fn failure_response(req: &Request<Email, PgContext>) -> Response<()> {
    let error: BoxDynError = format!("boom from {}", req.parts.task_id).into();
    Response::failure(
        ApalisError::Failed(Arc::new(error)),
        req.parts.task_id.clone(),
        req.parts.attempt.clone(),
    )
}

fn abort_response(req: &Request<Email, PgContext>) -> Response<()> {
    let error: BoxDynError = "aborting".to_string().into();
    Response::failure(
        ApalisError::Abort(Arc::new(error)),
        req.parts.task_id.clone(),
        req.parts.attempt.clone(),
    )
}

/// `(status, attempts)` of a row, as raw as the suite's assertions get.
async fn status_of(storage: &mut PostgresStorage<Email>, task_id: &TaskId) -> (State, i32) {
    let row = sqlx::query(
        "SELECT status, attempts FROM rushwind_apalis_jobs WHERE task_id = $1 AND queue = $2",
    )
    .bind(task_id.to_string())
    .bind(storage.queue())
    .fetch_one(storage.pool())
    .await
    .expect("status row");
    let status: String = row.try_get("status").expect("status");
    let attempts: i32 = row.try_get("attempts").expect("attempts");
    let state = match status.as_str() {
        "pending" => State::Pending,
        "running" => State::Running,
        "done" => State::Done,
        "killed" => State::Killed,
        other => panic!("unknown status {other}"),
    };
    (state, attempts)
}

// sqlx 0.9 gatekeeps dynamic SQL behind `SqlSafeStr`; the suite only ever
// reads these two columns, so each gets its own static statement instead of
// a formatted one.
async fn text_column(
    storage: &PostgresStorage<Email>,
    task_id: &TaskId,
    column: &str,
) -> Option<String> {
    let sql = match column {
        "last_error" => {
            "SELECT last_error FROM rushwind_apalis_jobs WHERE task_id = $1 AND queue = $2"
        }
        "lock_by" => "SELECT lock_by FROM rushwind_apalis_jobs WHERE task_id = $1 AND queue = $2",
        other => panic!("unsupported column {other}"),
    };
    let row = sqlx::query(sql)
        .bind(task_id.to_string())
        .bind(storage.queue())
        .fetch_one(storage.pool())
        .await
        .expect("row");
    row.try_get::<Option<String>, _>(column).expect("column")
}

/// Push → claim → ack success lands the row in `done`, with the
/// payload intact through the codec round-trip and attempts counting
/// claims.
#[tokio::test]
async fn push_claim_ack_success_roundtrip() {
    let mut storage = setup(&unique_queue()).await;
    let pushed = storage.push(example_email()).await.expect("push");

    let (_worker, mut rx) = drive(&storage, "rw-roundtrip");
    let claimed = rx.recv().await.expect("claimed task");
    assert_eq!(claimed.parts.task_id, pushed.task_id);
    assert_eq!(claimed.args, example_email());
    assert_eq!(claimed.parts.attempt.current(), 1, "attempts count claims");

    storage
        .ack(&claimed.parts.context, &success_response(&claimed))
        .await
        .expect("ack");

    // The heartbeat batch-writes acks; give it a beat.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (state, attempts) = status_of(&mut storage, &pushed.task_id).await;
    assert_eq!(state, State::Done);
    assert_eq!(attempts, 1);
}

/// Failures come back with exponential backoff and die into `killed`
/// once the attempt budget runs out.
#[tokio::test]
async fn failure_retries_then_kills() {
    let mut storage = setup(&unique_queue()).await;

    // A task with a two-run budget: first failure retries, second kills.
    let mut parts = Parts::<PgContext>::default();
    parts.context = PgContext::new(2);
    let pushed = storage
        .push_request(Request::new_with_parts(example_email(), parts))
        .await
        .expect("push");

    let (_worker, mut rx) = drive(&storage, "rw-retry");
    let first = rx.recv().await.expect("first claim");
    assert_eq!(first.parts.task_id, pushed.task_id);
    assert_eq!(first.parts.attempt.current(), 1);

    storage
        .ack(&first.parts.context, &failure_response(&first))
        .await
        .expect("ack failure");
    tokio::time::sleep(Duration::from_millis(700)).await;

    let (state, attempts) = status_of(&mut storage, &pushed.task_id).await;
    assert_eq!(state, State::Pending, "attempt 1 of 2 retries");
    assert_eq!(attempts, 1);

    let row = sqlx::query("SELECT run_at, last_error FROM rushwind_apalis_jobs WHERE task_id = $1")
        .bind(pushed.task_id.to_string())
        .fetch_one(storage.pool())
        .await
        .expect("row");
    let run_at: chrono::DateTime<chrono::Utc> = row.try_get("run_at").expect("run_at");
    let wait = (run_at - chrono::Utc::now()).num_milliseconds();
    assert!(
        wait > 0 && wait <= 1100,
        "backoff should schedule ~1s out, got {wait}ms"
    );
    assert_eq!(retry_backoff(1), chrono::Duration::seconds(1));
    let last_error = text_column(&storage, &pushed.task_id, "last_error").await;
    assert!(
        last_error.as_deref().is_some_and(|e| e.contains("boom")),
        "the failure message should be kept: {last_error:?}"
    );

    // Second run exhausts the budget.
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("second claim within the backoff window")
        .expect("claimed");
    assert_eq!(second.parts.task_id, pushed.task_id);
    assert_eq!(second.parts.attempt.current(), 2);
    storage
        .ack(&second.parts.context, &failure_response(&second))
        .await
        .expect("ack failure");
    tokio::time::sleep(Duration::from_millis(700)).await;

    let (state, attempts) = status_of(&mut storage, &pushed.task_id).await;
    assert_eq!(state, State::Killed, "attempt 2 of 2 is the last one");
    assert_eq!(attempts, 2);

    let row = sqlx::query("SELECT done_at FROM rushwind_apalis_jobs WHERE task_id = $1")
        .bind(pushed.task_id.to_string())
        .fetch_one(storage.pool())
        .await
        .expect("row");
    let done_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("done_at").expect("done_at");
    assert!(done_at.is_some(), "killed rows carry their death time");
}

/// A task whose worker dies mid-flight (visibility deadline passes
/// without a heartbeat) returns to the pool, its claim stamps cleared
/// and the orphaning recorded.
#[tokio::test]
async fn orphaned_task_is_requeued_after_lock_expiry() {
    let mut storage = setup(&unique_queue()).await;
    let pushed = storage.push(example_email()).await.expect("push");

    // Claim the task "manually" — no heartbeat of ours will refresh it.
    let mut claimer = storage.clone();
    let claimed = claimer
        .fetch_next(&WorkerId::new("rw-doomed-worker"))
        .await
        .expect("claim")
        .pop()
        .expect("exactly one task");
    assert_eq!(claimed.parts.task_id, pushed.task_id);

    // Age the visibility deadline into the past: the worker "died".
    sqlx::query(
        "UPDATE rushwind_apalis_jobs
         SET lock_at = now() - interval '1 second' WHERE task_id = $1",
    )
    .bind(pushed.task_id.to_string())
    .execute(storage.pool())
    .await
    .expect("age lock");

    // Another worker's heartbeat starts with a sweep, so the orphan is
    // back in the pool long before its first poll tick can re-claim it.
    let (_worker, rx) = drive(&storage, "rw-gravedigger");
    drop(rx);

    let mut observed_pending = false;
    for _ in 0..20 {
        let (state, _) = status_of(&mut storage, &pushed.task_id).await;
        if state == State::Pending {
            observed_pending = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(observed_pending, "the orphan should be re-queued");

    let lock_by = text_column(&storage, &pushed.task_id, "lock_by").await;
    let last_error = text_column(&storage, &pushed.task_id, "last_error").await;
    assert!(lock_by.is_none(), "claim stamps cleared");
    assert!(
        last_error
            .as_deref()
            .is_some_and(|e| e.contains("abandoned")),
        "orphaning should be recorded: {last_error:?}"
    );
}

/// A late ack for a task that orphan recovery already re-queued is
/// dropped, not applied.
#[tokio::test]
async fn stale_ack_after_orphan_recovery_is_dropped() {
    let mut storage = setup(&unique_queue()).await;
    let pushed = storage.push(example_email()).await.expect("push");

    let mut claimer = storage.clone();
    let claimed = claimer
        .fetch_next(&WorkerId::new("rw-doomed-worker"))
        .await
        .expect("claim")
        .pop()
        .expect("one task");
    assert_eq!(claimed.parts.task_id, pushed.task_id);

    // Orphan-recover the row first, and push its due time out so the
    // writer's poll cannot re-claim it mid-test…
    sqlx::query(
        "UPDATE rushwind_apalis_jobs
         SET status = 'pending', lock_by = NULL, lock_at = NULL,
             run_at = now() + interval '1 hour'
         WHERE task_id = $1",
    )
    .bind(pushed.task_id.to_string())
    .execute(storage.pool())
    .await
    .expect("requeue");

    // …then the dead worker's success ack arrives. It must not mark
    // the re-queued task done.
    storage
        .ack(&claimed.parts.context, &success_response(&claimed))
        .await
        .expect("ack accepted");
    let (_worker, rx) = drive(&storage, "rw-ack-writer");
    drop(rx);
    tokio::time::sleep(Duration::from_millis(700)).await;

    let (state, _) = status_of(&mut storage, &pushed.task_id).await;
    assert_eq!(state, State::Pending, "stale acks never finalize tasks");
}

/// The kill switch: an `Error::Abort` ack goes straight to `killed`
/// regardless of remaining attempts.
#[tokio::test]
async fn aborted_task_is_killed_immediately() {
    let mut storage = setup(&unique_queue()).await;
    let pushed = storage.push(example_email()).await.expect("push");

    let claimed = storage
        .fetch_next(&WorkerId::new("rw-abort-probe"))
        .await
        .expect("claim")
        .pop()
        .expect("one task");
    storage
        .ack(&claimed.parts.context, &abort_response(&claimed))
        .await
        .expect("ack");
    let (_worker, rx) = drive(&storage, "rw-abort-writer");
    drop(rx);
    tokio::time::sleep(Duration::from_millis(700)).await;

    let (state, attempts) = status_of(&mut storage, &pushed.task_id).await;
    assert_eq!(state, State::Killed);
    assert_eq!(attempts, 1, "the abort consumed its first attempt");
}

/// len/is_empty track the pending pool, scheduled tasks park until
/// their run_at, and vacuum clears terminal rows older than the
/// retention window.
#[tokio::test]
async fn len_is_empty_and_vacuum() {
    let mut storage = setup(&unique_queue()).await;
    assert!(storage.is_empty().await.expect("is_empty"));

    let row1 = storage.push(example_email()).await.expect("push");
    let row2 = storage.push(example_email()).await.expect("push");
    assert_eq!(storage.len().await.expect("len"), 2);

    // One claim sweeps both due tasks, in FIFO order.
    let claimed = storage
        .fetch_next(&WorkerId::new("rw-len-probe"))
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].parts.task_id, row1.task_id, "oldest first");
    assert_eq!(claimed[1].parts.task_id, row2.task_id);
    assert_eq!(storage.len().await.expect("len"), 0);

    // Finish one of them via the ack path.
    storage
        .ack(&claimed[0].parts.context, &success_response(&claimed[0]))
        .await
        .expect("ack");
    let (_worker, rx) = drive(&storage, "rw-vacuum-writer");
    drop(rx);
    tokio::time::sleep(Duration::from_millis(700)).await;
    // Nothing is pending: one row is done, the other still running.
    assert!(storage.is_empty().await.expect("is_empty"));

    // Age the terminal rows past the retention and vacuum them.
    sqlx::query(
        "UPDATE rushwind_apalis_jobs SET done_at = now() - interval '8 days' WHERE queue = $1",
    )
    .bind(storage.queue())
    .execute(storage.pool())
    .await
    .expect("age done_at");
    let removed = storage.vacuum().await.expect("vacuum");
    assert_eq!(removed, 1, "the acked task is vacuumed");

    assert!(
        storage
            .fetch_by_id(&row1.task_id)
            .await
            .expect("by id")
            .is_none(),
        "the vacuumed task is gone"
    );
    assert!(
        storage
            .fetch_by_id(&row2.task_id)
            .await
            .expect("by id")
            .is_some(),
        "the running task survives the vacuum"
    );
}

/// Scheduled tasks are invisible to the claim query until run_at, then
/// claim in run_at order with their payload intact.
#[tokio::test]
async fn scheduled_task_is_not_claimed_before_run_at() {
    let mut storage = setup(&unique_queue()).await;

    let parts = storage
        .schedule(example_email(), chrono::Utc::now().timestamp() + 1)
        .await
        .expect("schedule");

    let probe = WorkerId::new("rw-schedule-probe");
    let claimed = storage.fetch_next(&probe).await.expect("fetch");
    assert!(
        claimed.is_empty(),
        "a task scheduled a second out must not be claimed yet"
    );

    tokio::time::sleep(Duration::from_millis(1300)).await;
    let claimed = storage.fetch_next(&probe).await.expect("fetch");
    assert_eq!(claimed.len(), 1, "the task is due now");
    assert_eq!(claimed[0].parts.task_id, parts.task_id);
    assert_eq!(claimed[0].args, example_email());
    assert_eq!(claimed[0].parts.attempt.current(), 1);
}
