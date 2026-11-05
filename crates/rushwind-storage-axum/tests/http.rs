//! HTTP end to end over the in-memory engine: CRUD routes, the full
//! list-query vocabulary, error mapping, the viewer hook, the deny
//! default of an unmounted policy, and the tenancy-payload guard.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rushwind_storage::{ColumnKind, QueryCtx, Record, Repository, Schema, Viewer};
use rushwind_storage_axum::CrudApi;
use rushwind_storage_memory::MemoryRepo;
use serde_json::{json, Value};
use tower::ServiceExt;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("schema is valid")
}

/// The tenant-scoped twin of [`schema`]: the conventional tenant column,
/// declared as the schema's tenant column.
fn tenant_schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("owner_id", ColumnKind::Int)
        .column("tenant_id", ColumnKind::Int)
        .tenant_column("tenant_id")
        .build()
        .expect("schema is valid")
}

fn app() -> axum::Router {
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    // All-access is an explicit opt-in since the fail-closed flip: the
    // deny default would 404 every read these tests exercise.
    CrudApi::new(Arc::new(repo))
        .with_viewer(Arc::new(|_headers| Viewer::all()))
        .router()
}

async fn send_json(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request builds"),
        )
        .await
        .expect("response arrives");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json body")
    };
    (status, json)
}

async fn send(app: &axum::Router, method: &str, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response arrives");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json body")
    };
    (status, json)
}

#[tokio::test]
async fn create_then_get_roundtrip() {
    let app = app();
    let (status, stored) = send_json(
        &app,
        "POST",
        "/",
        json!({"name": "bolt", "age": 7, "score": 2.5, "owner_id": 1, "unit_id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let id = stored["id"].as_i64().expect("id backfilled");

    let (status, row) = send(&app, "GET", &format!("/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["name"], "bolt");
    assert_eq!(row["age"], 7);
    assert_eq!(row["score"], 2.5);
}

#[tokio::test]
async fn list_with_aip_filter_sort_and_mask() {
    let app = app();
    for (name, age) in [("alpha", 5), ("Bravo", 10), ("charlie", 15)] {
        send_json(
            &app,
            "POST",
            "/",
            json!({"name": name, "age": age, "score": null, "owner_id": 1, "unit_id": 1}),
        )
        .await;
    }

    let (status, page) = send(
        &app,
        "GET",
        "/?filter=age%20%3E%3D%2010&sort=age:desc&fields=id,name",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = page["items"].as_array().expect("items array");
    let names: Vec<&str> = items
        .iter()
        .map(|row| row["name"].as_str().expect("masked name"))
        .collect();
    assert_eq!(names, vec!["charlie", "Bravo"]);
    // The mask projects: only id and name survive.
    assert!(items
        .iter()
        .all(|row| row.as_object().expect("obj").len() == 2));
    assert_eq!(page["total"], 2);
}

#[tokio::test]
async fn list_accepts_the_protojson_document() {
    let app = app();
    for (name, age) in [("alpha", 5), ("Bravo", 10), ("charlie", 15)] {
        send_json(
            &app,
            "POST",
            "/",
            json!({"name": name, "age": age, "score": null, "owner_id": 1, "unit_id": 1}),
        )
        .await;
    }

    // Exactly the document an external client would put on the wire (URL-encoded).
    let q = r#"{"filterExpr":{"conditions":[{"field":"age","op":"GTE","value":10}]},"paginationType":{"pageBased":{"page":1,"pageSize":10}},"sorting":[{"field":"age","direction":"ASC"}]}"#;
    let encoded = q
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-=._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect::<String>();
    let (status, page) = send(&app, "GET", &format!("/?q={encoded}")).await;
    assert_eq!(status, StatusCode::OK);
    let items = page["items"].as_array().expect("items array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["name"], "Bravo");
    assert_eq!(items[1]["name"], "charlie");
}

#[tokio::test]
async fn patch_put_and_delete_lifecycle() {
    let app = app();
    let (_, stored) = send_json(
        &app,
        "POST",
        "/",
        json!({"name": "v1", "age": 1, "score": null, "owner_id": 1, "unit_id": 1}),
    )
    .await;
    let id = stored["id"].as_i64().expect("id");

    let (status, updated) = send_json(&app, "PATCH", &format!("/{id}"), json!({"age": 2})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["age"], 2);
    assert_eq!(updated["name"], "v1", "patch touches only its own fields");

    let (status, upserted) = send_json(
        &app,
        "PUT",
        &format!("/{id}"),
        json!({"name": "v2", "age": 3, "score": null, "owner_id": 1, "unit_id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(upserted["name"], "v2");

    let (status, _) = send(&app, "DELETE", &format!("/{id}")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&app, "GET", &format!("/{id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn token_paging_streams_through_http() {
    let app = app();
    for i in 1..=5 {
        send_json(
            &app,
            "POST",
            "/",
            json!({"name": format!("w{i}"), "age": i, "score": null, "owner_id": 1, "unit_id": 1}),
        )
        .await;
    }

    let (status, first) = send(&app, "GET", "/?token=&limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"].as_array().expect("items").len(), 2);
    let token = first["nextToken"]
        .as_str()
        .expect("continuation")
        .to_owned();

    let (status, second) = send(&app, "GET", &format!("/?token={token}&limit=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["items"].as_array().expect("items").len(), 2);
}

#[tokio::test]
async fn errors_map_to_status_codes() {
    let app = app();
    let (status, body) = send(&app, "GET", "/404").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!body["error"].as_str().expect("error text").is_empty());

    let (status, body) = send(&app, "GET", "/?filter=unknown_col%20%3D%201").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]
        .as_str()
        .expect("error text")
        .contains("unknown"));

    let (status, _) = send_json(&app, "POST", "/", json!({"nope": 1})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send(&app, "GET", "/not-a-number").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn viewer_hook_scopes_every_request() {
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    let ctx = QueryCtx::all_access();
    for owner in [1, 2] {
        repo.create(
            ctx.clone(),
            rushwind_testkit::storage_conformance::widget(
                &format!("owned-by-{owner}"),
                1,
                None,
                owner,
                owner,
            ),
        )
        .await
        .expect("seed lands");
    }
    let api =
        CrudApi::new(Arc::new(repo)).with_viewer(Arc::new(|headers: &axum::http::HeaderMap| {
            match headers
                .get("x-owner")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok())
            {
                Some(owner) => Viewer::own(owner),
                // No identity header, no data — scopes close by default.
                None => Viewer::default(),
            }
        }));
    let app = api.router();

    let (_, page) = send(&app, "GET", "/").await;
    assert_eq!(
        page["total"], 0,
        "no header, no actor: the OWN scope denies"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .header("x-owner", "2")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("response arrives");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads");
    let page: Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(page["total"], 1);
    assert_eq!(
        page["items"][0]["name"], "owned-by-2",
        "a scoped viewer sees only its own rows at the HTTP edge"
    );
}

/// The fail-closed default (audit H2): a CrudApi with no viewer policy
/// denies reads — nothing is listed, every row reads as missing.
#[tokio::test]
async fn no_viewer_policy_denies_reads() {
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    repo.create(
        QueryCtx::all_access(),
        rushwind_testkit::storage_conformance::widget("seeded", 1, None, 1, 1).set("id", 1),
    )
    .await
    .expect("seed lands");
    let app = CrudApi::new(Arc::new(repo)).router();

    let (status, page) = send(&app, "GET", "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        page["total"], 0,
        "no policy, no visibility: the deny default lists nothing"
    );
    let (status, _) = send(&app, "GET", "/1").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the deny default hides every row"
    );
}

/// On a tenant-scoped table the deny default errors instead of just
/// coming back empty — the engine's MissingViewer fail-closed verdict,
/// mapped to a 400 (a malformed request, not a server fault, and not a
/// 401/403 that would collide with authn/authz semantics).
#[tokio::test]
async fn missing_viewer_on_a_tenant_table_is_a_bad_request() {
    let repo = MemoryRepo::new(tenant_schema()).expect("schema is valid");
    repo.create(
        QueryCtx::all_access(),
        Record::new()
            .set("name", "t5")
            .set("owner_id", 1)
            .set("tenant_id", 5)
            .set("id", 1),
    )
    .await
    .expect("seed lands");
    let app = CrudApi::new(Arc::new(repo)).router();

    let (status, body) = send(&app, "GET", "/").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("error text")
            .contains("viewer"),
        "the MissingViewer verdict rides the error envelope: {body}"
    );
}

/// The tenancy-payload guard (audit H3): a tenant viewer forging another
/// actor's `owner_id` gets a clean 400 before the engine would silently
/// stamp over it; its own actor id passes.
#[tokio::test]
async fn forged_owner_payloads_are_rejected() {
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    let app = CrudApi::new(Arc::new(repo))
        .with_viewer(Arc::new(|_headers| Viewer::tenant_user(5, 1)))
        .router();

    let (status, body) =
        send_json(&app, "POST", "/", json!({"name": "forged", "owner_id": 2})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("error text")
            .contains("owner_id"),
        "the rejection names the forged column: {body}"
    );

    let (status, stored) =
        send_json(&app, "POST", "/", json!({"name": "mine", "owner_id": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "the viewer's own id passes");
    let id = stored["id"].as_i64().expect("id");

    // And the boundary holds on patches too:
    let (status, _) = send_json(&app, "PATCH", &format!("/{id}"), json!({"owner_id": 3})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The guard's unit half: a unit-scoped viewer may address its own
/// `unit_id` and nothing else.
#[tokio::test]
async fn forged_unit_payloads_are_rejected() {
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    let app = CrudApi::new(Arc::new(repo))
        .with_viewer(Arc::new(|_headers| Viewer::unit(10)))
        .router();

    let (status, body) =
        send_json(&app, "POST", "/", json!({"name": "forged", "unit_id": 20})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("error text")
            .contains("unit_id"),
        "the rejection names the forged column: {body}"
    );

    let (status, _) = send_json(&app, "POST", "/", json!({"name": "kept", "unit_id": 10})).await;
    assert_eq!(status, StatusCode::CREATED, "the viewer's own unit passes");
}

/// The guard's tenant half: a tenant viewer forging the tenant column is
/// rejected at create, patch, and upsert — the row cannot migrate.
#[tokio::test]
async fn tenant_forgery_is_rejected_at_create_patch_and_upsert() {
    let repo = MemoryRepo::new(tenant_schema()).expect("schema is valid");
    let app = CrudApi::new(Arc::new(repo))
        .with_viewer(Arc::new(|_headers| Viewer::tenant(5)))
        .router();

    let (status, body) =
        send_json(&app, "POST", "/", json!({"name": "forge", "tenant_id": 6})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("error text")
            .contains("tenant_id"),
        "the rejection names the forged column: {body}"
    );

    let (status, stored) =
        send_json(&app, "POST", "/", json!({"name": "kept", "tenant_id": 5})).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "the viewer's own tenant passes"
    );
    assert_eq!(stored["tenant_id"], 5);
    let id = stored["id"].as_i64().expect("id");

    let (status, _) = send_json(&app, "PATCH", &format!("/{id}"), json!({"tenant_id": 6})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send_json(
        &app,
        "PUT",
        &format!("/{id}"),
        json!({"name": "v2", "tenant_id": 6}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The row never migrated:
    let (status, row) = send(&app, "GET", &format!("/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["tenant_id"], 5, "the forged migrations never landed");
}

/// Platform viewers bypass the guard: they administer every row of every
/// tenant, addressing owners, units, and tenants explicitly.
#[tokio::test]
async fn platform_viewers_bypass_the_payload_guard() {
    let repo = MemoryRepo::new(tenant_schema()).expect("schema is valid");
    let app = CrudApi::new(Arc::new(repo))
        .with_viewer(Arc::new(|_headers| Viewer::all()))
        .router();

    let (status, stored) = send_json(
        &app,
        "POST",
        "/",
        json!({"name": "administered", "owner_id": 99, "tenant_id": 7}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "platform may write any row: {stored}"
    );
    assert_eq!(stored["tenant_id"], 7, "the explicit tenant survives");
}

/// Counts create events — the audit sink wired through the HTTP layer.
#[derive(Default)]
struct RecordingAuditor {
    creates: std::sync::Mutex<Vec<String>>,
}

impl rushwind_storage::Auditor for RecordingAuditor {
    fn record(&self, entry: rushwind_storage::AuditEntry) {
        if entry.action == rushwind_storage::AuditAction::Create {
            let name = entry.target.map(|_| "row".to_string()).unwrap_or_default();
            self.creates.lock().expect("log").push(name);
        }
    }
}

#[tokio::test]
async fn auditor_hook_receives_mutation_entries() {
    let auditor = Arc::new(RecordingAuditor::default());
    let repo = MemoryRepo::new(schema()).expect("schema is valid");
    let app = CrudApi::new(Arc::new(repo))
        .with_auditor(auditor.clone() as Arc<dyn rushwind_storage::Auditor>)
        .router();

    send_json(
        &app,
        "POST",
        "/",
        json!({"name": "logged", "age": 1, "score": null, "owner_id": 1, "unit_id": 1}),
    )
    .await;
    assert_eq!(
        auditor.creates.lock().expect("log").len(),
        1,
        "a create through HTTP must reach the wired audit sink"
    );
}
