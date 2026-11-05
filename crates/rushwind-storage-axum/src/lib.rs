//! The axum endpoint layer for the [`Repository`] contract — the last mile
//! of the list-query story: HTTP in, rows out.
//!
//! [`CrudApi::new`] binds a repository; [`CrudApi::router`] mounts the CRUD
//! surface, ready to nest under any path:
//!
//! ```ignore
//! let app = Router::new().nest("/api/widgets", CrudApi::new(repo).router());
//! ```
//!
//! # Routes
//!
//! | method + path | behavior |
//! |:---|:---|
//! | `GET /` | list — query vocabulary below |
//! | `POST /` | create — JSON object body, schema-validated |
//! | `GET /{id}` | point read |
//! | `PATCH /{id}` | update by patch |
//! | `PUT /{id}` | upsert |
//! | `DELETE /{id}` | delete |
//!
//! # The list-query vocabulary
//!
//! `GET /?q={...}` takes a **protojson** `PagingRequest` document verbatim
//! (the same bytes any client sends). For ad-hoc callers the individual
//! parameters spell the same request:
//!
//! - `filter` — AIP text (`age >= 10 AND name LIKE "a%"`)
//! - `page`+`size`, or `offset`+`limit`, or `token`+`limit`
//! - `sort` — `name:desc,age:asc`
//! - `fields` — comma-separated field mask (`id,name`)
//!
//! # Tenancy
//!
//! The default is **fail closed**: without a viewer policy every request
//! runs [`Viewer::default`] — nothing is visible, and on a tenant-scoped
//! table the engine rejects the call outright with
//! [`StorageError::MissingViewer`] (mapped to 400). Production tenancy
//! plugs in through [`CrudApi::with_viewer`]: a function from request
//! headers to a [`Viewer`], applied on every call — the same scope
//! enforcement the conformance suite pins, now at the HTTP edge. The
//! usual shapes are one line: all-access (for demos and internal tools)
//! is the explicit `.with_viewer(Arc::new(|_| Viewer::all()))`, a real
//! deployment derives the viewer from its authenticated claims.
//!
//! On top of the engine's enforcement the edge runs a defense-in-depth
//! payload guard (`CrudApi::check_payload`): a viewer with a scoped range
//! or a tenant identity may not address tenancy columns it is not
//! entitled to (`owner_id`/`unit_id`/the schema's tenant column) — a
//! forged write answers a clean 400 before the engine would silently
//! stamp over it. Platform viewers ([`Viewer::all`]/[`Viewer::system`])
//! bypass the guard: they administer every row.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod json;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use rushwind_storage::{
    Auditor, FilterExpr, ListQuery, Paging, QueryCtx, Record, Repository, Sort, SortDir,
    StorageError, Tenancy, Value, Viewer,
};
use rushwind_storage_proto::wire::list_query_from_json;

/// The per-request viewer policy: headers in, tenancy scope out.
pub type ViewerFn = Arc<dyn Fn(&HeaderMap) -> Viewer + Send + Sync>;

/// The CRUD endpoint surface bound to one repository.
#[derive(Clone)]
pub struct CrudApi {
    repo: Arc<dyn Repository>,
    viewer: Option<ViewerFn>,
    auditor: Option<Arc<dyn Auditor>>,
}

impl CrudApi {
    /// Binds the surface to a repository. **Fail closed:** with no viewer
    /// policy installed every request runs [`Viewer::default`] — nothing
    /// is visible, writes land invisible on non-tenant tables, and
    /// tenant-scoped tables reject the call with
    /// [`StorageError::MissingViewer`]. All-access is an explicit choice:
    /// `.with_viewer(Arc::new(|_| Viewer::all()))`.
    pub fn new(repo: Arc<dyn Repository>) -> Self {
        Self {
            repo,
            viewer: None,
            auditor: None,
        }
    }

    /// Installs the tenancy policy — headers to viewer, enforced on every
    /// call by the engine underneath.
    pub fn with_viewer(mut self, viewer: ViewerFn) -> Self {
        self.viewer = Some(viewer);
        self
    }

    /// Attaches the audit sink mutations flow into (the contract's
    /// [`Auditor`] hook, fed by the engine on every write).
    pub fn with_auditor(mut self, auditor: Arc<dyn Auditor>) -> Self {
        self.auditor = Some(auditor);
        self
    }

    /// The mounted CRUD router; nest it under your API prefix.
    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(list).post(create))
            .route(
                "/{id}",
                get(get_one).patch(update).put(upsert).delete(delete_one),
            )
            .with_state(self)
    }

    /// The viewer this request runs under: the installed policy's answer,
    /// or the fail-closed [`Viewer::default`] when no policy is mounted.
    fn viewer(&self, headers: &HeaderMap) -> Viewer {
        self.viewer
            .as_ref()
            .map(|policy| policy(headers))
            .unwrap_or_default()
    }

    fn ctx(&self, viewer: Viewer) -> QueryCtx {
        match &self.auditor {
            Some(auditor) => QueryCtx::new(viewer).audited(Arc::clone(auditor)),
            None => QueryCtx::new(viewer),
        }
    }

    /// The defense-in-depth payload guard: a viewer with an actor, a
    /// unit, or a tenant identity may not address the matching tenancy
    /// columns with someone else's value — a forged `owner_id`,
    /// `unit_id`, or tenant column is rejected with
    /// [`StorageError::InvalidQuery`] (a clean 400) before the engine
    /// would silently force-stamp over it. Absent and `NULL` payloads
    /// pass: the engine stamps the defaults underneath. Platform viewers
    /// ([`Viewer::all`]/[`Viewer::system`], [`Tenancy::Platform`])
    /// bypass the guard — they administer every row of every tenant.
    fn check_payload(&self, viewer: &Viewer, row: &Record) -> Result<(), StorageError> {
        if viewer.tenancy == Tenancy::Platform {
            return Ok(());
        }
        if let Tenancy::Tenant(tenant) = viewer.tenancy {
            if let Some(column) = self.repo.schema().tenant_column.as_deref() {
                match row.get(column) {
                    None | Some(Value::Null) => {}
                    Some(Value::Int(id)) if *id == tenant => {}
                    Some(other) => {
                        return Err(StorageError::InvalidQuery(format!(
                            "the {column:?} payload column must carry the viewer's tenant \
                             {tenant}, got {other:?}"
                        )));
                    }
                }
            }
        }
        match (viewer.actor_id, row.get(Viewer::OWNER_COLUMN)) {
            (Some(actor), Some(Value::Int(owner))) if *owner != actor => {
                return Err(StorageError::InvalidQuery(format!(
                    "the {:?} payload column must carry the viewer's actor {actor}, got {owner}",
                    Viewer::OWNER_COLUMN
                )));
            }
            _ => {}
        }
        match (viewer.unit_id, row.get(Viewer::UNIT_COLUMN)) {
            (Some(unit), Some(Value::Int(id))) if *id != unit => {
                return Err(StorageError::InvalidQuery(format!(
                    "the {:?} payload column must carry the viewer's unit {unit}, got {id}",
                    Viewer::UNIT_COLUMN
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

// ---- handlers --------------------------------------------------------------

async fn list(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let query = match build_list_query(&params) {
        Ok(query) => query,
        Err(error) => return error_response(error),
    };
    match api.repo.list(api.ctx(api.viewer(&headers)), &query).await {
        Ok(page) => {
            let items: Vec<serde_json::Value> =
                page.items.iter().map(json::record_to_json).collect();
            (
                StatusCode::OK,
                Json(json!({
                    "items": items,
                    "total": page.total,
                    "nextToken": page.next_token,
                })),
            )
                .into_response()
        }
        Err(error) => error_response(error),
    }
}

async fn create(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let viewer = api.viewer(&headers);
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(row) => {
            if let Err(error) = api.check_payload(&viewer, &row) {
                return error_response(error);
            }
            match api.repo.create(api.ctx(viewer), row).await {
                Ok(stored) => {
                    (StatusCode::CREATED, Json(json::record_to_json(&stored))).into_response()
                }
                Err(error) => error_response(error),
            }
        }
        Err(error) => error_response(error),
    }
}

async fn get_one(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match api
        .repo
        .get(api.ctx(api.viewer(&headers)), Value::Int(id))
        .await
    {
        Ok(Some(row)) => (StatusCode::OK, Json(json::record_to_json(&row))).into_response(),
        Ok(None) => error_response(StorageError::NotFound),
        Err(error) => error_response(error),
    }
}

async fn update(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    let viewer = api.viewer(&headers);
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(patch) => {
            if let Err(error) = api.check_payload(&viewer, &patch) {
                return error_response(error);
            }
            match api
                .repo
                .update(api.ctx(viewer), Value::Int(id), patch)
                .await
            {
                Ok(stored) => (StatusCode::OK, Json(json::record_to_json(&stored))).into_response(),
                Err(error) => error_response(error),
            }
        }
        Err(error) => error_response(error),
    }
}

async fn upsert(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    let viewer = api.viewer(&headers);
    match json::record_from_json(&body, api.repo.schema()) {
        Ok(mut row) => {
            if let Err(error) = api.check_payload(&viewer, &row) {
                return error_response(error);
            }
            row.insert(api.repo.schema().primary_key.as_str(), Value::Int(id));
            match api.repo.upsert(api.ctx(viewer), row).await {
                Ok(stored) => (StatusCode::OK, Json(json::record_to_json(&stored))).into_response(),
                Err(error) => error_response(error),
            }
        }
        Err(error) => error_response(error),
    }
}

async fn delete_one(
    State(api): State<CrudApi>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(id) = parse_id(&id) else {
        return error_response(StorageError::InvalidQuery(
            "the id must be an integer".into(),
        ));
    };
    match api
        .repo
        .delete(api.ctx(api.viewer(&headers)), Value::Int(id))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(error),
    }
}

// ---- helpers ---------------------------------------------------------------

fn parse_id(raw: &str) -> Option<i64> {
    raw.parse::<i64>().ok()
}

fn error_response(error: StorageError) -> Response {
    let status = match &error {
        StorageError::NotFound => StatusCode::NOT_FOUND,
        StorageError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        // The caller's request carried no tenancy identity against a
        // tenant-scoped table: a malformed request (4xx), not a server
        // fault — and deliberately not 401/403, which belong to the
        // authn/authz layers.
        StorageError::MissingViewer => StatusCode::BAD_REQUEST,
        StorageError::Conflict(_) => StatusCode::CONFLICT,
        StorageError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        StorageError::Timeout | StorageError::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

/// Builds the list query: a `q` protojson document wins; otherwise the
/// individual parameters spell it — `filter` in AIP text, `sort` as
/// `field:dir` pairs, `fields` as the comma-separated mask.
fn build_list_query(params: &HashMap<String, String>) -> Result<ListQuery, StorageError> {
    if let Some(document) = params.get("q") {
        return list_query_from_json(document);
    }
    let mut query = match (
        params.get("token"),
        params.get("offset"),
        params.get("page"),
    ) {
        (Some(token), _, _) => ListQuery {
            paging: Paging::Token {
                token: token.clone(),
                limit: limit_of(params)?,
            },
            ..ListQuery::default()
        },
        (_, Some(offset), _) => ListQuery {
            paging: Paging::Offset {
                offset: offset
                    .parse()
                    .map_err(|_| StorageError::InvalidQuery("offset must be an integer".into()))?,
                limit: limit_of(params)?,
            },
            ..ListQuery::default()
        },
        (_, _, page) => ListQuery {
            paging: Paging::Page {
                page: match page {
                    Some(page) => page.parse().map_err(|_| {
                        StorageError::InvalidQuery("page must be an integer".into())
                    })?,
                    None => 1,
                },
                size: limit_of(params)?,
            },
            ..ListQuery::default()
        },
    };
    if let Some(filter) = params.get("filter") {
        query.filter = Some(FilterExpr::from_aip(filter)?);
    }
    if let Some(sort) = params.get("sort") {
        let mut fields = Vec::new();
        for term in sort.split(',') {
            let term = term.trim();
            let Some((field, dir)) = term.rsplit_once(':') else {
                return Err(StorageError::InvalidQuery(format!(
                    "sort term {term:?} must be field:asc or field:desc"
                )));
            };
            let dir = match dir.to_ascii_lowercase().as_str() {
                "asc" => SortDir::Asc,
                "desc" => SortDir::Desc,
                other => {
                    return Err(StorageError::InvalidQuery(format!(
                        "sort direction {other:?} must be asc or desc"
                    )))
                }
            };
            fields.push(rushwind_storage::SortField {
                field: field.trim().to_owned(),
                dir,
            });
        }
        query.sort = Sort { fields };
    }
    if let Some(fields) = params.get("fields") {
        query.mask = Some(rushwind_storage::FieldMask::of(
            fields.split(',').map(str::trim).filter(|f| !f.is_empty()),
        ));
    }
    Ok(query)
}

fn limit_of(params: &HashMap<String, String>) -> Result<u32, StorageError> {
    let raw = params
        .get("size")
        .or_else(|| params.get("limit"))
        .map(String::as_str)
        .unwrap_or("20");
    raw.parse::<u32>()
        .map_err(|_| StorageError::InvalidQuery("page size must be an integer".into()))
}
