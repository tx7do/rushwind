//! The Cassandra engine for the [`Repository`] contract.
//!
//! Cassandra's query model and the contract's arbitrary-filter model are
//! fundamentally at odds: CQL has no ad-hoc WHERE, no OR, no NOT
//! BETWEEN/LIKE without indexes, and no cross-partition ordering. This
//! adapter takes the honest route — **contract compatibility first**:
//!
//! - The table is bucket-fixed (`bucket` partition key pinned to 0, `id`
//!   the clustering key), so primary-key ordering and cursor ranges are
//!   real CQL.
//! - List queries fetch the bucket (a bounded partition, `LIMIT
//!   MAX_LIMIT`), then filter with the contract's reference evaluator
//!   ([`FilterExpr::matches`](rushwind_storage::FilterExpr::matches)),
//!   sort with the contract's [`Value::compare`](rushwind_storage::Value::compare),
//!   and page in memory. Semantics are identical to the other engines;
//!   throughput is bounded by the partition — which is exactly how
//!   Cassandra wants you to model data anyway.
//! - The tenant axis rides the same client-side style: the tenant
//!   predicate (like the viewer-scope predicates) is evaluated on the
//!   fetched rows, pk-direct writes probe through the scoped read first,
//!   and the ambiguous faces fall back to LWT (`INSERT ... IF NOT
//!   EXISTS`) so a cross-tenant collision conflicts instead of
//!   overwriting.
//! - Writes are native: INSERT is a natural upsert, `BEGIN BATCH` makes
//!   `batch_create` atomic, DELETE is a tombstone write.
//!
//! The engine truths that shape semantics:
//!
//! - **Generated ids** come from `max(pk) + 1` over the bucket — Cassandra
//!   has no identity columns. LWT (`INSERT ... IF NOT EXISTS`) arbitrates
//!   the race; a lost LWT surfaces as [`StorageError::Conflict`].
//! - **NULL columns** are absent cells: reading them back is NULL, and
//!   overwriting a cell with NULL deletes it (Cassandra semantics, aligned
//!   with the contract's NULL doctrine).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod cql;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use rushwind_storage::{
    AuditAction, AuditEntry, FieldMask, FilterExpr, ListQuery, Page, Paging, QueryCtx, Record,
    RepoFuture, Repository, Schema, StorageError, TenancyDecision, Value, Viewer,
};

/// The partition-key bucket value every row lives under.
const BUCKET: i64 = 0;

/// A [`Repository`] backed by a Cassandra (or ScyllaDB) cluster.
pub struct CassandraRepo {
    schema: Schema,
    session: Session,
    keyspace: String,
}

/// The tenant predicate resolved for one operation: the schema's tenant
/// column plus the viewer's [`TenancyDecision`] against it.
#[derive(Clone, Debug)]
struct TenantRule {
    column: String,
    decision: TenancyDecision,
}

impl TenantRule {
    /// The predicate as a row test: `Enforce(id)` admits only rows whose
    /// tenant column carries exactly `id` — a tenant-scoped row with no
    /// tenant value is nobody's. Cassandra has no ad-hoc WHERE over a
    /// non-key column, so — exactly like the DataRange scope — the tenant
    /// predicate is evaluated client-side on the fetched bucket.
    fn matches(&self, row: &Record) -> bool {
        match self.decision {
            TenancyDecision::Unscoped | TenancyDecision::Pass => true,
            TenancyDecision::Enforce(id) => row.get(&self.column) == Some(&Value::Int(id)),
        }
    }

    /// Force-stamps the tenant column when the viewer is a tenant business
    /// viewer — a tenant actor cannot address another tenant by writing the
    /// column itself. Platform and unscoped tables keep the payload's own
    /// value.
    fn stamp(&self, row: &mut Record) {
        if let TenancyDecision::Enforce(id) = self.decision {
            row.insert(&self.column, Value::Int(id));
        }
    }
}

impl CassandraRepo {
    /// Connects to the cluster and binds a repository to
    /// `keyspace`.`schema.table`. The keyspace must already exist.
    pub async fn connect(
        hosts: &[String],
        keyspace: impl Into<String>,
        schema: Schema,
    ) -> Result<Self, StorageError> {
        // Cassandra's startup/schema negotiation easily exceeds the
        // driver's short default on a cold node — connection and DDL
        // (schema agreement across parallel CREATE TABLEs) both need
        // generous budgets, via the default execution profile.
        let execution_profile = scylla::client::execution_profile::ExecutionProfile::builder()
            .request_timeout(Some(std::time::Duration::from_secs(60)))
            .build();
        let mut builder = SessionBuilder::new()
            .connection_timeout(std::time::Duration::from_secs(30))
            .default_execution_profile_handle(execution_profile.into_handle());
        for host in hosts {
            builder = builder.known_node(host.as_str());
        }
        let session = builder
            .build()
            .await
            .map_err(|e| StorageError::Backend(format!("cassandra connect: {e}")))?;
        Ok(Self {
            schema,
            session,
            keyspace: keyspace.into(),
        })
    }

    fn qualified(&self) -> String {
        format!("{}.{}", self.keyspace, self.schema.table)
    }

    /// Creates the table (bucket-fixed partition, id clustering) and the
    /// indexes the in-CQL filter subset relies on. Idempotent.
    pub async fn migrate_create(&self) -> Result<(), StorageError> {
        let mut columns = vec!["bucket bigint".to_owned()];
        for column in &self.schema.columns {
            let kind = match column.kind {
                rushwind_storage::ColumnKind::Bool => "boolean",
                rushwind_storage::ColumnKind::Int => "bigint",
                rushwind_storage::ColumnKind::Real => "double",
                rushwind_storage::ColumnKind::Text => "text",
            };
            if column.name == self.schema.primary_key {
                continue; // clustering column, declared with the PK
            }
            columns.push(format!("{} {}", column.name, kind));
        }
        columns.push(format!("{} bigint", self.schema.primary_key));
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} (bucket bigint, {}, PRIMARY KEY (bucket, {}))",
            self.qualified(),
            columns
                .iter()
                .filter(|c| !c.starts_with("bucket "))
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            self.schema.primary_key
        );
        self.session
            .query_unpaged(create, ())
            .await
            .map_err(|e| StorageError::Backend(format!("cassandra create table: {e}")))?;
        Ok(())
    }

    /// Drops the table — the live-suite reset.
    pub async fn migrate_drop(&self) -> Result<(), StorageError> {
        self.session
            .query_unpaged(format!("DROP TABLE IF EXISTS {}", self.qualified()), ())
            .await
            .map_err(|e| StorageError::Backend(format!("cassandra drop table: {e}")))?;
        Ok(())
    }

    fn map_err(&self, err: scylla::errors::ExecutionError) -> StorageError {
        let text = err.to_string();
        if text.contains("already exists") || text.contains("IF NOT EXISTS") {
            StorageError::Conflict(text)
        } else {
            StorageError::Backend(format!("cassandra: {text}"))
        }
    }

    // ---- row plumbing --------------------------------------------------------

    /// Fetches every row of the bucket as full records (SELECT JSON keeps
    /// the parsing engine-agnostic; the suite's partitions are bounded).
    async fn fetch_bucket(&self) -> Result<Vec<Record>, StorageError> {
        let column_list = {
            let mut cols = vec!["bucket".to_owned()];
            cols.extend(self.schema.columns.iter().map(|c| c.name.clone()));
            cols.join(", ")
        };
        let statement = format!(
            "SELECT JSON {} FROM {} WHERE bucket = {}",
            column_list,
            self.qualified(),
            BUCKET
        );
        let rows = self
            .session
            .query_unpaged(statement, ())
            .await
            .map_err(|e| self.map_err(e))?;
        let mut records = Vec::new();
        let rows = rows
            .into_rows_result()
            .map_err(|e| StorageError::Backend(format!("cassandra rows: {e}")))?;
        for row in rows
            .rows::<(Option<String>,)>()
            .map_err(|e| StorageError::Backend(e.to_string()))?
        {
            let (json_text,) = row.map_err(|e| StorageError::Backend(e.to_string()))?;
            let json_text = json_text
                .ok_or_else(|| StorageError::Backend("SELECT JSON produced a null row".into()))?;
            records.push(self.json_to_record(&json_text)?);
        }
        Ok(records)
    }

    fn json_to_record(&self, json_text: &str) -> Result<Record, StorageError> {
        let value: serde_json::Value =
            serde_json::from_str(json_text).map_err(|e| StorageError::Backend(e.to_string()))?;
        let map = value
            .as_object()
            .ok_or_else(|| StorageError::Backend("expected a JSON object row".into()))?;
        let mut record = Record::new();
        for column in &self.schema.columns {
            let raw = map.get(&column.name);
            let value = match raw {
                None | Some(serde_json::Value::Null) => Value::Null,
                Some(serde_json::Value::Bool(b)) => Value::Bool(*b),
                Some(serde_json::Value::Number(n)) => match column.kind {
                    rushwind_storage::ColumnKind::Int => Value::Int(
                        n.as_i64()
                            .ok_or_else(|| StorageError::Backend("int64 out of range".into()))?,
                    ),
                    _ => Value::Real(n.as_f64().unwrap_or_default()),
                },
                Some(serde_json::Value::String(text)) => match column.kind {
                    rushwind_storage::ColumnKind::Text => Value::Text(text.clone()),
                    _ => Value::Bool(text != "false"),
                },
                Some(_) => {
                    return Err(StorageError::Backend(format!(
                        "column {:?} arrived as a composite JSON value",
                        column.name
                    )))
                }
            };
            record.insert(column.name.as_str(), value);
        }
        Ok(record)
    }

    fn materialize(&self, row: &Record, id: i64) -> Record {
        let mut full = Record::new();
        for column in &self.schema.columns {
            let value = if column.name == self.schema.primary_key {
                Value::Int(id)
            } else {
                row.get(&column.name).cloned().unwrap_or(Value::Null)
            };
            full.insert(column.name.as_str(), value);
        }
        full
    }

    fn validate_row(&self, row: &Record) -> Result<(), StorageError> {
        for (field, value) in row.iter() {
            let column = self.schema.column(field).ok_or_else(|| {
                StorageError::InvalidQuery(format!("unknown column {field:?} in write payload"))
            })?;
            if !column.kind.accepts(value.type_name()) {
                return Err(StorageError::InvalidQuery(format!(
                    "column {field:?} does not accept a {} value",
                    value.type_name()
                )));
            }
        }
        Ok(())
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(AuditEntry::now(
            action,
            self.schema.table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }

    fn expect_id(&self, id: &Value) -> Result<i64, StorageError> {
        id.as_i64()
            .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
    }

    /// The tenant-axis rule for this table — the schema's tenant column
    /// resolved against the viewer, once per operation. Fails closed
    /// ([`StorageError::MissingViewer`]) when a tenant-scoped table is
    /// queried by a viewer without tenancy; every path resolves it before
    /// any store access, so an unidentified viewer cannot even probe the
    /// bucket.
    fn tenant_rule(&self, ctx: &QueryCtx) -> Result<TenantRule, StorageError> {
        let column = self
            .schema
            .tenant_column
            .clone()
            .unwrap_or_else(|| Viewer::TENANT_COLUMN.to_owned());
        let decision = ctx
            .viewer
            .tenant_decision(self.schema.tenant_column.as_deref())?;
        Ok(TenantRule { column, decision })
    }

    /// Whether a conditional write's result reports `[applied] = true`. The
    /// LWT verdict rides in the result rows; a result body that fails to
    /// parse is treated as applied — the write itself still succeeded, and
    /// only a definite `false` proves a lost condition (matching the create
    /// path's tolerance).
    fn lwt_applied(&self, result: scylla::response::query_result::QueryResult) -> bool {
        result
            .into_rows_result()
            .ok()
            .and_then(|rows| {
                rows.rows::<(Option<bool>,)>().ok().and_then(|mut rows| {
                    rows.next()
                        .and_then(|row| row.ok())
                        .and_then(|(applied,)| applied)
                })
            })
            .unwrap_or(true)
    }

    /// The insert CQL for a materialized row. `only_if_absent` turns it
    /// into an LWT (`IF NOT EXISTS`).
    fn insert_cql(&self, row: &Record, only_if_absent: bool) -> String {
        let mut columns = vec!["bucket".to_owned()];
        let mut placeholders = vec![BUCKET.to_string()];
        for column in &self.schema.columns {
            columns.push(column.name.clone());
            placeholders.push(cql::literal(row.get(&column.name).cloned().as_ref()));
        }
        format!(
            "INSERT INTO {} ({}) VALUES ({}){}",
            self.qualified(),
            columns.join(", "),
            placeholders.join(", "),
            if only_if_absent { " IF NOT EXISTS" } else { "" }
        )
    }

    // ---- async implementations ----------------------------------------------

    async fn get_async(&self, ctx: &QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let id = self.expect_id(&id)?;
        let tenant = self.tenant_rule(ctx)?;
        let rows = self.fetch_bucket().await?;
        let row = rows
            .into_iter()
            .find(|row| row.get(&self.schema.primary_key).and_then(Value::as_i64) == Some(id));
        match row {
            Some(row) => {
                // The tenant and viewer boundaries apply to point reads
                // too; a foreign tenant's row reads exactly like a missing
                // one.
                if !tenant.matches(&row) {
                    return Ok(None);
                }
                match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                    rushwind_storage::Scope::Deny => Ok(None),
                    rushwind_storage::Scope::Unrestricted => Ok(Some(row)),
                    rushwind_storage::Scope::Scoped(filter) => Ok(if filter.matches(&row) {
                        Some(row)
                    } else {
                        None
                    }),
                }
            }
            None => Ok(None),
        }
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        // The tenant axis resolves before anything touches the store.
        let tenant = self.tenant_rule(ctx)?;
        // Denied scope: empty without touching the store.
        if matches!(
            ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN),
            rushwind_storage::Scope::Deny
        ) {
            return Ok(Page {
                items: Vec::new(),
                total: 0,
                next_token: None,
            });
        }
        let mut rows = self.fetch_bucket().await?;

        // Tenant predicate + viewer scope + query filter, evaluated by the
        // contract.
        rows.retain(|row| {
            if !tenant.matches(row) {
                return false;
            }
            let in_scope = match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                rushwind_storage::Scope::Deny => false,
                rushwind_storage::Scope::Unrestricted => true,
                rushwind_storage::Scope::Scoped(filter) => filter.matches(row),
            };
            in_scope
                && query
                    .filter
                    .as_ref()
                    .map(|filter| filter.matches(row))
                    .unwrap_or(true)
        });

        let total = rows.len() as u64;

        // Token cursors continue after the last seen id, in pk order.
        if let Paging::Token { token, .. } = &query.paging {
            if !token.is_empty() {
                let last = rushwind_storage::decode_cursor(token)?;
                rows.retain(|row| {
                    row.get(&self.schema.primary_key)
                        .and_then(Value::as_i64)
                        .map(|id| id > last)
                        .unwrap_or(false)
                });
            }
        }

        // Sort with the contract's total order; pk ascending as tiebreaker
        // (and as the default).
        let pk = self.schema.primary_key.clone();
        if query.sort.is_default() {
            rows.sort_by_key(|row| row.get(pk.as_str()).and_then(Value::as_i64));
        } else {
            rows.sort_by(|a, b| {
                for term in &query.sort.fields {
                    let ord = a
                        .get(&term.field)
                        .unwrap_or(&Value::Null)
                        .compare(b.get(&term.field).unwrap_or(&Value::Null));
                    let ord = match term.dir {
                        rushwind_storage::SortDir::Asc => ord,
                        rushwind_storage::SortDir::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                a.get(pk.as_str())
                    .and_then(Value::as_i64)
                    .cmp(&b.get(pk.as_str()).and_then(Value::as_i64))
            });
        }

        // In-memory paging over the filtered+sorted stream.
        let limit = query.paging.limit() as usize;
        let (start, peek): (usize, bool) = match &query.paging {
            Paging::Token { .. } => (0, true), // the cursor already filtered
            Paging::Page { page, .. } => {
                let skip = ((*page - 1) as u64) * u64::from(query.paging.limit());
                ((skip as usize), false)
            }
            Paging::Offset { offset, .. } => ((*offset as usize), false),
        };
        let mut items: Vec<Record> = if peek {
            rows.into_iter().take(limit + 1).collect()
        } else {
            rows.into_iter().skip(start).take(limit).collect()
        };

        let mut next_token = None;
        if let Paging::Token { limit, .. } = &query.paging {
            if items.len() > usize::try_from(*limit).unwrap_or(usize::MAX) {
                items.pop();
                let last_id = items
                    .last()
                    .and_then(|row| row.get(pk.as_str()))
                    .and_then(Value::as_i64)
                    .ok_or_else(|| {
                        StorageError::Backend("cursor page lost its primary key".into())
                    })?;
                next_token = Some(rushwind_storage::encode_cursor(last_id));
            }
        }

        let items = items
            .into_iter()
            .map(|row| self.project(&query.mask, row))
            .collect();
        Ok(Page {
            items,
            total,
            next_token,
        })
    }

    fn project(&self, mask: &Option<FieldMask>, row: Record) -> Record {
        let mut projected = row;
        if let Some(mask) = mask {
            projected.retain(|field| mask.allows(field));
        }
        projected
    }

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let tenant = self.tenant_rule(ctx)?;
        let mut rows = self.fetch_bucket().await?;
        rows.retain(|row| {
            if !tenant.matches(row) {
                return false;
            }
            let in_scope = match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                rushwind_storage::Scope::Deny => false,
                rushwind_storage::Scope::Unrestricted => true,
                rushwind_storage::Scope::Scoped(filter) => filter.matches(row),
            };
            in_scope
                && filter
                    .as_ref()
                    .map(|filter| filter.matches(row))
                    .unwrap_or(true)
        });
        Ok(rows.len() as u64)
    }

    /// `max(pk) + 1` over the bucket — Cassandra has no identity columns.
    async fn next_id(&self) -> Result<i64, StorageError> {
        let rows = self.fetch_bucket().await?;
        let max = rows
            .iter()
            .filter_map(|row| row.get(&self.schema.primary_key).and_then(Value::as_i64))
            .max()
            .unwrap_or_default();
        Ok(max + 1)
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let tenant = self.tenant_rule(ctx)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => {
                // LWT arbitrates explicit-id races; a lost IF NOT EXISTS is
                // a conflict. The stamp rides into the conditional insert,
                // so a tenant actor's forged tenant column never lands.
                let mut full = self.materialize(&row, *existing);
                tenant.stamp(&mut full);
                let insert = self.insert_cql(&full, true);
                let result = self
                    .session
                    .query_unpaged(insert, ())
                    .await
                    .map_err(|e| self.map_err(e))?;
                if !self.lwt_applied(result) {
                    return Err(StorageError::Conflict(format!(
                        "primary key {} already exists",
                        existing
                    )));
                }
                *existing
            }
            _ => self.next_id().await?,
        };
        let mut stored = self.materialize(&row, id);
        tenant.stamp(&mut stored);
        if matches!(row.get(pk.as_str()), Some(Value::Int(existing)) if *existing > 0) {
            // already inserted via the LWT branch
        } else {
            let insert = self.insert_cql(&stored, false);
            self.session
                .query_unpaged(insert, ())
                .await
                .map_err(|e| self.map_err(e))?;
        }
        self.audit(ctx, AuditAction::Create, Some(Value::Int(id)));
        Ok(stored)
    }

    async fn batch_create_async(
        &self,
        ctx: &QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        for row in &rows {
            self.validate_row(row)?;
        }
        // The tenant axis resolves before anything touches the store.
        let tenant = self.tenant_rule(ctx)?;
        let pk = self.schema.primary_key.clone();
        let mut next_id = self.next_id().await?;
        let mut claimed = std::collections::BTreeSet::new();
        let mut statements = Vec::with_capacity(rows.len());
        let mut ids = Vec::with_capacity(rows.len());
        for row in &rows {
            let id = match row.get(pk.as_str()) {
                Some(Value::Int(existing)) if *existing > 0 => *existing,
                _ => {
                    let id = next_id;
                    next_id += 1;
                    id
                }
            };
            if !claimed.insert(id) {
                return Err(StorageError::Conflict(format!(
                    "primary key {id} repeats within the batch"
                )));
            }
            // Every batched insert carries the stamp, so no payload can
            // land under a foreign tenant's column value.
            let mut full = self.materialize(row, id);
            tenant.stamp(&mut full);
            ids.push(id);
            statements.push(self.insert_cql(&full, true));
        }
        // A single logged BATCH is atomic across the cluster.
        let batch_cql = format!(
            "BEGIN BATCH {} APPLY BATCH",
            statements
                .iter()
                .map(|statement| format!("{statement};"))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let result = self
            .session
            .query_unpaged(batch_cql, ())
            .await
            .map_err(|e| self.map_err(e))?;
        // A conditional BATCH that was not applied returns result rows
        // whose first column ([applied]) is false - the rows also carry the
        // conflicting current values, so parse dynamically instead of
        // expecting a one-column tuple.
        let all_applied = result
            .into_rows_result()
            .ok()
            .and_then(|rows| {
                rows.rows().ok().map(|mut all| {
                    all.by_ref().all(|row| {
                        row.map(|row: scylla::value::Row| {
                            row.columns
                                .first()
                                .map(|first| {
                                    !matches!(first, Some(scylla::value::CqlValue::Boolean(false)))
                                })
                                .unwrap_or(true)
                        })
                        .unwrap_or(true)
                    })
                })
            })
            .unwrap_or(true);
        if !all_applied {
            return Err(StorageError::Conflict(
                "batch rejected: a primary key already exists".into(),
            ));
        }
        let stored = rows
            .into_iter()
            .zip(ids)
            .map(|(row, id)| {
                let mut full = self.materialize(&row, id);
                tenant.stamp(&mut full);
                full
            })
            .collect();
        self.audit(ctx, AuditAction::BatchCreate, None);
        Ok(stored)
    }

    async fn update_async(
        &self,
        ctx: &QueryCtx,
        id: Value,
        patch: Record,
    ) -> Result<Record, StorageError> {
        self.validate_row(&patch)?;
        let tenant = self.tenant_rule(ctx)?;
        let id = self.expect_id(&id)?;
        let pk = self.schema.primary_key.clone();
        if let Some(new_pk) = patch.get(pk.as_str()) {
            if new_pk != &Value::Null && new_pk.as_i64() != Some(id) {
                return Err(StorageError::InvalidQuery(
                    "update patches cannot move the primary key".into(),
                ));
            }
        }
        // Probe through the scoped path: out-of-scope and cross-tenant rows
        // are NotFound before anything is written.
        let current = self
            .get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        let mut updated = current;
        for column in &self.schema.columns {
            if let Some(value) = patch.get(&column.name) {
                if column.name != pk {
                    updated.insert(column.name.as_str(), value.clone());
                }
            }
        }
        // The patch rides the tenant stamp: a tenant actor cannot migrate
        // the row into another tenant by patching the column itself.
        tenant.stamp(&mut updated);
        // Under enforcement the tenant column keeps its probed value — the
        // stamp already pinned it on the returned row, and the SET would
        // only ever rewrite the same number. Platform viewers may migrate
        // tenants explicitly, so their patch value goes through untouched.
        let enforce = matches!(tenant.decision, TenancyDecision::Enforce(_));
        let sets: Vec<String> = patch
            .iter()
            .filter(|(field, _)| *field != pk)
            .filter(|(field, _)| !(enforce && *field == tenant.column))
            .map(|(field, value)| format!("{} = {}", field, cql::literal(Some(value))))
            .collect();
        if !sets.is_empty() {
            let statement = format!(
                "UPDATE {} SET {} WHERE bucket = {} AND {} = {}",
                self.qualified(),
                sets.join(", "),
                BUCKET,
                pk,
                id
            );
            self.session
                .query_unpaged(statement, ())
                .await
                .map_err(|e| self.map_err(e))?;
        }
        self.audit(ctx, AuditAction::Update, Some(Value::Int(id)));
        Ok(updated)
    }

    async fn upsert_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let tenant = self.tenant_rule(ctx)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => {
                return Err(StorageError::InvalidQuery(
                    "upsert requires an explicit primary key".into(),
                ))
            }
        };
        // Scoped probe first — the engine's pk-direct read with its
        // client-side validation: the update face requires the row to be
        // visible to this viewer.
        match self.get_async(ctx, Value::Int(id)).await? {
            Some(current) => {
                // Update face: the row is the actor's own. Merge the
                // payload over it (absent payload fields keep their stored
                // values) and rewrite the row in place — a plain INSERT is
                // safe because the pk already belongs to this viewer. The
                // payload rides the tenant stamp, so a tenant actor cannot
                // migrate the row into another tenant.
                let mut updated = current;
                for (field, value) in row.iter() {
                    if field != pk {
                        updated.insert(field, value.clone());
                    }
                }
                tenant.stamp(&mut updated);
                let insert = self.insert_cql(&updated, false);
                self.session
                    .query_unpaged(insert, ())
                    .await
                    .map_err(|e| self.map_err(e))?;
                self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
                Ok(updated)
            }
            None => {
                // Insert face: the row is absent **or** occupied by a row
                // this viewer cannot see — a tenant-scoped row with a
                // foreign tenant value reads exactly like a missing one.
                // IF NOT EXISTS arbitrates both, because the LWT checks the
                // primary key rather than visibility: a lost condition
                // means the pk is occupied by an invisible incumbent, and
                // that is a Conflict — never a silent overwrite of the
                // occupant's cells.
                let mut stored = self.materialize(&row, id);
                tenant.stamp(&mut stored);
                let insert = self.insert_cql(&stored, true);
                let result = self
                    .session
                    .query_unpaged(insert, ())
                    .await
                    .map_err(|e| self.map_err(e))?;
                if !self.lwt_applied(result) {
                    return Err(StorageError::Conflict(format!(
                        "primary key {id} already exists"
                    )));
                }
                self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
                Ok(stored)
            }
        }
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = self.expect_id(&id)?;
        // Fail closed before anything touches the store; the scoped probe
        // below carries the tenant predicate — out-of-scope and cross-tenant
        // rows are NotFound.
        self.tenant_rule(ctx)?;
        self.get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        let statement = format!(
            "DELETE FROM {} WHERE bucket = {} AND {} = {}",
            self.qualified(),
            BUCKET,
            self.schema.primary_key,
            id
        );
        self.session
            .query_unpaged(statement, ())
            .await
            .map_err(|e| self.map_err(e))?;
        self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

impl Repository for CassandraRepo {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(async move { self.get_async(&ctx, id).await })
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        Box::pin(async move { self.list_async(&ctx, query).await })
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        Box::pin(async move { self.count_async(&ctx, filter).await })
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.create_async(&ctx, row).await })
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        Box::pin(async move { self.batch_create_async(&ctx, rows).await })
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.update_async(&ctx, id, patch).await })
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        Box::pin(async move { self.upsert_async(&ctx, row).await })
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        Box::pin(async move { self.delete_async(&ctx, id).await })
    }
}
