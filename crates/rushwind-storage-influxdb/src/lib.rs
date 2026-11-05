//! The InfluxDB engine for the [`Repository`] contract
//! (InfluxQL over the 1.x HTTP API).
//!
//! A time-series database has no primary keys, no arbitrary WHERE, and no
//! cross-series ordering — the adapter closes that gap with the same
//! pattern as the Cassandra engine:
//!
//! - A measurement is a table; the contract primary key is a **series
//!   tag** (`id`), giving every row its own series and making same-id
//!   writes overwrite in place (identical series + timestamp).
//! - Writes are line protocol with an explicit epoch-0 timestamp, so the
//!   overwrite is deterministic. Absent/null fields are simply omitted
//!   (InfluxDB reads them back as NULL).
//! - List/count fetch the measurement and filter with the contract's
//!   reference evaluator ([`FilterExpr::matches`](rushwind_storage::FilterExpr::matches)),
//!   sort with [`Value::compare`](rushwind_storage::Value::compare), and
//!   page in memory — semantics identical to the other engines. The tenant
//!   predicate conjoins beside the viewer-scope predicates in that same
//!   in-memory evaluation, and writes stamp/probe through it.
//! - Deletes use InfluxQL `DELETE ... WHERE "id" = '…'`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod influx;

/// Escaping helpers shared between the query and write paths.
pub(crate) mod rushwind_storage_influx_escape {
    pub fn query(text: &str) -> String {
        text.replace(' ', "%20")
    }
}

use rushwind_storage::{
    AuditAction, AuditEntry, FieldMask, FilterExpr, ListQuery, Page, Paging, QueryCtx, Record,
    RepoFuture, Repository, Schema, StorageError, TenancyDecision, Value, Viewer,
};

/// A [`Repository`] backed by an InfluxDB 1.x HTTP endpoint.
pub struct InfluxRepo {
    schema: Schema,
    base: String,
    database: String,
    http: reqwest::Client,
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
    /// tenant value is nobody's. Conjoined beside the viewer-scope
    /// predicates in the adapter's in-memory evaluation of the fetched
    /// series.
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

impl InfluxRepo {
    /// Binds a repository to an InfluxDB endpoint (`http://host:8086`) and
    /// a schema. The measurement is the schema's table; the database must
    /// already exist.
    pub fn new(endpoint: impl Into<String>, database: impl Into<String>, schema: Schema) -> Self {
        let mut base = endpoint.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            schema,
            base,
            database: database.into(),
            http: reqwest::Client::new(),
        }
    }

    async fn write_lines(&self, lines: Vec<String>) -> Result<(), StorageError> {
        if lines.is_empty() {
            return Ok(());
        }
        let url = format!(
            "{}/write?db={}",
            self.base,
            rushwind_storage_influx_escape::query(&self.database)
        );
        let body = lines.join("\n");
        let response = self
            .http
            .post(&url)
            .header("content-type", "text/plain")
            .body(body)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("influxdb write transport: {e}")))?;
        let status = response.status().as_u16();
        if status != 204 {
            let text = response.text().await.unwrap_or_default();
            return Err(StorageError::Backend(format!(
                "influxdb write failed ({status}): {text}"
            )));
        }
        Ok(())
    }

    /// Runs an InfluxQL query and flattens the single-series result into
    /// records; a query against an empty measurement yields no series.
    async fn query_records(&self, influxql: &str) -> Result<Vec<Record>, StorageError> {
        use serde_json::Value as Json;
        let url = format!(
            "{}/query?db={}",
            self.base,
            rushwind_storage_influx_escape::query(&self.database)
        );
        let response = self
            .http
            .post(&url)
            .form(&[("q", influxql)])
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("influxdb query transport: {e}")))?;
        let status = response.status().as_u16();
        let body: Json = response.json().await.unwrap_or(Json::Null);
        if status != 200 {
            let message = body
                .pointer("/results/0/error")
                .and_then(Json::as_str)
                .unwrap_or("unknown error");
            // A query against a measurement with no data reads as empty.
            if message.contains("measurement not found") {
                return Ok(Vec::new());
            }
            return Err(StorageError::Backend(format!(
                "influxdb query failed ({status}): {message}"
            )));
        }
        let Some(series) = body
            .pointer("/results/0/series")
            .and_then(Json::as_array)
            .and_then(|series| series.first())
        else {
            return Ok(Vec::new());
        };
        let columns: Vec<String> = series
            .get("columns")
            .and_then(Json::as_array)
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| column.as_str().unwrap_or_default().to_owned())
                    .collect()
            })
            .unwrap_or_default();
        let values = series
            .get("values")
            .and_then(Json::as_array)
            .cloned()
            .unwrap_or_default();
        let mut records = Vec::with_capacity(values.len());
        for row in values {
            let mut record = Record::new();
            for (position, column) in columns.iter().enumerate() {
                if column == "time" {
                    continue; // engine bookkeeping, not a contract column
                }
                let value =
                    match row.get(position) {
                        None | Some(Json::Null) => Value::Null,
                        Some(Json::Bool(b)) => Value::Bool(*b),
                        Some(Json::Number(n)) => match self.schema.column(column).map(|c| c.kind) {
                            Some(rushwind_storage::ColumnKind::Real) => {
                                Value::Real(n.as_f64().unwrap_or_default())
                            }
                            _ => Value::Int(n.as_i64().ok_or_else(|| {
                                StorageError::Backend("int64 out of range".into())
                            })?),
                        },
                        Some(Json::String(text)) => {
                            // Tags ride the wire as strings — the primary key
                            // (a series tag) and integer columns convert back
                            // to the contract kinds.
                            let is_pk = column.as_str() == self.schema.primary_key;
                            let is_int_tag = self.schema.column(column).map(|c| c.kind)
                                == Some(rushwind_storage::ColumnKind::Int);
                            if is_pk || is_int_tag {
                                Value::Int(text.parse().map_err(|_| {
                                    StorageError::Backend(format!(
                                        "column {column:?} is not an integer"
                                    ))
                                })?)
                            } else {
                                Value::Text(text.clone())
                            }
                        }
                        Some(_) => {
                            return Err(StorageError::Backend(format!(
                                "column {column:?} arrived as a composite JSON value"
                            )))
                        }
                    };
                record.insert(column.clone().as_str(), value);
            }
            // Columns this measurement has never carried are absent from
            // the series response; they are NULL by the contract doctrine.
            for column in &self.schema.columns {
                if record.get(&column.name).is_none() {
                    record.insert(column.name.as_str(), Value::Null);
                }
            }
            records.push(record);
        }
        Ok(records)
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(AuditEntry::now(
            action,
            self.schema.table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }

    /// All primary keys currently in the measurement — the raw existence
    /// probe, deliberately unscoped: an id occupied by a row the caller
    /// cannot see still counts as occupied, exactly like the reference
    /// engine's create/upsert collision check.
    async fn fetch_all_ids(&self) -> Result<Vec<i64>, StorageError> {
        let ctx = QueryCtx::all_access();
        let tenant = self.tenant_rule(&ctx)?;
        Ok(self
            .visible_rows(&tenant, &ctx, None)
            .await?
            .into_iter()
            .filter_map(|row| row.get(&self.schema.primary_key).and_then(Value::as_i64))
            .collect())
    }

    fn expect_id(&self, id: &Value) -> Result<i64, StorageError> {
        id.as_i64()
            .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
    }

    /// The tenant-axis rule for this table — the schema's tenant column
    /// resolved against the viewer, once per operation. Fails closed
    /// ([`StorageError::MissingViewer`]) when a tenant-scoped table is
    /// queried by a viewer without tenancy; every path resolves it before
    /// any HTTP round-trip, so an unidentified viewer cannot even probe the
    /// measurement.
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

    /// The tenant-scoped, scope-filtered view of the measurement, contract
    /// semantics: the tenant predicate conjoins beside the viewer-scope
    /// predicates, then the query filter.
    async fn visible_rows(
        &self,
        tenant: &TenantRule,
        ctx: &QueryCtx,
        filter: Option<&FilterExpr>,
    ) -> Result<Vec<Record>, StorageError> {
        let mut rows = self
            .query_records(&influx::select_all(&self.schema.table))
            .await?;
        rows.retain(|row| {
            if !tenant.matches(row) {
                return false;
            }
            let in_scope = match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                rushwind_storage::Scope::Deny => false,
                rushwind_storage::Scope::Unrestricted => true,
                rushwind_storage::Scope::Scoped(scope) => scope.matches(row),
            };
            in_scope
                && filter
                    .as_ref()
                    .map(|filter| filter.matches(row))
                    .unwrap_or(true)
        });
        Ok(rows)
    }

    // ---- async implementations ----------------------------------------------

    async fn get_async(&self, ctx: &QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let tenant = self.tenant_rule(ctx)?;
        let id = self.expect_id(&id)?;
        let mut rows = self
            .visible_rows(
                &tenant,
                ctx,
                Some(&FilterExpr::cond(
                    self.schema.primary_key.as_str(),
                    rushwind_storage::Op::Eq,
                    [Value::Int(id)],
                )),
            )
            .await?;
        Ok(rows.pop())
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        let tenant = self.tenant_rule(ctx)?;
        let mut rows = self
            .visible_rows(&tenant, ctx, query.filter.as_ref())
            .await?;
        let total = rows.len() as u64;

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

        let limit = query.paging.limit() as usize;
        let (start, peek): (usize, bool) = match &query.paging {
            Paging::Token { token, .. } => {
                if !token.is_empty() {
                    let last = rushwind_storage::decode_cursor(token)?;
                    rows.retain(|row| {
                        row.get(pk.as_str())
                            .and_then(Value::as_i64)
                            .map(|id| id > last)
                            .unwrap_or(false)
                    });
                }
                (0, true)
            }
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
        let rows = self.visible_rows(&tenant, ctx, filter.as_ref()).await?;
        Ok(rows.len() as u64)
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let tenant = self.tenant_rule(ctx)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => self.next_id().await?,
        };
        // Raw existence probe before the write: InfluxDB overwrites a
        // repeated series+timestamp silently, so an id already in the
        // measurement — any tenant's — must conflict here rather than
        // clobber the incumbent point (the same check `batch_create`
        // runs).
        if self.fetch_all_ids().await?.contains(&id) {
            return Err(StorageError::Conflict(format!(
                "primary key {id} already exists"
            )));
        }
        let mut stored = self.materialize(&row, id);
        tenant.stamp(&mut stored);
        self.write_lines(vec![influx::line(&self.schema.table, &stored)])
            .await?;
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
        let mut ids = Vec::with_capacity(rows.len());
        let mut lines = Vec::with_capacity(rows.len());
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
            // Every line carries the stamp, so no payload can land under a
            // foreign tenant's column value.
            let mut full = self.materialize(row, id);
            tenant.stamp(&mut full);
            ids.push(id);
            lines.push(influx::line(self.schema.table.as_str(), &full));
        }
        // InfluxDB overwrites silently, so duplicates are a pre-check: any
        // id already in the measurement — any tenant's — fails the whole
        // batch up front.
        let existing: std::collections::BTreeSet<i64> =
            self.fetch_all_ids().await?.into_iter().collect();
        if let Some(id) = ids.iter().find(|id| existing.contains(id)) {
            return Err(StorageError::Conflict(format!(
                "primary key {id} already exists"
            )));
        }
        self.write_lines(lines).await?;
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
        // Visibility probe before mutating (scoped path): out-of-scope and
        // cross-tenant rows are NotFound before anything is written.
        let mut updated = self
            .get_async(ctx, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
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
        // InfluxDB overwrites a series in place when the point repeats:
        // rewrite the full merged row at the same series+timestamp.
        self.write_lines(vec![influx::line(self.schema.table.as_str(), &updated)])
            .await?;
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
        // Scoped probe first — the engine's pk-direct read with the tenant
        // and scope predicates conjoined: the update face requires the row
        // to be visible to this viewer.
        match self.get_async(ctx, Value::Int(id)).await? {
            Some(current) => {
                // Update face: the series is the actor's own. Merge the
                // payload over it (absent payload fields keep their stored
                // values) and rewrite it in place at the same series+
                // timestamp. The payload rides the tenant stamp, so a
                // tenant actor cannot migrate the row into another tenant.
                let mut updated = current;
                for (field, value) in row.iter() {
                    if field != pk {
                        updated.insert(field, value.clone());
                    }
                }
                tenant.stamp(&mut updated);
                self.write_lines(vec![influx::line(self.schema.table.as_str(), &updated)])
                    .await?;
                self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
                Ok(updated)
            }
            None => {
                // Insert face: the row is absent **or** occupied by a row
                // this viewer cannot see — a tenant-scoped row with a
                // foreign tenant value reads exactly like a missing one.
                // The raw existence probe arbitrates: an occupied pk is a
                // Conflict without writing, never a silent overwrite of
                // the invisible incumbent.
                if self.fetch_all_ids().await?.contains(&id) {
                    return Err(StorageError::Conflict(format!(
                        "primary key {id} already exists"
                    )));
                }
                let mut stored = self.materialize(&row, id);
                tenant.stamp(&mut stored);
                self.write_lines(vec![influx::line(self.schema.table.as_str(), &stored)])
                    .await?;
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
        let statement = influx::delete_by_id(&self.schema.table, &self.schema.primary_key, id);
        let url = format!(
            "{}/query?db={}",
            self.base,
            rushwind_storage_influx_escape::query(&self.database)
        );
        let response = self
            .http
            .post(&url)
            .form(&[("q", statement)])
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("influxdb delete transport: {e}")))?;
        if response.status().as_u16() != 200 {
            return Err(StorageError::Backend(format!(
                "influxdb delete failed: {}",
                response.text().await.unwrap_or_default()
            )));
        }
        // The delete's read-visibility is eventually consistent: poll the
        // scoped read until the row is really gone.
        for _ in 0..20 {
            if self.get_async(ctx, Value::Int(id)).await?.is_none() {
                self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err(StorageError::Backend(
            "the deleted row never left the read path".into(),
        ))
    }

    /// `max(pk) + 1` over the visible rows — InfluxDB has no identity
    /// columns.
    async fn next_id(&self) -> Result<i64, StorageError> {
        let ctx = QueryCtx::all_access();
        let tenant = self.tenant_rule(&ctx)?;
        let rows = self.visible_rows(&tenant, &ctx, None).await?;
        let max = rows
            .iter()
            .filter_map(|row| row.get(&self.schema.primary_key).and_then(Value::as_i64))
            .max()
            .unwrap_or_default();
        Ok(max + 1)
    }
}

impl Repository for InfluxRepo {
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
