//! ClickHouse engine for the RushWind storage contract.
//!
//! ClickHouse speaks SQL over plain HTTP (`FORMAT JSONEachRow`); this
//! adapter is a thin reqwest client over [`sql`]'s statement generator.
//! Two engine truths shape the semantics, both documented because they
//! diverge from the relational norm:
//!
//! - **Mutations are normally asynchronous** — UPDATE/DELETE are ALTER
//!   TABLE statements. Every mutation carries `SETTINGS mutations_sync = 1`
//!   so reads-after-writes hold and the conformance suite's expectations
//!   are met verbatim.
//! - **There is no unique constraint.** The primary key orders the
//!   MergeTree; it does not deduplicate. Duplicate ids are rejected by a
//!   probe (check-then-act, best-effort under concurrency — the suite's
//!   single-writer model is what the guarantee covers). Upsert rides the
//!   same mechanism on both faces: probe-delete-insert when the scoped
//!   probe sees the row, and the same existence probe `create` uses on the
//!   insert face — an id occupied by a row the viewer cannot see conflicts
//!   instead of minting a second row with that primary key.
//!
//! [`sql`]: crate::sql

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod sql;

use serde_json::Value as Json;

use rushwind_storage::{
    AuditAction, AuditEntry, ColumnKind, Condition, FieldMask, FilterExpr, FilterNode, ListQuery,
    Op, Page, Paging, QueryCtx, Record, RepoFuture, Repository, Schema, Scope, StorageError,
    TenancyDecision, Value, Viewer,
};

/// A [`Repository`] backed by a ClickHouse server over HTTP.
pub struct ClickHouseRepo {
    schema: Schema,
    endpoint: String,
    http: reqwest::Client,
}

/// The tenant-axis rule for one operation: the schema's tenant column
/// resolved against the viewer's tenancy — the same shape the in-memory
/// reference engine carries. Resolved once per path, *before* the store is
/// touched, so an unidentified viewer on a tenant-scoped table fails
/// closed ([`StorageError::MissingViewer`]) instead of silently losing the
/// tenant predicate.
#[derive(Clone, Debug)]
struct TenantRule {
    column: String,
    decision: TenancyDecision,
}

impl TenantRule {
    /// The predicate as a WHERE fragment: `Enforce(id)` pins the tenant
    /// column to exactly `id` — a tenant-scoped row with no tenant value
    /// is nobody's. `Pass`/`Unscoped` need no fragment.
    fn predicate(&self) -> Option<String> {
        match self.decision {
            TenancyDecision::Unscoped | TenancyDecision::Pass => None,
            TenancyDecision::Enforce(id) => {
                Some(format!("{} = {}", sql::identifier(&self.column), id))
            }
        }
    }

    /// The predicate as a contract condition — the filter-node shape the
    /// mutation statements conjoin.
    fn condition(&self) -> Option<Condition> {
        match self.decision {
            TenancyDecision::Unscoped | TenancyDecision::Pass => None,
            TenancyDecision::Enforce(id) => Some(Condition::new(
                self.column.as_str(),
                Op::Eq,
                [Value::Int(id)],
            )),
        }
    }

    /// Force-stamps the tenant column when the viewer is a tenant business
    /// viewer — a tenant actor cannot address another tenant by writing
    /// the column itself. Platform and unscoped tables keep the payload's
    /// own value.
    fn stamp(&self, row: &mut Record) {
        if let TenancyDecision::Enforce(id) = self.decision {
            row.insert(&self.column, Value::Int(id));
        }
    }
}

impl ClickHouseRepo {
    /// Binds a repository to a ClickHouse HTTP endpoint (`http://host:8123`,
    /// optionally with credentials in the URL) and a schema.
    pub fn new(endpoint: impl Into<String>, schema: Schema) -> Self {
        let mut base = endpoint.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            schema,
            endpoint: base,
            http: reqwest::Client::new(),
        }
    }

    // ---- shared plumbing ---------------------------------------------------

    /// Executes a statement, returning the JSONEachRow rows parsed as JSON
    /// objects (empty for mutations).
    async fn execute(&self, statement: &str) -> Result<Vec<Json>, StorageError> {
        // Only queries produce rows; appending a FORMAT clause to
        // INSERT/ALTER would be parsed as part of the statement itself.
        let is_query = statement
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("SELECT");
        let query = if is_query {
            format!("{statement} FORMAT JSONEachRow")
        } else {
            statement.to_owned()
        };
        let response = self
            .http
            .post(&self.endpoint)
            .body(query)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("clickhouse transport: {e}")))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|e| StorageError::Backend(format!("clickhouse read: {e}")))?;
        if status != 200 {
            return Err(StorageError::Backend(format!(
                "clickhouse error: {}",
                body.lines().next().unwrap_or(&body)
            )));
        }
        let mut rows = Vec::new();
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            rows.push(
                serde_json::from_str(line)
                    .map_err(|e| StorageError::Backend(format!("clickhouse row: {e}")))?,
            );
        }
        Ok(rows)
    }

    async fn count_rows(&self, statement: &str) -> Result<u64, StorageError> {
        let rows = self.execute(statement).await?;
        // JSONEachRow renders UInt64 as a string; accept both shapes.
        rows.first()
            .and_then(|row| row.get("count()"))
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            })
            .ok_or_else(|| StorageError::Backend("count() produced no value".into()))
    }

    fn document_to_record(&self, document: &Json) -> Result<Record, StorageError> {
        let map = document
            .as_object()
            .ok_or_else(|| StorageError::Backend("expected a JSON object row".into()))?;
        let mut record = Record::new();
        for column in &self.schema.columns {
            let value = match map.get(&column.name) {
                None | Some(Json::Null) => Value::Null,
                Some(Json::Bool(b)) => Value::Bool(*b),
                Some(Json::Number(n)) => match column.kind {
                    ColumnKind::Int => Value::Int(
                        n.as_i64()
                            .ok_or_else(|| StorageError::Backend("int64 out of range".into()))?,
                    ),
                    _ => Value::Real(n.as_f64().unwrap_or_default()),
                },
                Some(Json::String(text)) => match column.kind {
                    ColumnKind::Text => Value::Text(text.clone()),
                    // JSONEachRow renders integers as strings; parse by the
                    // schema's declared kind.
                    ColumnKind::Int => Value::Int(text.parse().map_err(|_| {
                        StorageError::Backend(format!("column {:?} is not an integer", column.name))
                    })?),
                    ColumnKind::Real => Value::Real(text.parse().map_err(|_| {
                        StorageError::Backend(format!("column {:?} is not a float", column.name))
                    })?),
                    ColumnKind::Bool => Value::Bool(text != "0"),
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

    fn select_columns(&self, mask: &Option<FieldMask>) -> Vec<String> {
        match mask {
            None => self.schema.columns.iter().map(|c| c.name.clone()).collect(),
            Some(mask) => {
                let pk = self.schema.primary_key.clone();
                let mut cols: Vec<String> = mask
                    .paths()
                    .filter(|p| *p != pk && self.schema.column(p).is_some())
                    .map(str::to_owned)
                    .collect();
                cols.push(pk);
                cols
            }
        }
    }

    fn project(&self, mask: &Option<FieldMask>, row: Record) -> Record {
        let mut projected = row;
        if let Some(mask) = mask {
            projected.retain(|field| mask.allows(field));
        }
        projected
    }

    /// The tenant-axis rule for this table — the schema's tenant column
    /// resolved against the viewer, once per operation. Fails closed
    /// ([`StorageError::MissingViewer`]) when a tenant-scoped table is
    /// queried by a viewer without tenancy.
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

    /// The WHERE fragment for a read: viewer scope, the tenant predicate
    /// and the query filter conjoined. `None` means denied (empty
    /// everything).
    fn read_where(
        &self,
        ctx: &QueryCtx,
        tenant: &TenantRule,
        query: &ListQuery,
    ) -> Result<Option<String>, StorageError> {
        let mut denied = false;
        let mut parts: Vec<String> = Vec::new();
        match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
            Scope::Deny => denied = true,
            Scope::Unrestricted => {}
            Scope::Scoped(filter) => parts.push(sql::node_sql(filter.node())?),
        }
        // The tenant predicate rides beside the scope predicate: a foreign
        // tenant's row is as invisible as a foreign owner's.
        if let Some(predicate) = tenant.predicate() {
            parts.push(predicate);
        }
        if let Some(filter) = &query.filter {
            parts.push(sql::node_sql(filter.node())?);
        }
        if denied {
            return Ok(None);
        }
        Ok(if parts.is_empty() {
            Some("1".into())
        } else {
            Some(parts.join(" AND "))
        })
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

    /// Duplicate ids cannot be a storage-level constraint in ClickHouse;
    /// the conflict check is a probe. Best-effort by nature.
    async fn ensure_id_free(&self, id: i64) -> Result<(), StorageError> {
        let statement = format!(
            "SELECT count() FROM `{}` WHERE `{}` = {}",
            self.schema.table, self.schema.primary_key, id
        );
        if self.count_rows(&statement).await? > 0 {
            return Err(StorageError::Conflict(format!(
                "primary key {id} already exists"
            )));
        }
        Ok(())
    }

    /// `max(pk) + 1` — ClickHouse has no identity columns.
    async fn next_id(&self) -> Result<i64, StorageError> {
        let statement = format!(
            "SELECT max(`{}`) FROM `{}`",
            self.schema.primary_key, self.schema.table
        );
        let rows = self.execute(&statement).await?;
        let max = rows
            .first()
            .and_then(|row| {
                row.get(format!("max(`{}`)", self.schema.primary_key).as_str())
                    .or_else(|| row.as_object().and_then(|map| map.values().next()))
            })
            .and_then(|value| {
                value
                    .as_u64()
                    .map(|v| v as i64)
                    .or_else(|| value.as_i64())
                    .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or_default();
        Ok(max + 1)
    }

    /// Creates the table if it does not exist — the DDL counterpart of the
    /// [`Schema`] (MergeTree ordered by the primary key).
    pub async fn migrate_create(&self) -> Result<(), StorageError> {
        let create = sql::create_table_sql(&self.schema);
        self.execute(&create).await?;
        Ok(())
    }

    /// Drops the table — the inverse of [`ClickHouseRepo::migrate_create`],
    /// used by live-suite setups to guarantee a clean slate.
    pub async fn migrate_drop(&self) -> Result<(), StorageError> {
        let statement = format!("DROP TABLE IF EXISTS `{}`", self.schema.table);
        self.execute(&statement).await?;
        Ok(())
    }

    // ---- async implementations ----------------------------------------------

    /// The WHERE fragment for a point read: the primary key equality with
    /// the viewer scope and tenant predicate conjoined. `None` when the
    /// viewer is denied outright (empty result, no statement issued).
    fn get_where(
        &self,
        ctx: &QueryCtx,
        tenant: &TenantRule,
        id: i64,
    ) -> Result<Option<String>, StorageError> {
        let mut parts = vec![format!(
            "{} = {}",
            sql::identifier(&self.schema.primary_key),
            id
        )];
        match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
            Scope::Deny => return Ok(None),
            Scope::Unrestricted => {}
            Scope::Scoped(filter) => parts.push(sql::node_sql(filter.node())?),
        }
        if let Some(predicate) = tenant.predicate() {
            parts.push(predicate);
        }
        Ok(Some(parts.join(" AND ")))
    }

    /// The point-read statement: every declared column, the conjoined
    /// WHERE, and `LIMIT 1` behind an explicit primary-key ordering — the
    /// table is a MergeTree ordered by the pk, but the statement names its
    /// row deterministically instead of relying on storage order.
    fn get_statement(&self, where_clause: &str) -> String {
        format!(
            "SELECT * FROM `{}` WHERE {} ORDER BY {} ASC LIMIT 1",
            self.schema.table,
            where_clause,
            sql::identifier(&self.schema.primary_key)
        )
    }

    async fn get_async(
        &self,
        ctx: &QueryCtx,
        tenant: &TenantRule,
        id: Value,
    ) -> Result<Option<Record>, StorageError> {
        let id = self.expect_id(&id)?;
        let where_clause = match self.get_where(ctx, tenant, id)? {
            Some(where_clause) => where_clause,
            None => return Ok(None),
        };
        let rows = self.execute(&self.get_statement(&where_clause)).await?;
        match rows.first() {
            Some(row) => Ok(Some(self.document_to_record(row)?)),
            None => Ok(None),
        }
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        let tenant = self.tenant_rule(ctx)?;
        let where_clause = match self.read_where(ctx, &tenant, query)? {
            Some(where_clause) => where_clause,
            None => {
                return Ok(Page {
                    items: Vec::new(),
                    total: 0,
                    next_token: None,
                })
            }
        };
        let columns = self.select_columns(&query.mask);
        let statement = sql::select_sql(&self.schema, &columns, Some(&where_clause), query)?;
        let rows = self.execute(&statement).await?;
        let mut items = rows
            .iter()
            .map(|row| self.document_to_record(row))
            .collect::<Result<Vec<_>, StorageError>>()?;

        let mut next_token = None;
        if let Paging::Token { limit, .. } = &query.paging {
            if items.len() > usize::try_from(*limit).unwrap_or(usize::MAX) {
                items.pop();
                let last_id = items
                    .last()
                    .and_then(|row| row.get(&self.schema.primary_key))
                    .and_then(Value::as_i64)
                    .ok_or_else(|| {
                        StorageError::Backend("cursor page lost its primary key".into())
                    })?;
                next_token = Some(rushwind_storage::encode_cursor(last_id));
            }
        }

        let total = self
            .count_rows(&sql::count_sql(&self.schema, Some(&where_clause))?)
            .await?;
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

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let wrapped = ListQuery {
            filter,
            ..ListQuery::default()
        };
        let tenant = self.tenant_rule(ctx)?;
        let where_clause = match self.read_where(ctx, &tenant, &wrapped)? {
            Some(where_clause) => where_clause,
            None => return Ok(0),
        };
        self.count_rows(&sql::count_sql(&self.schema, Some(&where_clause))?)
            .await
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let tenant = self.tenant_rule(ctx)?;
        let mut row = row;
        tenant.stamp(&mut row);
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => {
                self.ensure_id_free(*existing).await?;
                *existing
            }
            _ => self.next_id().await?,
        };
        let stored = self.materialize(&row, id);
        self.execute(&sql::insert_sql(&self.schema, &stored)?)
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
        let tenant = self.tenant_rule(ctx)?;
        let pk = self.schema.primary_key.clone();
        let mut next_id = self.next_id().await?;
        let mut claimed = std::collections::BTreeSet::new();
        let mut staged: Vec<(i64, Record)> = Vec::with_capacity(rows.len());
        for row in &rows {
            // The stamp applies per row, before the row is staged — a
            // forged tenant column in a batch payload loses, same as the
            // single-item create.
            let mut stamped = row.clone();
            tenant.stamp(&mut stamped);
            let id = match stamped.get(pk.as_str()) {
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
            self.ensure_id_free(id).await?;
            staged.push((id, self.materialize(&stamped, id)));
        }
        let mut stored = Vec::with_capacity(staged.len());
        for (_, document) in &staged {
            self.execute(&sql::insert_sql(&self.schema, document)?)
                .await?;
        }
        stored.extend(staged.into_iter().map(|(_, document)| document));
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
        // Visibility probe first: out-of-scope (or foreign-tenant) rows
        // are NotFound before the mutation is issued.
        self.get_async(ctx, &tenant, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        // The stamp rides the patch, so the mutated row keeps the
        // probe-confirmed tenant: a tenant actor cannot migrate the row by
        // patching the tenant column itself.
        let mut patch = patch;
        tenant.stamp(&mut patch);
        let sets: Vec<(String, Value)> = patch
            .iter()
            .filter(|(field, _)| *field != pk)
            .map(|(field, value)| (field.to_owned(), value.clone()))
            .collect();
        if !sets.is_empty() {
            let guard = self.mutation_guard(ctx, &tenant)?;
            let statement = sql::update_sql(&self.schema, id, &sets, guard.as_ref())?;
            self.execute(&statement).await?;
        }
        let updated = self
            .get_async(ctx, &tenant, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        self.audit(ctx, AuditAction::Update, Some(Value::Int(id)));
        Ok(updated)
    }

    /// The mutation guard as one filter node: the viewer scope and the
    /// tenant predicate conjoined, so the ALTER's WHERE carries both
    /// boundaries even though the probe already gated the path —
    /// check-then-act stays inside the viewer's reach if the two steps
    /// ever race. `None` when the viewer is unrestricted and unscoped;
    /// a denied viewer errors (the probe turned it into NotFound first).
    fn mutation_guard(
        &self,
        ctx: &QueryCtx,
        tenant: &TenantRule,
    ) -> Result<Option<FilterNode>, StorageError> {
        let mut parts: Vec<FilterNode> = Vec::new();
        match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
            Scope::Deny => return Err(StorageError::NotFound),
            Scope::Unrestricted => {}
            Scope::Scoped(filter) => parts.push(filter.node().clone()),
        }
        if let Some(condition) = tenant.condition() {
            parts.push(FilterNode::Cond(condition));
        }
        Ok(match parts.as_slice() {
            [] => None,
            [one] => Some(one.clone()),
            _ => Some(FilterNode::All(parts)),
        })
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
        // The stamp applies before anything is written: the update face
        // re-pins the probe-confirmed tenant, the insert face lands with
        // the viewer's own.
        let mut row = row;
        tenant.stamp(&mut row);
        // Insert-or-update as probe-delete-insert through synced mutations.
        // The scoped probe decides the face: a row visible through the
        // scope-and-tenant boundary is replaced; an invisible (or absent)
        // id is an INSERT — and inserting must run the same existence probe
        // `create` uses, so an id occupied by a row the viewer cannot see
        // conflicts instead of minting a second row with that primary key.
        let visible = self
            .get_async(ctx, &tenant, Value::Int(id))
            .await?
            .is_some();
        if visible {
            let statement = sql::delete_sql(&self.schema, id, None)?;
            self.execute(&statement).await?;
        } else {
            self.ensure_id_free(id).await?;
        }
        let stored = self.materialize(&row, id);
        self.execute(&sql::insert_sql(&self.schema, &stored)?)
            .await?;
        self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
        Ok(stored)
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let tenant = self.tenant_rule(ctx)?;
        let id = self.expect_id(&id)?;
        // Scoped probe first: out-of-scope (or foreign-tenant) rows are
        // NotFound.
        self.get_async(ctx, &tenant, Value::Int(id))
            .await?
            .ok_or(StorageError::NotFound)?;
        let guard = self.mutation_guard(ctx, &tenant)?;
        let statement = sql::delete_sql(&self.schema, id, guard.as_ref())?;
        self.execute(&statement).await?;
        self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

impl Repository for ClickHouseRepo {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        Box::pin(async move {
            let tenant = self.tenant_rule(&ctx)?;
            self.get_async(&ctx, &tenant, id).await
        })
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

#[cfg(test)]
mod tests {
    //! Offline coverage of the tenancy translation — the same pure-layer
    //! unit testing the SQL generator gets: rule resolution (fail-closed),
    //! the WHERE fragments the tenant predicate joins, the mutation guard,
    //! and the write-side stamp. The wire paths themselves are pinned by
    //! the live suite.

    use super::*;
    use rushwind_storage::ColumnKind as CK;

    /// A tenant-scoped schema: the suite table plus the conventional
    /// tenant column, declared as the schema's tenant column.
    fn tenant_schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", CK::Text)
            .column("age", CK::Int)
            .column("owner_id", CK::Int)
            .column(Viewer::TENANT_COLUMN, CK::Int)
            .tenant_column(Viewer::TENANT_COLUMN)
            .build()
            .expect("tenant schema is valid")
    }

    /// The same table without a tenant column — unscoped.
    fn plain_schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", CK::Text)
            .column("owner_id", CK::Int)
            .build()
            .expect("plain schema is valid")
    }

    #[test]
    fn unidentified_viewer_fails_closed_before_the_store_is_touched() {
        let repo = ClickHouseRepo::new("http://localhost:8123", tenant_schema());
        // Viewer::own carries a data range but no tenancy — exactly the
        // shape that once dropped the tenant predicate silently.
        let ctx = QueryCtx::new(Viewer::own(1));
        let err = repo
            .tenant_rule(&ctx)
            .expect_err("an unidentified viewer must fail closed");
        assert_eq!(err, StorageError::MissingViewer);
    }

    #[test]
    fn unscoped_tables_skip_the_tenant_axis() {
        let repo = ClickHouseRepo::new("http://localhost:8123", plain_schema());
        let ctx = QueryCtx::new(Viewer::own(1));
        let rule = repo.tenant_rule(&ctx).expect("unscoped tables never fail");
        assert!(
            rule.predicate().is_none(),
            "no tenant column, no tenant predicate"
        );
    }

    #[test]
    fn point_read_where_conjoins_scope_and_tenant() {
        let repo = ClickHouseRepo::new("http://localhost:8123", tenant_schema());
        // A tenant user inside owner 1: both boundaries in one WHERE.
        let ctx = QueryCtx::new(Viewer::tenant_user(5, 1));
        let rule = repo.tenant_rule(&ctx).expect("resolves");
        let where_clause = repo
            .get_where(&ctx, &rule, 7)
            .expect("builds")
            .expect("not denied");
        assert_eq!(
            where_clause,
            "`id` = 7 AND `owner_id` = 1 AND `tenant_id` = 5"
        );
    }

    #[test]
    fn read_where_carries_the_tenant_predicate_beside_scope_and_filter() {
        let repo = ClickHouseRepo::new("http://localhost:8123", tenant_schema());
        let ctx = QueryCtx::new(Viewer::tenant(5));
        let rule = repo.tenant_rule(&ctx).expect("resolves");
        let query = ListQuery {
            filter: Some(FilterExpr::cond("age", Op::Gt, [Value::Int(10)])),
            ..ListQuery::default()
        };
        let where_clause = repo
            .read_where(&ctx, &rule, &query)
            .expect("builds")
            .expect("not denied");
        assert_eq!(where_clause, "`tenant_id` = 5 AND `age` > 10");

        // Platform passes without a predicate.
        let platform = QueryCtx::all_access();
        let rule = repo.tenant_rule(&platform).expect("platform passes");
        let where_clause = repo
            .read_where(&platform, &rule, &query)
            .expect("builds")
            .expect("not denied");
        assert_eq!(where_clause, "`age` > 10");
    }

    #[test]
    fn mutation_guard_conjoins_scope_and_tenant() {
        let repo = ClickHouseRepo::new("http://localhost:8123", tenant_schema());
        let ctx = QueryCtx::new(Viewer::tenant_user(5, 1));
        let rule = repo.tenant_rule(&ctx).expect("resolves");
        let guard = repo
            .mutation_guard(&ctx, &rule)
            .expect("builds")
            .expect("scoped viewer gets a guard");
        assert_eq!(
            sql::node_sql(&guard).expect("translates"),
            "(`owner_id` = 1 AND `tenant_id` = 5)"
        );

        // A platform viewer on the same table needs no guard.
        let platform = QueryCtx::all_access();
        let rule = repo.tenant_rule(&platform).expect("platform passes");
        assert!(repo
            .mutation_guard(&platform, &rule)
            .expect("builds")
            .is_none());
    }

    #[test]
    fn stamp_wins_over_a_forged_payload_and_pass_keeps_it() {
        // Enforce: the tenant actor cannot address another tenant by
        // writing the column itself.
        let enforce = TenantRule {
            column: Viewer::TENANT_COLUMN.to_owned(),
            decision: TenancyDecision::Enforce(5),
        };
        let mut forged = Record::new().set(Viewer::TENANT_COLUMN, 6i64);
        enforce.stamp(&mut forged);
        assert_eq!(forged.get(Viewer::TENANT_COLUMN), Some(&Value::Int(5)));

        // Pass (platform): the payload's own tenant value survives.
        let pass = TenantRule {
            column: Viewer::TENANT_COLUMN.to_owned(),
            decision: TenancyDecision::Pass,
        };
        let mut addressed = Record::new().set(Viewer::TENANT_COLUMN, 6i64);
        pass.stamp(&mut addressed);
        assert_eq!(addressed.get(Viewer::TENANT_COLUMN), Some(&Value::Int(6)));
    }

    #[test]
    fn get_statement_orders_by_the_pk_before_limit_one() {
        let repo = ClickHouseRepo::new("http://localhost:8123", tenant_schema());
        // The point read picks one deterministic row: the pk ordering rides
        // ahead of `LIMIT 1` instead of relying on MergeTree storage order.
        assert_eq!(
            repo.get_statement("`id` = 7 AND `tenant_id` = 5"),
            "SELECT * FROM `widgets` WHERE `id` = 7 AND `tenant_id` = 5 \
             ORDER BY `id` ASC LIMIT 1"
        );
    }
}
