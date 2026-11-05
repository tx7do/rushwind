//! The SeaORM-backed engine for the [`Repository`] contract.
//!
//! GORM popularized the dynamic query builder; this crate applies the
//! same idea: [`sea_orm::DatabaseConnection`]
//! for pooling, drivers and transactions, [`sea_orm::sea_query`] for dynamic
//! statement construction. Nothing here needs generated entities — the
//! [`Schema`] is the single source of truth, exactly like the contract wants.
//!
//! Filter trees translate to `sea_query::Condition`s (nested `All`/`Any`
//! groups become nested `AND`/`OR`); the three paging strategies translate
//! to `LIMIT/OFFSET` or an id-greater-than cursor; viewer scope and tenant
//! predicates conjoin an extra predicate or short-circuit to empty. Batch
//! writes run inside a real transaction. All three SQL backends ship
//! enabled — SQLite (the conformance-tested, embedded flavor), PostgreSQL
//! and MySQL (`SeaRepo::connect`); the per-dialect statement rendering is
//! pinned by snapshot tests, and the live suites run against service
//! containers in CI.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use sea_orm::sea_query::{
    Alias, ColumnDef, Condition, DeleteStatement, Expr, ExprTrait, Func, InsertStatement,
    MysqlQueryBuilder, Order, PostgresQueryBuilder, Query, SelectStatement, SqliteQueryBuilder,
    Table, TableCreateStatement, UpdateStatement, Values,
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, DbErr, RuntimeErr,
    Statement, TransactionTrait,
};

use rushwind_storage::{
    AuditAction, AuditEntry, ColumnKind, FieldMask, FilterExpr, FilterNode, ListQuery, Op, Page,
    Paging, QueryCtx, Record, RepoFuture, Repository, Schema, Scope, SortDir, StorageError,
    TenancyDecision, Value, Viewer,
};

/// A [`Repository`] backed by a SeaORM connection pool.
pub struct SeaRepo {
    schema: Schema,
    db: DatabaseConnection,
}

/// Renders a query statement for a concrete backend.
trait Buildable {
    /// The SQL text and bind values for `backend`.
    fn build_for(&self, backend: DbBackend) -> (String, Values);
}

macro_rules! buildable {
    ($($ty:ty),+ $(,)?) => {
        $(impl Buildable for $ty {
            fn build_for(&self, backend: DbBackend) -> (String, Values) {
                match backend {
                    DbBackend::MySql => self.build(MysqlQueryBuilder),
                    DbBackend::Postgres => self.build(PostgresQueryBuilder),
                    _ => self.build(SqliteQueryBuilder),
                }
            }
        })+
    };
}

buildable!(
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    DeleteStatement
);

// DDL statements build to plain SQL, without bind values.
impl Buildable for sea_orm::sea_query::TableDropStatement {
    fn build_for(&self, backend: DbBackend) -> (String, Values) {
        let sql = match backend {
            DbBackend::MySql => self.build(MysqlQueryBuilder),
            DbBackend::Postgres => self.build(PostgresQueryBuilder),
            _ => self.build(SqliteQueryBuilder),
        };
        (sql, Values(Vec::new()))
    }
}

impl Buildable for TableCreateStatement {
    fn build_for(&self, backend: DbBackend) -> (String, Values) {
        let sql = match backend {
            DbBackend::MySql => self.build(MysqlQueryBuilder),
            DbBackend::Postgres => self.build(PostgresQueryBuilder),
            _ => self.build(SqliteQueryBuilder),
        };
        (sql, Values(Vec::new()))
    }
}

impl SeaRepo {
    /// Binds a repository to a live connection and a schema.
    pub fn new(db: DatabaseConnection, schema: Schema) -> Self {
        Self { schema, db }
    }

    /// Opens a SQLite in-memory database suited for this engine — a
    /// single-connection pool so `:memory:` is one database, not one per
    /// pooled connection.
    pub async fn sqlite_memory(schema: Schema) -> Result<Self, StorageError> {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let db = Database::connect(options)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(Self::new(db, schema))
    }

    /// Connects to any backend SeaORM speaks — `postgres://…`, `mysql://…`,
    /// `sqlite://…` — and binds a repository to it. This is the production
    /// entry point; [`SeaRepo::sqlite_memory`] is the embedded/test flavor.
    pub async fn connect(url: impl Into<String>, schema: Schema) -> Result<Self, StorageError> {
        let db = Database::connect(ConnectOptions::new(url.into()))
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(Self::new(db, schema))
    }

    /// Drops the table if it exists — the inverse of [`SeaRepo::migrate_create`],
    /// used by live-suite setups to guarantee a clean slate.
    pub async fn migrate_drop(&self) -> Result<(), StorageError> {
        let drop = sea_orm::sea_query::Table::drop()
            .table(Alias::new(&self.schema.table))
            .if_exists()
            .to_owned();
        self.db
            .execute_raw(self.statement(&drop))
            .await
            .map_err(|e| self.map_err(e))?;
        Ok(())
    }

    /// Creates the table if it does not exist yet — the DDL counterpart of
    /// the [`Schema`], for embedded and test setups. Production deployments
    /// usually own their migrations instead.
    pub async fn migrate_create(&self) -> Result<(), StorageError> {
        let create = create_table_statement(&self.schema, self.backend());
        self.db
            .execute_raw(self.statement(&create))
            .await
            .map_err(|e| self.map_err(e))?;
        Ok(())
    }

    // ---- shared plumbing ---------------------------------------------------

    fn backend(&self) -> DbBackend {
        self.db.get_database_backend()
    }

    fn statement<S: Buildable>(&self, stmt: &S) -> Statement {
        let (sql, values) = stmt.build_for(self.backend());
        Statement::from_sql_and_values(self.backend(), sql, values.0)
    }

    /// Maps a driver error onto the contract: a unique or primary-key
    /// violation is [`StorageError::Conflict`] — the upsert insert face
    /// relies on the table's uniqueness to turn an occupied primary key
    /// into a conflict instead of a silent overwrite. sqlx classifies the
    /// error structurally (the violation codes of SQLite, PostgreSQL and
    /// MySQL alike); the text fallback below catches the same condition in
    /// shapes that do not ride the sqlx wrapper (transaction and session
    /// errors). Everything else is [`StorageError::Backend`] — engine-level
    /// errors such as [`StorageError::MissingViewer`] never enter this
    /// mapping, so they reach the caller untouched.
    fn map_err(&self, err: DbErr) -> StorageError {
        if let DbErr::Exec(RuntimeErr::SqlxError(e)) | DbErr::Query(RuntimeErr::SqlxError(e)) = &err
        {
            if e.as_database_error()
                .is_some_and(|db| db.is_unique_violation())
            {
                return StorageError::Conflict(err.to_string());
            }
        }
        let text = err.to_string();
        if text.contains("UNIQUE constraint")
            || text.contains("Duplicate entry")
            || text.contains("duplicate key")
        {
            StorageError::Conflict(text)
        } else {
            StorageError::Backend(text)
        }
    }

    /// The viewer scope as a SQL predicate, or denial of the whole query.
    fn scope_sql(&self, ctx: &QueryCtx) -> Result<ScopeSql, StorageError> {
        Ok(
            match ctx.viewer.scope(Viewer::OWNER_COLUMN, Viewer::UNIT_COLUMN) {
                Scope::Deny => ScopeSql::Deny,
                Scope::Unrestricted => ScopeSql::Pass(None),
                Scope::Scoped(filter) => ScopeSql::Pass(Some(translate(filter.node())?)),
            },
        )
    }

    fn audit(&self, ctx: &QueryCtx, action: AuditAction, target: Option<Value>) {
        ctx.audit(AuditEntry::now(
            action,
            self.schema.table.clone(),
            target,
            ctx.viewer.actor_id,
        ));
    }

    fn row_to_record(
        &self,
        row: &sea_orm::QueryResult,
        columns: &[String],
    ) -> Result<Record, StorageError> {
        let mut record = Record::new();
        for name in columns {
            let kind = self
                .schema
                .column(name)
                .ok_or_else(|| StorageError::InvalidQuery(format!("unknown column {name:?}")))?
                .kind;
            let value = match kind {
                ColumnKind::Bool => Value::from(
                    row.try_get_by::<Option<bool>, _>(name.as_str())
                        .map_err(|e| self.map_err(e))?,
                ),
                ColumnKind::Int => Value::from(
                    row.try_get_by::<Option<i64>, _>(name.as_str())
                        .map_err(|e| self.map_err(e))?,
                ),
                ColumnKind::Real => Value::from(
                    row.try_get_by::<Option<f64>, _>(name.as_str())
                        .map_err(|e| self.map_err(e))?,
                ),
                ColumnKind::Text => Value::from(
                    row.try_get_by::<Option<String>, _>(name.as_str())
                        .map_err(|e| self.map_err(e))?,
                ),
            };
            record.insert(name.as_str(), value);
        }
        Ok(record)
    }

    /// The columns a list query returns: everything, or the masked fields
    /// plus the primary key (cursors need it even when masked out).
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

    /// The WHERE clause shared by list and count: scope and the tenant
    /// predicate conjoined with the query filter.
    fn where_sql(
        &self,
        scope: &ScopeSql,
        tenant: &TenantSql,
        query: &ListQuery,
    ) -> Result<Option<Condition>, StorageError> {
        let mut combined = Condition::all();
        let mut any = false;
        if let ScopeSql::Pass(Some(scope_cond)) = scope {
            combined = combined.add(scope_cond.clone());
            any = true;
        }
        if let Some(predicate) = tenant.predicate() {
            combined = combined.add(predicate);
            any = true;
        }
        if let Some(filter) = &query.filter {
            combined = combined.add(translate(filter.node())?);
            any = true;
        }
        Ok(if any { Some(combined) } else { None })
    }

    async fn count_matching(
        &self,
        table: &Alias,
        where_clause: &Option<Condition>,
    ) -> Result<u64, StorageError> {
        let mut count = Query::select();
        count.from(table.clone()).expr(Expr::cust("COUNT(*)"));
        if let Some(cond) = where_clause {
            count.cond_where(cond.clone());
        }
        let row = self
            .db
            .query_one_raw(self.statement(&count))
            .await
            .map_err(|e| self.map_err(e))?;
        match row {
            Some(row) => {
                let n = row
                    .try_get_by_index::<i64>(0)
                    .map_err(|e| self.map_err(e))?;
                Ok(Ord::max(n, 0) as u64)
            }
            None => Ok(0),
        }
    }

    /// Reads a row the engine itself just wrote — no scope re-check, because
    /// the write already happened inside the caller's context. Reserved for
    /// that read-back (`create`/`batch_create`): every other path must go
    /// through the scoped reads, whose WHERE clause carries the viewer's
    /// scope and tenant.
    async fn fetch_by_pk_unscoped(&self, id: i64) -> Result<Record, StorageError> {
        let pk = self.schema.primary_key.clone();
        let cols = self.select_columns(&None);
        let mut select = Query::select();
        select.from(Alias::new(&self.schema.table));
        for col in &cols {
            select.column(Alias::new(col));
        }
        select.cond_where(Condition::all().add(Expr::col(Alias::new(&pk)).eq(id)));
        select.limit(1);
        let row = self
            .db
            .query_one_raw(self.statement(&select))
            .await
            .map_err(|e| self.map_err(e))?;
        row.map(|row| self.row_to_record(&row, &cols))
            .unwrap_or_else(|| Err(StorageError::NotFound))
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

    /// The stored record a write returns: the payload materialized over the
    /// schema with the primary key filled — the row the engine itself just
    /// wrote, exactly what the INSERT put in the table. Used by the upsert
    /// insert face, where a re-read through the scope would only repeat the
    /// probe (and could legitimately see nothing, the same denial the probe
    /// saw) while an unscoped read-back would leak past the viewer.
    fn materialized_record(&self, row: &Record, id: i64) -> Record {
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

    // ---- async implementations ----------------------------------------------

    async fn get_async(&self, ctx: &QueryCtx, id: Value) -> Result<Option<Record>, StorageError> {
        let id = expect_id(&id)?;
        // The tenant rule resolves before the denial check: an unidentified
        // viewer on a tenant-scoped table fails closed even when its range
        // would have denied anyway — the error is the contract, not an
        // empty result.
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let scope = self.scope_sql(ctx)?;
        if matches!(scope, ScopeSql::Deny) {
            return Ok(None);
        }
        let pk = self.schema.primary_key.clone();
        let cols = self.select_columns(&None);
        let mut select = Query::select();
        select.from(Alias::new(&self.schema.table));
        for col in &cols {
            select.column(Alias::new(col));
        }
        let mut cond = Condition::all().add(Expr::col(Alias::new(&pk)).eq(id));
        if let ScopeSql::Pass(Some(scope_cond)) = &scope {
            cond = cond.add(scope_cond.clone());
        }
        if let Some(predicate) = tenant.predicate() {
            cond = cond.add(predicate);
        }
        select.cond_where(cond);
        select.limit(1);
        let row = self
            .db
            .query_one_raw(self.statement(&select))
            .await
            .map_err(|e| self.map_err(e))?;
        match row {
            Some(row) => Ok(Some(self.row_to_record(&row, &cols)?)),
            None => Ok(None),
        }
    }

    async fn list_async(
        &self,
        ctx: &QueryCtx,
        query: &ListQuery,
    ) -> Result<Page<Record>, StorageError> {
        query.validate(&self.schema)?;
        // Tenant first, like the reference engine: the fail-closed error
        // beats the denial short-circuit.
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let scope = self.scope_sql(ctx)?;
        if matches!(scope, ScopeSql::Deny) {
            return Ok(Page {
                items: Vec::new(),
                total: 0,
                next_token: None,
            });
        }
        let where_clause = self.where_sql(&scope, &tenant, query)?;
        let table = Alias::new(&self.schema.table);
        let pk = self.schema.primary_key.clone();

        let total = self.count_matching(&table, &where_clause).await?;

        let cols = self.select_columns(&query.mask);
        let select = list_select(
            &self.schema,
            &cols,
            where_clause.as_ref(),
            query,
            self.backend(),
        )?;

        let rows = self
            .db
            .query_all_raw(self.statement(&select))
            .await
            .map_err(|e| self.map_err(e))?;
        let mut items: Vec<Record> = rows
            .iter()
            .map(|row| self.row_to_record(row, &cols))
            .collect::<Result<Vec<_>, _>>()?;

        let mut next_token = None;
        if let Paging::Token { limit, .. } = &query.paging {
            if items.len() > usize::try_from(*limit).unwrap_or(usize::MAX) {
                items.pop();
                let last_id = items
                    .last()
                    .and_then(|row| row.get(&pk))
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

    async fn count_async(
        &self,
        ctx: &QueryCtx,
        filter: Option<FilterExpr>,
    ) -> Result<u64, StorageError> {
        if let Some(filter) = &filter {
            filter.validate(&self.schema)?;
        }
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let scope = self.scope_sql(ctx)?;
        if matches!(scope, ScopeSql::Deny) {
            return Ok(0);
        }
        let where_clause = self.where_sql(
            &scope,
            &tenant,
            &ListQuery {
                filter,
                ..ListQuery::default()
            },
        )?;
        self.count_matching(&Alias::new(&self.schema.table), &where_clause)
            .await
    }

    /// One INSERT for a validated row; returns the primary key it landed on.
    async fn insert_async<C: ConnectionTrait>(
        &self,
        conn: &C,
        row: &Record,
    ) -> Result<i64, StorageError> {
        let pk = self.schema.primary_key.clone();
        let explicit = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => Some(*existing),
            _ => None,
        };
        let mut columns: Vec<Alias> = Vec::new();
        let mut values: Vec<Expr> = Vec::new();
        for column in &self.schema.columns {
            if column.name == pk && explicit.is_none() {
                continue; // let the engine generate the rowid
            }
            let value = if column.name == pk {
                Value::Int(explicit.expect("pk present when explicit"))
            } else {
                row.get(&column.name).cloned().unwrap_or(Value::Null)
            };
            columns.push(Alias::new(&column.name));
            values.push(bind(&value));
        }
        let mut insert = Query::insert();
        insert
            .into_table(Alias::new(&self.schema.table))
            .columns(columns)
            .values(values)
            .map_err(|e| StorageError::InvalidQuery(e.to_string()))?;
        if let Some(explicit_id) = explicit {
            // The payload carries its own primary key; plain insert.
            conn.execute_raw(self.statement(&insert))
                .await
                .map_err(|e| self.map_err(e))?;
            return Ok(explicit_id);
        }
        let backend = self.backend();
        if backend == DbBackend::MySql {
            // MySQL has no RETURNING; the last generated id lives on the
            // connection's session state.
            let exec = conn
                .execute_raw(self.statement(&insert))
                .await
                .map_err(|e| self.map_err(e))?;
            Ok(i64::try_from(exec.last_insert_id())
                .map_err(|_| StorageError::Backend("generated primary key out of range".into()))?)
        } else {
            // PostgreSQL and SQLite (>= 3.35) render RETURNING.
            insert.returning_col(Alias::new(pk.as_str()));
            let row = conn
                .query_one_raw(self.statement(&insert))
                .await
                .map_err(|e| self.map_err(e))?;
            // PG identity columns decode strictly as INT4 (i32); SQLite
            // rowid RETURNING is INT8 (i64). Try both widths by name —
            // sqlx's decoders reject the wrong width outright, so a failed
            // narrow read falls back to the wide one and vice versa.
            let id = match row {
                Some(row) => {
                    let wide = row.try_get_by::<Option<i64>, _>(pk.as_str()).ok().flatten();
                    let narrow = row
                        .try_get_by::<Option<i32>, _>(pk.as_str())
                        .ok()
                        .flatten()
                        .map(i64::from);
                    narrow.or(wide).ok_or_else(|| {
                        StorageError::Backend("INSERT RETURNING produced no primary key".into())
                    })?
                }
                None => {
                    return Err(StorageError::Backend(
                        "INSERT RETURNING produced no primary key".into(),
                    ))
                }
            };
            Ok(id)
        }
    }

    async fn create_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        // The payload rides the stamp: a tenant actor cannot address another
        // tenant by writing the column itself.
        let mut row = row;
        tenant.stamp(&mut row);
        let id = self.insert_async(&self.db, &row).await?;
        let stored = self.fetch_by_pk_unscoped(id).await?;
        self.audit(ctx, AuditAction::Create, Some(Value::Int(id)));
        Ok(stored)
    }

    async fn batch_create_async(
        &self,
        ctx: &QueryCtx,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>, StorageError> {
        // The tenant rule resolves up front, like the reference engine, and
        // every row of the batch rides the stamp.
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let mut rows = rows;
        for row in &mut rows {
            self.validate_row(row)?;
            tenant.stamp(row);
        }
        let txn = self.db.begin().await.map_err(|e| self.map_err(e))?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in &rows {
            ids.push(self.insert_async(&txn, row).await?);
        }
        txn.commit().await.map_err(|e| self.map_err(e))?;
        let mut stored = Vec::with_capacity(ids.len());
        for id in ids {
            stored.push(self.fetch_by_pk_unscoped(id).await?);
        }
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
        let id = expect_id(&id)?;
        // The tenant rule resolves before the denial check — an unidentified
        // viewer fails closed even when its range would deny anyway — and the
        // patch rides the stamp: applied after the patch, so a tenant actor
        // cannot migrate the row into another tenant through it.
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let mut patch = patch;
        tenant.stamp(&mut patch);
        let scope = self.scope_sql(ctx)?;
        if matches!(scope, ScopeSql::Deny) {
            return Err(StorageError::NotFound);
        }
        let pk = self.schema.primary_key.clone();
        if let Some(new_pk) = patch.get(pk.as_str()) {
            if new_pk != &Value::Null && new_pk.as_i64() != Some(id) {
                return Err(StorageError::InvalidQuery(
                    "update patches cannot move the primary key".into(),
                ));
            }
        }
        let mut update = Query::update();
        update.table(Alias::new(&self.schema.table));
        let mut touched = 0;
        for column in &self.schema.columns {
            if let Some(value) = patch.get(&column.name) {
                let bound = if column.name == pk {
                    Value::Int(id)
                } else {
                    value.clone()
                };
                update.value(Alias::new(&column.name), bind(&bound));
                touched += 1;
            }
        }
        if touched > 0 {
            let mut cond = Condition::all().add(Expr::col(Alias::new(&pk)).eq(id));
            if let ScopeSql::Pass(Some(scope_cond)) = &scope {
                cond = cond.add(scope_cond.clone());
            }
            if let Some(predicate) = tenant.predicate() {
                cond = cond.add(predicate);
            }
            update.cond_where(cond);
            let exec = self
                .db
                .execute_raw(self.statement(&update))
                .await
                .map_err(|e| self.map_err(e))?;
            if exec.rows_affected() == 0 {
                return Err(StorageError::NotFound);
            }
        }
        match self.get_async(ctx, Value::Int(id)).await? {
            Some(row) => {
                self.audit(ctx, AuditAction::Update, Some(Value::Int(id)));
                Ok(row)
            }
            None => Err(StorageError::NotFound),
        }
    }

    async fn upsert_async(&self, ctx: &QueryCtx, row: Record) -> Result<Record, StorageError> {
        self.validate_row(&row)?;
        let pk = self.schema.primary_key.clone();
        let id = match row.get(pk.as_str()) {
            Some(Value::Int(existing)) if *existing > 0 => *existing,
            _ => {
                return Err(StorageError::InvalidQuery(
                    "upsert requires an explicit primary key".into(),
                ))
            }
        };
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        // Both faces ride the stamp: a tenant actor can neither create into
        // another tenant nor migrate a row into one through the payload.
        let mut row = row;
        tenant.stamp(&mut row);
        // The probe decides the face through the scoped read path (get
        // conjoins scope and tenant): only a row *visible in this viewer's
        // scope and tenant* authorizes the update face. Anything else is
        // indistinguishable from an absent row — and the insert face turns an
        // occupied primary key into a Conflict instead of a silent overwrite.
        // The previous ON CONFLICT (pk) DO UPDATE conjoined neither scope nor
        // tenant, so a tenant viewer could rewrite any row on the table by pk
        // and get it back unscoped.
        match self.get_async(ctx, Value::Int(id)).await? {
            Some(_) => {
                // Update face: merge the payload's own columns over the
                // visible row through a scoped UPDATE — the WHERE side
                // carries the primary key, the scope and the tenant. The
                // payload already rides the stamp, so the merged row cannot
                // land in another tenant.
                let mut update = Query::update();
                update.table(Alias::new(&self.schema.table));
                let mut touched = 0;
                for column in &self.schema.columns {
                    if column.name == pk {
                        continue; // the primary key addresses the row, never moves
                    }
                    if let Some(value) = row.get(&column.name) {
                        update.value(Alias::new(&column.name), bind(value));
                        touched += 1;
                    }
                }
                if touched > 0 {
                    let mut cond = Condition::all().add(Expr::col(Alias::new(&pk)).eq(id));
                    if let ScopeSql::Pass(Some(scope_cond)) = &self.scope_sql(ctx)? {
                        cond = cond.add(scope_cond.clone());
                    }
                    if let Some(predicate) = tenant.predicate() {
                        cond = cond.add(predicate);
                    }
                    update.cond_where(cond);
                    let exec = self
                        .db
                        .execute_raw(self.statement(&update))
                        .await
                        .map_err(|e| self.map_err(e))?;
                    if exec.rows_affected() == 0 {
                        // The row left the scope between probe and write.
                        return Err(StorageError::NotFound);
                    }
                }
                // The update face returns through a scoped read: the row was
                // just written inside this viewer's context, and the read
                // must obey the same boundaries the write did.
                let stored = self
                    .get_async(ctx, Value::Int(id))
                    .await?
                    .ok_or(StorageError::NotFound)?;
                self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
                Ok(stored)
            }
            None => {
                // Insert face: absent **or** invisible. A plain INSERT — no
                // ON CONFLICT — so an invisible occupant of the primary key
                // fails on the table's uniqueness and maps to
                // [`StorageError::Conflict`]: never a silent overwrite. The
                // return value is the stamped row just written, not an
                // unscoped read-back of a row this viewer may not see.
                self.insert_async(&self.db, &row).await?;
                self.audit(ctx, AuditAction::Upsert, Some(Value::Int(id)));
                Ok(self.materialized_record(&row, id))
            }
        }
    }

    async fn delete_async(&self, ctx: &QueryCtx, id: Value) -> Result<(), StorageError> {
        let id = expect_id(&id)?;
        let tenant = TenantSql::resolve(&self.schema, ctx)?;
        let scope = self.scope_sql(ctx)?;
        if matches!(scope, ScopeSql::Deny) {
            return Err(StorageError::NotFound);
        }
        let pk = self.schema.primary_key.clone();
        let mut delete = Query::delete();
        delete.from_table(Alias::new(&self.schema.table));
        let mut cond = Condition::all().add(Expr::col(Alias::new(&pk)).eq(id));
        if let ScopeSql::Pass(Some(scope_cond)) = &scope {
            cond = cond.add(scope_cond.clone());
        }
        if let Some(predicate) = tenant.predicate() {
            cond = cond.add(predicate);
        }
        delete.cond_where(cond);
        let exec = self
            .db
            .execute_raw(self.statement(&delete))
            .await
            .map_err(|e| self.map_err(e))?;
        if exec.rows_affected() == 0 {
            return Err(StorageError::NotFound);
        }
        self.audit(ctx, AuditAction::Delete, Some(Value::Int(id)));
        Ok(())
    }
}

impl Repository for SeaRepo {
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

/// The viewer scope rendered for SQL: denied (empty everything) or a
/// conjoinable predicate.
enum ScopeSql {
    /// The viewer may see nothing; engines short-circuit.
    Deny,
    /// The extra predicate to AND in, if any.
    Pass(Option<Condition>),
}

/// The tenant axis resolved for one operation: the schema's tenant column
/// plus the viewer's [`TenancyDecision`] against it.
///
/// Mirrors the in-memory reference engine's `TenantRule`: resolution is
/// fail-closed — a tenant-scoped table queried by an unidentified viewer
/// errors with [`StorageError::MissingViewer`] from *every* repository
/// path, before any statement is sent.
#[derive(Clone, Debug)]
enum TenantSql {
    /// No tenant predicate anywhere: the table declares no tenant column,
    /// or the viewer is platform/system.
    Pass,
    /// Conjoin `column = id` on every path and force-stamp writes with id.
    Enforce { column: String, id: i64 },
}

impl TenantSql {
    /// Resolves the schema's tenant column against the viewer.
    fn resolve(schema: &Schema, ctx: &QueryCtx) -> Result<Self, StorageError> {
        Ok(
            match ctx
                .viewer
                .tenant_decision(schema.tenant_column.as_deref())?
            {
                TenancyDecision::Unscoped | TenancyDecision::Pass => TenantSql::Pass,
                TenancyDecision::Enforce(id) => TenantSql::Enforce {
                    // Enforce only arises when the schema declares the
                    // column; the fallback keeps the conventional name so
                    // the stamp cannot silently miss.
                    column: schema
                        .tenant_column
                        .clone()
                        .unwrap_or_else(|| Viewer::TENANT_COLUMN.to_owned()),
                    id,
                },
            },
        )
    }

    /// The predicate to AND into every query's WHERE clause, if any.
    fn predicate(&self) -> Option<Condition> {
        match self {
            TenantSql::Pass => None,
            TenantSql::Enforce { column, id } => {
                Some(Condition::all().add(Expr::col(Alias::new(column.as_str())).eq(*id)))
            }
        }
    }

    /// Force-stamps the tenant column when the viewer is a tenant business
    /// viewer — a tenant actor cannot address another tenant by writing the
    /// column itself. Platform and unscoped writes keep the payload value.
    fn stamp(&self, row: &mut Record) {
        if let TenantSql::Enforce { column, id } = self {
            row.insert(column.as_str(), Value::Int(*id));
        }
    }
}

fn expect_id(id: &Value) -> Result<i64, StorageError> {
    id.as_i64()
        .ok_or_else(|| StorageError::InvalidQuery("the primary key must be an integer".into()))
}

/// Binds a contract [`Value`] as a SQL parameter.
fn bind(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::val(Option::<i64>::None),
        Value::Bool(b) => Expr::val(*b),
        Value::Int(i) => Expr::val(*i),
        Value::Real(f) => Expr::val(*f),
        Value::Text(s) => Expr::val(s.clone()),
    }
}

/// Translates a filter node into a SQL condition — the counterpart of the
/// in-memory engine's evaluator; the conformance suite pins them together.
fn translate(node: &FilterNode) -> Result<Condition, StorageError> {
    match node {
        FilterNode::All(children) => {
            let mut cond = Condition::all();
            for child in children {
                cond = cond.add(translate(child)?);
            }
            Ok(cond)
        }
        FilterNode::Any(children) => {
            let mut cond = Condition::any();
            for child in children {
                cond = cond.add(translate(child)?);
            }
            Ok(cond)
        }
        FilterNode::Cond(cond) => {
            let col = Expr::col(Alias::new(&cond.field));
            let first = || cond.values.first().cloned().unwrap_or(Value::Null);
            let pattern = |fmt: fn(&str) -> String| match first().as_str() {
                Some(s) => fmt(s),
                None => String::new(),
            };
            let expr: Expr = match cond.op {
                Op::Eq => col.eq(bind(&first())),
                Op::NotEq => col.ne(bind(&first())),
                Op::Gt => col.gt(bind(&first())),
                Op::Gte => col.gte(bind(&first())),
                Op::Lt => col.lt(bind(&first())),
                Op::Lte => col.lte(bind(&first())),
                Op::In => col.is_in(cond.values.iter().map(bind).collect::<Vec<Expr>>()),
                Op::NotIn => col.is_not_in(cond.values.iter().map(bind).collect::<Vec<Expr>>()),
                Op::Like => col.like(pattern(|s| s.to_owned())),
                Op::NotLike => col.not_like(pattern(|s| s.to_owned())),
                // No ILIKE in SQLite/MySQL: LOWER on the column, folded in Rust.
                Op::Ilike => Expr::from(Func::lower(col)).like(pattern(|s| s.to_lowercase())),
                Op::IsNull => col.is_null(),
                Op::IsNotNull => col.is_not_null(),
                Op::Between => {
                    let lo = cond.values.first().cloned().unwrap_or(Value::Null);
                    let hi = cond.values.get(1).cloned().unwrap_or(Value::Null);
                    col.between(bind(&lo), bind(&hi))
                }
                Op::NotBetween => {
                    let lo = cond.values.first().cloned().unwrap_or(Value::Null);
                    let hi = cond.values.get(1).cloned().unwrap_or(Value::Null);
                    col.not_between(bind(&lo), bind(&hi))
                }
                Op::Contains => col.like(pattern(|s| format!("%{s}%"))),
                Op::StartsWith => col.like(pattern(|s| format!("{s}%"))),
                Op::EndsWith => col.like(pattern(|s| format!("%{s}"))),
            };
            Ok(expr.into())
        }
    }
}

// ---- pure statement builders (unit-tested against every dialect) ----------

/// The DDL counterpart of a [`Schema`]: `CREATE TABLE IF NOT EXISTS` whose
/// primary key carries a backend-native id generator — SQLite leans on the
/// rowid alias, MySQL gets `AUTO_INCREMENT`, PostgreSQL gets an identity
/// column. Generated ids come back via RETURNING (PG/SQLite) or
/// `last_insert_id` (MySQL).
fn create_table_statement(schema: &Schema, backend: DbBackend) -> TableCreateStatement {
    let mut create = Table::create();
    create.table(Alias::new(&schema.table)).if_not_exists();
    for column in &schema.columns {
        let mut def = ColumnDef::new(Alias::new(&column.name));
        match column.kind {
            ColumnKind::Bool => {
                def.boolean();
            }
            // Int columns are BIGINT everywhere but SQLite: the contract's
            // identity model is i64 and sqlx decodes strictly, so PG's INT4
            // would reject it. SQLite alone keeps `integer` — its rowid
            // alias requires exactly that spelling on the primary key.
            ColumnKind::Int => match backend {
                DbBackend::Postgres | DbBackend::MySql => {
                    def.big_integer();
                }
                _ => {
                    def.integer();
                }
            },
            ColumnKind::Real => {
                def.double();
            }
            ColumnKind::Text => {
                def.text();
            }
        }
        if column.name == schema.primary_key {
            def.primary_key();
            match backend {
                // sea-query's auto_increment renders nothing on PostgreSQL;
                // spell both dialects out explicitly instead. BIGINT keeps
                // the contract's i64 identity model: PG's RETURNING decodes
                // strictly as INT8, MySQL AUTO_INCREMENT is width-honest.
                // (SQLite is the exception: its rowid alias requires the
                // exact `integer PRIMARY KEY` spelling above.)
                DbBackend::MySql => {
                    def.big_integer().not_null().extra("AUTO_INCREMENT");
                }
                DbBackend::Postgres => {
                    def.big_integer()
                        .not_null()
                        .extra("GENERATED BY DEFAULT AS IDENTITY");
                }
                _ => {}
            }
        }
        create.col(def);
    }
    create
}

/// Text columns order by byte value (the contract's pinned collation);
/// locale-collating backends get an explicit binary COLLATE so the suite's
/// ordering expectations hold verbatim on PostgreSQL and MySQL.
fn order_expr(field: &str, kind: ColumnKind, backend: DbBackend) -> Expr {
    if kind != ColumnKind::Text {
        return Expr::col(Alias::new(field));
    }
    match backend {
        DbBackend::MySql => Expr::cust(format!("`{field}` COLLATE utf8mb4_bin")),
        DbBackend::Postgres => Expr::cust(format!("\"{field}\" COLLATE \"C\"")),
        _ => Expr::col(Alias::new(field)),
    }
}

/// The page-of-rows SELECT for a list query: projection, WHERE, ordering
/// (primary key ascending by default), and the paging clause — token paging
/// continues after the cursor with a one-row peek.
fn list_select(
    schema: &Schema,
    cols: &[String],
    where_clause: Option<&Condition>,
    query: &ListQuery,
    backend: DbBackend,
) -> Result<SelectStatement, StorageError> {
    let pk = schema.primary_key.as_str();
    let kind_of = |field: &str| {
        schema
            .column(field)
            .map(|column| column.kind)
            .unwrap_or(ColumnKind::Text)
    };
    let mut select = Query::select();
    select.from(Alias::new(schema.table.as_str()));
    for col in cols {
        select.column(Alias::new(col));
    }
    if let Some(cond) = where_clause {
        select.cond_where(cond.clone());
    }
    if query.sort.is_default() {
        select.order_by_expr(order_expr(pk, ColumnKind::Int, backend), Order::Asc);
    } else {
        for term in &query.sort.fields {
            let order = match term.dir {
                SortDir::Asc => Order::Asc,
                SortDir::Desc => Order::Desc,
            };
            select.order_by_expr(
                order_expr(&term.field, kind_of(&term.field), backend),
                order,
            );
        }
    }
    let limit = query.paging.limit();
    match &query.paging {
        Paging::Token { token, .. } => {
            if !token.is_empty() {
                let last = rushwind_storage::decode_cursor(token)?;
                select.cond_where(Condition::all().add(Expr::col(Alias::new(pk)).gt(last)));
            }
            // Peek one row past the page to learn whether the stream continues.
            select.limit(u64::from(limit) + 1);
        }
        Paging::Page { page, .. } => {
            select
                .offset((u64::from(*page - 1)) * u64::from(limit))
                .limit(u64::from(limit));
        }
        Paging::Offset { offset, .. } => {
            select.offset(*offset).limit(u64::from(limit));
        }
    }
    Ok(select)
}

#[cfg(test)]
mod sql_snapshots {
    //! Dialect snapshots: the exact SQL every backend receives, pinned as
    //! strings — no live database needed to catch a translation regression.

    use super::*;
    use rushwind_storage::Sort;

    fn schema() -> Schema {
        Schema::builder("widgets", "id")
            .column("name", ColumnKind::Text)
            .column("age", ColumnKind::Int)
            .column("score", ColumnKind::Real)
            .column("owner_id", ColumnKind::Int)
            .column("unit_id", ColumnKind::Int)
            .build()
            .expect("valid schema")
    }

    fn render(stmt: &impl Buildable, backend: DbBackend) -> (String, usize) {
        let (sql, values) = stmt.build_for(backend);
        (sql, values.0.len())
    }

    #[test]
    fn ddl_per_dialect() {
        let create = create_table_statement(&schema(), DbBackend::Sqlite);
        let (mysql, _) = render(
            &create_table_statement(&schema(), DbBackend::MySql),
            DbBackend::MySql,
        );
        let (postgres, _) = render(
            &create_table_statement(&schema(), DbBackend::Postgres),
            DbBackend::Postgres,
        );
        let (sqlite, _) = render(&create, DbBackend::Sqlite);
        assert!(
            sqlite.contains("CREATE TABLE IF NOT EXISTS \"widgets\""),
            "{sqlite}"
        );
        // Exact `integer PRIMARY KEY` (no BIGINT, no NOT NULL noise): SQLite
        // aliases the rowid only for this spelling, which generated ids and
        // last_insert_id rely on.
        assert!(
            sqlite.contains("\"id\" integer PRIMARY KEY"),
            "sqlite pk must alias the rowid: {sqlite}"
        );

        assert!(
            mysql.contains("CREATE TABLE IF NOT EXISTS `widgets`"),
            "{mysql}"
        );
        assert!(
            mysql.contains("`id` bigint NOT NULL PRIMARY KEY AUTO_INCREMENT"),
            "mysql pk must be a BIGINT AUTO_INCREMENT: {mysql}"
        );
        assert!(mysql.contains("`age` bigint"), "{mysql}");

        assert!(
            postgres.contains("CREATE TABLE IF NOT EXISTS \"widgets\""),
            "{postgres}"
        );
        assert!(
            postgres
                .contains("\"id\" bigint NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY"),
            "pg pk must be a BIGINT identity column: {postgres}"
        );
        assert!(postgres.contains("\"age\" bigint"), "{postgres}");
    }

    #[test]
    fn filter_placeholders_per_dialect() {
        let filter = FilterExpr::all([
            FilterExpr::cond("age", Op::Gte, [Value::Int(10)]),
            FilterExpr::cond("name", Op::Like, [Value::Text("%a%".into())]),
            FilterExpr::cond("score", Op::IsNull, []),
        ]);
        let select = list_select(
            &schema(),
            &["id".into(), "name".into()],
            Some(&translate(filter.node()).expect("translates")),
            &ListQuery::page(2, 10),
            DbBackend::Sqlite,
        )
        .expect("builds");

        let (sqlite, binds) = render(&select, DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id", "name" FROM "widgets" WHERE "age" >= ? AND "name" LIKE ? AND "score" IS NULL ORDER BY "id" ASC LIMIT ? OFFSET ?"#
        );
        assert_eq!(
            binds, 4,
            "two filter operands plus the bound limit and offset"
        );

        let (mysql, _) = render(&select, DbBackend::MySql);
        assert_eq!(
            mysql,
            "SELECT `id`, `name` FROM `widgets` WHERE `age` >= ? AND `name` LIKE ? AND `score` IS NULL ORDER BY `id` ASC LIMIT ? OFFSET ?"
        );

        let (postgres, _) = render(&select, DbBackend::Postgres);
        assert_eq!(
            postgres,
            "SELECT \"id\", \"name\" FROM \"widgets\" WHERE \"age\" >= $1 AND \"name\" LIKE $2 AND \"score\" IS NULL ORDER BY \"id\" ASC LIMIT $3 OFFSET $4"
        );
    }

    #[test]
    fn ilike_lowers_the_column() {
        let filter = FilterExpr::cond("name", Op::Ilike, [Value::Text("%E%".into())]);
        let select = list_select(
            &schema(),
            &["id".into()],
            Some(&translate(filter.node()).expect("translates")),
            &ListQuery::default(),
            DbBackend::Sqlite,
        )
        .expect("builds");
        let (sqlite, _) = render(&select, DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id" FROM "widgets" WHERE LOWER("name") LIKE ? ORDER BY "id" ASC LIMIT ? OFFSET ?"#
        );
    }

    #[test]
    fn text_sorts_force_binary_collation_on_locale_backends() {
        let query = ListQuery {
            paging: Paging::Offset {
                offset: 0,
                limit: 9,
            },
            sort: Sort::by("name", SortDir::Asc),
            ..ListQuery::default()
        };
        let build = |backend| {
            list_select(&schema(), &["id".into()], None, &query, backend).expect("builds")
        };
        let (sqlite, _) = render(&build(DbBackend::Sqlite), DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id" FROM "widgets" ORDER BY "name" ASC LIMIT ? OFFSET ?"#
        );
        let (mysql, _) = render(&build(DbBackend::MySql), DbBackend::MySql);
        assert_eq!(
            mysql,
            "SELECT `id` FROM `widgets` ORDER BY `name` COLLATE utf8mb4_bin ASC LIMIT ? OFFSET ?"
        );
        let (postgres, _) = render(&build(DbBackend::Postgres), DbBackend::Postgres);
        assert_eq!(
            postgres,
            "SELECT \"id\" FROM \"widgets\" ORDER BY \"name\" COLLATE \"C\" ASC LIMIT $1 OFFSET $2"
        );
    }

    #[test]
    fn in_and_between_bind_every_operand() {
        let filter = FilterExpr::all([
            FilterExpr::cond(
                "unit_id",
                Op::In,
                [Value::Int(10), Value::Int(20), Value::Int(30)],
            ),
            FilterExpr::cond("age", Op::Between, [Value::Int(5), Value::Int(25)]),
        ]);
        let select = list_select(
            &schema(),
            &["id".into()],
            Some(&translate(filter.node()).expect("translates")),
            &ListQuery::default(),
            DbBackend::Sqlite,
        )
        .expect("builds");
        let (sqlite, binds) = render(&select, DbBackend::Sqlite);
        assert!(sqlite.contains(r#""unit_id" IN (?, ?, ?)"#), "{sqlite}");
        assert!(sqlite.contains(r#""age" BETWEEN ? AND ?"#), "{sqlite}");
        assert_eq!(
            binds, 7,
            "three IN operands plus two BETWEEN bounds plus limit and offset"
        );
    }

    #[test]
    fn token_cursor_filters_and_peeks_one_row() {
        let query = ListQuery {
            paging: Paging::Token {
                token: rushwind_storage::encode_cursor(42),
                limit: 3,
            },
            ..ListQuery::default()
        };
        let select = list_select(&schema(), &["id".into()], None, &query, DbBackend::Sqlite)
            .expect("builds");
        let (sqlite, binds) = render(&select, DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id" FROM "widgets" WHERE "id" > ? ORDER BY "id" ASC LIMIT ?"#
        );
        assert_eq!(binds, 2, "the cursor id plus the peek limit");
    }

    #[test]
    fn default_sort_is_primary_key_ascending() {
        let select = list_select(
            &schema(),
            &["id".into()],
            None,
            &ListQuery {
                paging: Paging::Offset {
                    offset: 5,
                    limit: 7,
                },
                ..ListQuery::default()
            },
            DbBackend::Sqlite,
        )
        .expect("builds");
        let (sqlite, _) = render(&select, DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id" FROM "widgets" ORDER BY "id" ASC LIMIT ? OFFSET ?"#
        );
    }

    #[test]
    fn explicit_sort_terms_render_in_order() {
        let query = ListQuery {
            paging: Paging::Offset {
                offset: 0,
                limit: 9,
            },
            sort: Sort::by("unit_id", SortDir::Asc).then("age", SortDir::Desc),
            ..ListQuery::default()
        };
        let select = list_select(&schema(), &["id".into()], None, &query, DbBackend::Sqlite)
            .expect("builds");
        let (sqlite, _) = render(&select, DbBackend::Sqlite);
        assert_eq!(
            sqlite,
            r#"SELECT "id" FROM "widgets" ORDER BY "unit_id" ASC, "age" DESC LIMIT ? OFFSET ?"#
        );
    }

    #[test]
    fn garbage_cursor_is_invalid_query() {
        let query = ListQuery {
            paging: Paging::Token {
                token: "!!!".into(),
                limit: 3,
            },
            ..ListQuery::default()
        };
        let err = list_select(&schema(), &["id".into()], None, &query, DbBackend::Sqlite)
            .expect_err("garbage cursor must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }
}
