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
    Alias, Condition, DeleteStatement, Expr, ExprTrait, InsertStatement, MysqlQueryBuilder,
    PostgresQueryBuilder, Query, SelectStatement, SqliteQueryBuilder, TableCreateStatement,
    UpdateStatement, Values,
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, DbErr, RuntimeErr,
    Statement, TransactionTrait,
};

use rushwind_storage::{
    AuditAction, AuditEntry, ColumnKind, FieldMask, FilterExpr, ListQuery, Page, Paging, QueryCtx,
    Record, RecursiveCte, RepoFuture, Repository, Schema, Scope, StorageError, Value, Viewer,
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

    fn recursive_cte(&self) -> Option<&dyn RecursiveCte> {
        Some(self)
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

mod condition;
#[cfg(test)]
mod sql_snapshots;
mod statements;
mod tree;

use condition::{bind, expect_id, translate, ScopeSql, TenantSql};
use statements::{create_table_statement, list_select};
