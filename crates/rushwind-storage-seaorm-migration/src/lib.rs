//! Schema-migration machinery for SeaORM services.
//!
//! Entity definitions are the schema's source of truth: the builder
//! derives create-table statements straight from `EntityTrait`
//! implementations, and the finished migration is tracked and applied
//! through `sea-orm-migration` — so a service bootstraps its schema
//! from the very definitions its code maps rows with. Indexes and
//! foreign keys beyond the entity catalog ride later migrations via
//! [`EntityTables::statement`].

#![forbid(unsafe_code)]
use sea_orm::sea_query::{ColumnDef as QueryColumnDef, Index, StringLen, Table};
use sea_orm::{
    ColumnTrait, ColumnType, DatabaseBackend, DbErr, EntityTrait, IdenStatic, Iterable,
    PrimaryKeyToColumn, PrimaryKeyTrait, RelationTrait,
};
use sea_orm_migration::async_trait;
use sea_orm_migration::prelude::TableCreateStatement;
pub use sea_orm_migration::{MigrationName, MigrationTrait, MigratorTrait, SchemaManager};

/// Fluently assembles an entity-derived migration: one [`table`] call
/// per table, then [`build`].
///
/// [`table`]: EntityTables::table
/// [`build`]: EntityTables::build
pub struct EntityTables {
    name: String,
    backend: DatabaseBackend,
    tables: Vec<TableCreateStatement>,
}

impl EntityTables {
    /// Starts a migration over `backend`, tracked under `name`.
    pub fn new(name: impl Into<String>, backend: DatabaseBackend) -> Self {
        Self {
            name: name.into(),
            backend,
            tables: Vec::new(),
        }
    }

    /// Appends the create-table statement derived from one entity:
    /// column names, types, nullability, defaults, primary keys, and
    /// relations-turned-foreign-keys come from the entity's own
    /// definitions. On PostgreSQL a single auto-increment primary key
    /// coerces to its signed base type — unsigned serials do not exist
    /// there — unsigned id fields materialize as `integer`
    /// identities.
    pub fn table<E>(mut self) -> Self
    where
        E: EntityTrait + Default,
    {
        self.tables.push(entity_table::<E>(self.backend));
        self
    }

    /// Appends a hand-built statement — custom tables and, in later
    /// migrations, indexes and foreign keys.
    pub fn statement(mut self, stmt: TableCreateStatement) -> Self {
        self.tables.push(stmt);
        self
    }

    /// Finishes the migration.
    pub fn build(self) -> EntityTablesMigration {
        let EntityTables { name, tables, .. } = self;
        EntityTablesMigration { name, tables }
    }
}

/// The finished migration: creates its tables in declaration order,
/// tracked by sea-orm-migration.
pub struct EntityTablesMigration {
    name: String,
    tables: Vec<TableCreateStatement>,
}

impl MigrationName for EntityTablesMigration {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl MigrationTrait for EntityTablesMigration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for stmt in &self.tables {
            manager.create_table(stmt.clone()).await?;
        }
        Ok(())
    }
}

/// Derives one create-table statement from an entity. Mirrors the
/// sea-orm schema builder, with one portability patch: a single
/// auto-increment primary key whose value type is unsigned is emitted
/// in its signed base width on PostgreSQL.
fn entity_table<E>(backend: DatabaseBackend) -> TableCreateStatement
where
    E: EntityTrait + Default,
{
    let entity = E::default();
    let mut create = Table::create().to_owned();
    let pk_cols: Vec<E::Column> = E::PrimaryKey::iter().map(|pk| pk.into_column()).collect();
    let single_pk_auto = E::PrimaryKey::auto_increment() && pk_cols.len() == 1;

    for column in E::Column::iter() {
        let orm = column.def();
        let is_pk = pk_cols.iter().any(|c| c.as_str() == column.as_str());
        let col_type: ColumnType = match orm.get_column_type() {
            // ActiveEnum columns reference a PG enum type that a fresh
            // database has no `CREATE TYPE` behind; the storage contract
            // for enums is their name as text.
            ColumnType::Enum { .. } => ColumnType::String(StringLen::None),
            // PostgreSQL has no unsigned integers: every unsigned column
            // materializes in its signed base width, and decoders agree.
            ColumnType::Unsigned if backend == DatabaseBackend::Postgres => ColumnType::Integer,
            other => other.clone(),
        };
        let mut def = QueryColumnDef::new_with_type(column, col_type);
        if !orm.is_null() {
            def.not_null();
        }
        if orm.is_unique() {
            def.unique_key();
        }
        if let Some(d) = orm.get_column_default() {
            def.default(d.clone());
        }
        if is_pk && single_pk_auto {
            def.auto_increment();
        }
        if is_pk && pk_cols.len() == 1 {
            def.primary_key();
        }
        create.col(&mut def);
    }

    if E::PrimaryKey::iter().count() > 1 {
        let mut idx_pk = Index::create();
        for pk in E::PrimaryKey::iter() {
            idx_pk.col(pk);
        }
        create.primary_key(idx_pk.name(format!("pk-{}", entity.to_string())).primary());
    }

    for relation in E::Relation::iter() {
        let rel = relation.def();
        if rel.is_owner || rel.skip_fk {
            continue;
        }
        create.foreign_key(&mut rel.into());
    }

    create.table(entity.table_ref()).take()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::sea_query::Alias;

    fn dummy_table(name: &str) -> TableCreateStatement {
        Table::create()
            .table(Alias::new(name))
            .col(
                QueryColumnDef::new(Alias::new("id"))
                    .integer()
                    .not_null()
                    .primary_key(),
            )
            .to_owned()
    }

    #[test]
    fn assembles_named_migration() {
        let m = EntityTables::new("m20250915_000001_init", DatabaseBackend::Postgres)
            .statement(dummy_table("t_one"))
            .statement(dummy_table("t_two"))
            .build();
        assert_eq!(MigrationName::name(&m), "m20250915_000001_init");
        assert_eq!(m.tables.len(), 2);
    }
}
