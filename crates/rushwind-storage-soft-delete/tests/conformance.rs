//! Contract transparency: the soft-delete decorator must pass the full
//! storage conformance suite over an in-memory engine — the base contract
//! on the plain table, and the tenancy suite on the tenant-scoped one.
//! Every decorator path forwards the caller's ctx (the tombstone pre-reads
//! and the silent tombstone write keep the viewer, dropping only the audit
//! sink), so the engine's tenant predicates hold underneath.

use std::sync::Arc;

use rushwind_storage::{Column, ColumnKind, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_soft_delete::{SoftDeleteRepo, DELETED_AT_COLUMN};

/// The suite schema plus the tombstone column.
fn soft_delete_schema() -> Schema {
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.columns.push(Column {
        name: DELETED_AT_COLUMN.into(),
        kind: ColumnKind::Int,
    });
    schema
}

/// The tenant-scoped suite schema plus the tombstone column.
fn tenant_soft_delete_schema() -> Schema {
    let mut schema = rushwind_testkit::storage_conformance::tenant_suite_schema();
    schema.columns.push(Column {
        name: DELETED_AT_COLUMN.into(),
        kind: ColumnKind::Int,
    });
    schema
}

/// A fresh, empty soft-deleting table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let inner = MemoryRepo::new(soft_delete_schema()).expect("suite schema is valid");
    SoftDeleteRepo::new(Arc::new(inner)).expect("schema carries the tombstone column")
}

/// A fresh, empty soft-deleting tenant-scoped table for the tenancy
/// suite — the same decorator stack over [`tenant_suite_schema`].
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    let inner = MemoryRepo::new(tenant_soft_delete_schema()).expect("tenant suite schema is valid");
    SoftDeleteRepo::new(Arc::new(inner)).expect("schema carries the tombstone column")
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
