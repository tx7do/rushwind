//! Storage conformance for the SeaORM-backed engine (SQLite in memory).

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_seaorm::SeaRepo;

/// A fresh, empty in-memory SQLite database for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let repo = SeaRepo::sqlite_memory(rushwind_testkit::storage_conformance::suite_schema())
        .await
        .expect("in-memory sqlite opens");
    repo.migrate_create().await.expect("suite table creates");
    Arc::new(repo)
}

/// A fresh, empty tenant-scoped database for the tenancy suite — same
/// embedded lifecycle as [`fresh_repo`], bound to the tenant schema instead.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    let repo = SeaRepo::sqlite_memory(rushwind_testkit::storage_conformance::tenant_suite_schema())
        .await
        .expect("in-memory sqlite opens");
    repo.migrate_create()
        .await
        .expect("tenant suite table creates");
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
