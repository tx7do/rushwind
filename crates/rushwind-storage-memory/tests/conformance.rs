//! Storage conformance for the in-memory reference engine.

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_memory::MemoryRepo;

/// A fresh, empty table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    Arc::new(
        MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
            .expect("suite schema is valid"),
    )
}

/// A fresh, empty tenant-scoped table for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    Arc::new(
        MemoryRepo::new(rushwind_testkit::storage_conformance::tenant_suite_schema())
            .expect("tenant suite schema is valid"),
    )
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
