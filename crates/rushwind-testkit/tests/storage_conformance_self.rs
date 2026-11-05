//! Suite self-check: the storage conformance suite must pass against the
//! in-memory reference engine — the same semantic baseline the SQL engines
//! are pinned to. If this fails, the suite has drifted from the contract,
//! not the engines.

#![cfg(feature = "storage")]

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_memory::MemoryRepo;

/// The reference engine: fresh, empty, bound to the suite schema.
async fn fresh_repo() -> Arc<dyn Repository> {
    Arc::new(
        MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
            .expect("suite schema is valid"),
    )
}

/// The reference engine on the tenant-scoped schema for the tenancy suite.
async fn fresh_tenant_repo() -> Arc<dyn Repository> {
    Arc::new(
        MemoryRepo::new(rushwind_testkit::storage_conformance::tenant_suite_schema())
            .expect("tenant suite schema is valid"),
    )
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo, tenant: crate::fresh_tenant_repo);
