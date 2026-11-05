//! Contract transparency: the tree crate's walks ride the repository
//! contract's own reads (every walk forwards the caller's ctx), so the
//! full conformance suite gates the engine the traversals bind to — the
//! base contract on the plain table, and the tenancy suite on the
//! tenant-scoped one. The crate ships no decorator of its own; the suite
//! runs over the bare reference engine.

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
