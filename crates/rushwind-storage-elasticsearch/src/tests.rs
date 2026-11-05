//! Offline coverage of the tenancy translation — the same pure-layer
//! unit testing the query module gets: rule resolution (fail-closed),
//! the DSL clause the tenant predicate compiles to, and the write-side
//! stamp. The wire paths themselves are pinned by the live suite.

use crate::*;
use rushwind_storage::{FilterExpr, Op};

/// A tenant-scoped schema: the suite table plus the conventional
/// tenant column, declared as the schema's tenant column.
fn tenant_schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("owner_id", ColumnKind::Int)
        .column(Viewer::TENANT_COLUMN, ColumnKind::Int)
        .tenant_column(Viewer::TENANT_COLUMN)
        .build()
        .expect("tenant schema is valid")
}

/// The same table without a tenant column — unscoped.
fn plain_schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("owner_id", ColumnKind::Int)
        .build()
        .expect("plain schema is valid")
}

#[test]
fn unidentified_viewer_fails_closed_before_the_store_is_touched() {
    let repo = ElasticRepo::new("http://localhost:9200", tenant_schema());
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
    let repo = ElasticRepo::new("http://localhost:9200", plain_schema());
    let ctx = QueryCtx::new(Viewer::own(1));
    let rule = repo.tenant_rule(&ctx).expect("unscoped tables never fail");
    assert!(rule.term().is_none(), "no tenant column, no tenant clause");
}

#[test]
fn tenant_term_conjoins_beside_the_scope_filter() {
    let repo = ElasticRepo::new("http://localhost:9200", tenant_schema());
    // A tenant user inside owner 1: both boundaries in one bool query.
    let ctx = QueryCtx::new(Viewer::tenant_user(5, 1));
    let rule = repo.tenant_rule(&ctx).expect("resolves");
    let scope = repo
        .scope_query(&ctx, &rule)
        .expect("builds")
        .expect("not denied");
    assert_eq!(
        scope,
        json!({ "bool": { "must": [
                { "term": { "owner_id": 1 } },
                { "term": { "tenant_id": 5 } },
            ] } })
    );
}

#[test]
fn unrestricted_scope_does_not_short_circuit_the_tenant_term() {
    let repo = ElasticRepo::new("http://localhost:9200", tenant_schema());
    // Tenant admin over the whole tenant: the boundary is NOT
    // match_all — a point read must still re-check the tenant column,
    // so the clause must be a real bool query.
    let ctx = QueryCtx::new(Viewer::tenant(5));
    let rule = repo.tenant_rule(&ctx).expect("resolves");
    let scope = repo
        .scope_query(&ctx, &rule)
        .expect("builds")
        .expect("not denied");
    assert_ne!(
        scope,
        json!({ "match_all": {} }),
        "a tenant viewer must never degrade to an unrestricted query"
    );
    assert_eq!(
        scope,
        json!({ "bool": { "must": [
                { "match_all": {} },
                { "term": { "tenant_id": 5 } },
            ] } })
    );
}

#[test]
fn platform_pass_keeps_the_query_unrestricted() {
    let repo = ElasticRepo::new("http://localhost:9200", tenant_schema());
    let ctx = QueryCtx::all_access();
    let rule = repo.tenant_rule(&ctx).expect("platform passes");
    let scope = repo
        .scope_query(&ctx, &rule)
        .expect("builds")
        .expect("not denied");
    assert_eq!(
        scope,
        json!({ "match_all": {} }),
        "Pass injects no predicate anywhere"
    );
}

#[test]
fn the_query_filter_lands_beside_the_boundaries() {
    let repo = ElasticRepo::new("http://localhost:9200", tenant_schema());
    let ctx = QueryCtx::new(Viewer::tenant(5));
    let rule = repo.tenant_rule(&ctx).expect("resolves");
    let filter = FilterExpr::cond("age", Op::Gt, [Value::Int(10)]);
    let combined = repo
        .combined_query(&ctx, &rule, Some(&filter))
        .expect("builds")
        .expect("not denied");
    assert_eq!(
        combined,
        json!({ "bool": { "must": [
                { "bool": { "must": [
                    { "match_all": {} },
                    { "term": { "tenant_id": 5 } },
                ] } },
                { "range": { "age": { "gt": 10 } } },
            ] } })
    );
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
