//! Wire-format end to end: protojson documents → contract queries → the
//! in-memory reference engine. The expectations mirror the conformance
//! suite's standard rows, so a divergence anywhere on the path fails loudly.

use std::sync::Arc;

use prost::Message as _;
use rushwind_storage::{
    FilterExpr, FilterNode, ListQuery, Op, Paging, QueryCtx, Record, Repository, StorageError,
    Value, MAX_LIMIT,
};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_proto::v1::{PagingRequest, Sorting};
use rushwind_storage_proto::wire::{
    filter_expr_from_json, list_query_from_json, list_query_from_proto,
};

async fn repo() -> Arc<dyn Repository> {
    let repo = MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
        .expect("suite schema is valid");
    for row in rushwind_testkit::storage_conformance::standard_rows() {
        repo.create(QueryCtx::all_access(), row)
            .await
            .expect("create succeeds");
    }
    Arc::new(repo)
}

fn ids(page: &Page<Record>) -> Vec<i64> {
    page.items
        .iter()
        .map(|row| row.get("id").and_then(Value::as_i64).expect("int id"))
        .collect()
}

use rushwind_storage::Page;

#[tokio::test]
async fn full_request_document_drives_the_engine() {
    let repo = repo().await;
    // protojson: lowerCamelCase, enum member names, FieldMask as a comma
    // string — the standard protojson encoding.
    let request = r#"{
        "paginationType": {"pageBased": {"page": 1, "pageSize": 10}},
        "filterExpr": {"type": "AND", "conditions": [
            {"field": "age", "op": "GTE", "value": 10},
            {"field": "unit_id", "op": "EQ", "value": 10}
        ]},
        "sorting": [{"field": "age", "direction": "DESC"}],
        "fieldMask": {"paths": ["id", "name"]}
    }"#;

    let query = list_query_from_json(request).expect("parses");
    let page = repo
        .list(QueryCtx::all_access(), &query)
        .await
        .expect("list");
    // unit 10 rows: alpha (age 5, filtered out), charlie (age 15, kept).
    assert_eq!(ids(&page), vec![3]);
    assert_eq!(
        page.items[0].get("name").and_then(Value::as_str),
        Some("charlie")
    );
    // The field mask projects: only id and name survive.
    assert_eq!(page.items[0].len(), 2);
}

#[test]
fn snake_case_and_operators_map_identically() {
    let camel = list_query_from_json(
        r#"{"paginationType": {"pageBased": {"page": 1, "pageSize": 20}},
            "filterExpr": {"conditions": [{"field": "score", "op": "IS_NULL"}]}}"#,
    )
    .expect("camel parses");
    let snake = list_query_from_json(
        r#"{"pagination_type": {"page_based": {"page": 1, "page_size": 20}},
            "filter_expr": {"conditions": [{"field": "score", "op": "IS_NULL"}]}}"#,
    )
    .expect("snake parses");
    let proto: PagingRequest = serde_json::from_str(
        r#"{"pagination_type": {"page_based": {"page": 1, "page_size": 20}}}"#,
    )
    .expect("proto parses");
    assert_eq!(camel.paging, snake.paging);
    assert_eq!(
        camel.paging,
        list_query_from_proto(&proto)
            .expect("proto converts")
            .paging
    );
    assert!(matches!(
        snake.filter,
        Some(ref f) if matches!(f.node(), FilterNode::All(_))
    ));
}

#[test]
fn derived_pattern_operators_fold_into_ilike() {
    let filter = filter_expr_from_json(
        r#"{"type": "AND", "conditions": [
            {"field": "name", "op": "ICONTAINS", "value": "am"},
            {"field": "name", "op": "ISTARTS_WITH", "value": "AL"},
            {"field": "name", "op": "IENDS_WITH", "value": "HA"}
        ]}"#,
    )
    .expect("parses");
    match filter.node() {
        FilterNode::All(children) => {
            assert_eq!(children.len(), 3, "three conditions, all folded to Ilike");
            for child in children {
                assert!(matches!(child, FilterNode::Cond(c) if c.op == Op::Ilike));
            }
        }
        other => panic!("unexpected tree: {other:?}"),
    }
}

#[test]
fn iexact_maps_to_its_own_operator() {
    let filter = filter_expr_from_json(
        r#"{"conditions": [{"field": "name", "op": "IEXACT", "value": "CHARLIE"}]}"#,
    )
    .expect("parses");
    let FilterNode::All(children) = filter.node() else {
        panic!("unexpected tree: {filter:?}");
    };
    assert_eq!(children.len(), 1);
    assert!(
        matches!(&children[0], FilterNode::Cond(c) if c.op == Op::Iexact),
        "IEXACT is its own contract operator, not a LIKE fold: {filter:?}"
    );
}

#[test]
fn nested_groups_translate_recursively() {
    let filter = filter_expr_from_json(
        r#"{"type": "OR", "conditions": [{"field": "age", "op": "LT", "value": 6}],
            "groups": [{"type": "AND", "conditions": [
                {"field": "score", "op": "GTE", "value": 2.5},
                {"field": "unit_id", "op": "EQ", "value": 30}
            ]}]}"#,
    )
    .expect("parses");
    match filter.node() {
        FilterNode::Any(arms) => {
            assert_eq!(arms.len(), 2);
            assert!(matches!(&arms[1], FilterNode::All(_)));
        }
        other => panic!("unexpected tree: {other:?}"),
    }
}

#[test]
fn unsupported_taxonomy_is_named_at_the_boundary() {
    for op in [
        "REGEXP",
        "IREGEXP",
        "JSON_CONTAINS",
        "ARRAY_CONTAINS",
        "EXISTS",
        "SEARCH",
    ] {
        let err = filter_expr_from_json(&format!(
            r#"{{"conditions": [{{"field": "name", "op": "{op}", "value": "x"}}]}}"#
        ))
        .expect_err("must be rejected");
        assert!(
            matches!(err, StorageError::Unsupported(ref m) if m.contains(op)),
            "op {op} must be a named Unsupported, got: {err}"
        );
    }
    // The date_part/json_path extensions likewise.
    let err = filter_expr_from_json(
        r#"{"conditions": [{"field": "created_at", "op": "GTE", "value": 1, "datePart": "DAY"}]}"#,
    )
    .expect_err("must be rejected");
    assert!(matches!(
        err,
        StorageError::Unsupported(ref m) if m.contains("date_part")
    ));
}

#[test]
fn malformed_wire_is_invalid_query() {
    let err =
        filter_expr_from_json(r#"{"conditions": [{"field": "age"}]}"#).expect_err("no operator");
    assert!(matches!(err, StorageError::InvalidQuery(_)));
    let err = filter_expr_from_json(r#"{"conditions": [{"field": "age", "op": "NOPE"}]}"#)
        .expect_err("unknown operator name");
    assert!(matches!(err, StorageError::InvalidQuery(_)));
    let err = filter_expr_from_json("not json at all").expect_err("not json");
    assert!(matches!(err, StorageError::InvalidQuery(_)));
}

#[tokio::test]
async fn json_query_drives_the_engine() {
    let repo = repo().await;
    let query = list_query_from_json(
        r#"{"paginationType": {"pageBased": {"page": 1, "pageSize": 10}},
            "filterExpr": {"type": "AND", "conditions": [
                {"field": "age", "op": "GTE", "value": 10},
                {"field": "name", "op": "LIKE", "value": "%a%"}
            ]},
            "sorting": [{"field": "age", "direction": "ASC"}]}"#,
    )
    .expect("parses");
    let page = repo
        .list(QueryCtx::all_access(), &query)
        .await
        .expect("list");
    assert_eq!(
        ids(&page),
        vec![2, 3, 4],
        "%a% matches Bravo, charlie, delta in age order"
    );
}

#[tokio::test]
async fn aip_text_drives_the_engine() {
    let repo = repo().await;
    let filter =
        FilterExpr::from_aip(r#"name = "alpha" OR (age >= 10 AND unit_id = 20)"#).expect("parses");
    let query = ListQuery {
        filter: Some(filter),
        ..ListQuery::page(1, 10)
    };
    let page = repo
        .list(QueryCtx::all_access(), &query)
        .await
        .expect("list");
    assert_eq!(ids(&page), vec![1, 2, 4]);

    let filter = FilterExpr::from_aip("owner_id IN (1, 3) AND NOT age = 10").expect("parses");
    let query = ListQuery {
        filter: Some(filter),
        ..ListQuery::page(1, 10)
    };
    let page = repo
        .list(QueryCtx::all_access(), &query)
        .await
        .expect("list");
    assert_eq!(ids(&page), vec![1, 5]);
}

#[test]
fn offset_based_pagination_maps_from_json() {
    let query =
        list_query_from_json(r#"{"paginationType": {"offsetBased": {"offset": 30, "limit": 10}}}"#)
            .expect("parses");
    assert_eq!(
        query.paging,
        Paging::Offset {
            offset: 30,
            limit: 10
        }
    );
}

#[test]
fn operand_scalars_keep_their_kinds() {
    let filter = filter_expr_from_json(
        r#"{"type": "AND", "conditions": [
            {"field": "flag", "op": "EQ", "value": true},
            {"field": "name", "op": "EQ", "value": "bolt"}
        ]}"#,
    )
    .expect("parses");
    match filter.node() {
        FilterNode::All(children) => {
            let expected = [Value::Bool(true), Value::Text("bolt".into())];
            for (child, want) in children.iter().zip(expected) {
                assert!(
                    matches!(child, FilterNode::Cond(c) if c.values[0] == want),
                    "scalar kind mismatch"
                );
            }
        }
        other => panic!("unexpected tree: {other:?}"),
    }
    // protojson semantics: JSON null on a message field means "not set", so
    // `EQ null` arrives with zero operands and is rejected as a malformed
    // arity — SQL NULL tests must use IS_NULL, which is the contract's
    // spelling of that intent.
    let err =
        filter_expr_from_json(r#"{"conditions": [{"field": "score", "op": "EQ", "value": null}]}"#)
            .expect("parses structurally");
    match err.node() {
        FilterNode::All(children) => match &children[0] {
            FilterNode::Cond(cond) => assert!(
                cond.values.is_empty(),
                "a protojson null operand is indistinguishable from an unset field"
            ),
            other => panic!("unexpected leaf: {other:?}"),
        },
        other => panic!("unexpected tree: {other:?}"),
    }
}

#[test]
fn binary_proto_roundtrip_matches_the_json_face() {
    // The same request encoded as protobuf bytes (what a gRPC client sends)
    // must convert to the identical contract query as its protojson twin.
    let proto = PagingRequest {
        pagination_type: Some(rushwind_storage_proto::v1::PaginationRequest {
            pagination_type: Some(
                rushwind_storage_proto::v1::pagination_request::PaginationType::OffsetBased(
                    rushwind_storage_proto::v1::OffsetBasedPagination {
                        offset: 6,
                        limit: 4,
                    },
                ),
            ),
        }),
        filter: Some(
            rushwind_storage_proto::v1::paging_request::Filter::FilterExpr(
                rushwind_storage_proto::v1::FilterExpr {
                    r#type: rushwind_storage_proto::v1::ExprType::And.into(),
                    conditions: vec![rushwind_storage_proto::v1::FilterCondition {
                        field: "age".into(),
                        op: rushwind_storage_proto::v1::Operator::Gte.into(),
                        value: Some(pbjson_types::Value {
                            kind: Some(pbjson_types::value::Kind::NumberValue(10.0)),
                        }),
                        values: Vec::new(),
                        date_part: String::new(),
                        json_path: String::new(),
                    }],
                    groups: Vec::new(),
                },
            ),
        ),
        sorting: vec![Sorting {
            field: "age".into(),
            direction: rushwind_storage_proto::v1::SortDirection::Desc.into(),
        }],
        field_mask: Some(rushwind_storage_proto::v1::FieldMask {
            paths: vec!["id".into(), "name".into()],
        }),
    };

    let bytes = proto.encode_to_vec();
    let decoded = PagingRequest::decode(bytes.as_slice()).expect("decodes");
    let from_bytes = list_query_from_proto(&decoded).expect("converts");
    let from_struct = list_query_from_proto(&proto).expect("converts");
    assert_eq!(from_bytes.paging, from_struct.paging);
    assert_eq!(from_bytes.sort, from_struct.sort);
    assert_eq!(from_bytes.mask, from_struct.mask);
    assert_eq!(
        from_bytes.paging,
        Paging::Offset {
            offset: 6,
            limit: 4
        }
    );
}

#[test]
fn no_paging_maps_to_the_max_window() {
    let query = list_query_from_json(r#"{"paginationType": {"noPaging": {}}}"#).expect("parses");
    assert_eq!(
        query.paging,
        Paging::Offset {
            offset: 0,
            limit: MAX_LIMIT
        }
    );
}
