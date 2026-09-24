//! Corpus-anchored fixture tests: the plan resolves the reference
//! deployment's option shapes and the apply mutates dynamic messages
//! exactly where the generated Go `Redact()` methods would.
//!
//! The checked-in descriptor sets are produced by the reference
//! `protoc` (buf in production, same bytes) — protox's serializer drops
//! custom-option values, which are the entire subject under test. See
//! fixtures/descriptors/README.md for the regeneration commands.

use std::path::PathBuf;

use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};

use rushwind_redact::RedactPlan;

fn descriptors() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/descriptors")
}

fn read_set(rel: &str) -> Vec<u8> {
    std::fs::read(descriptors().join(rel)).expect("fixture descriptor set")
}

fn pool(rel: &str) -> DescriptorPool {
    DescriptorPool::decode(read_set(rel).as_slice()).expect("descriptor set decodes")
}

fn build_at(rel: &str) -> RedactPlan {
    RedactPlan::build(&pool(rel)).expect("plan builds")
}

fn new_msg(p: &DescriptorPool, fq: &str) -> DynamicMessage {
    DynamicMessage::new(p.get_message_by_name(fq).expect("message in pool"))
}

fn get(msg: &DynamicMessage, name: &str) -> Value {
    msg.get_field_by_name(name)
        .expect("field present")
        .into_owned()
}

fn set(msg: &mut DynamicMessage, name: &str, value: Value) {
    msg.set_field_by_name(name, value);
}

#[test]
fn plan_resolves_the_corpus_shapes() {
    let plan = build_at("fixture.binpb");
    // email + mobile + logins(item) + audit_notes(empty)
    assert_eq!(plan.message_rule_count("fixture.v1.User"), 4);
    // items(nested) + roster(map nested)
    assert_eq!(plan.message_rule_count("fixture.v1.ListUsersResponse"), 2);
    assert_eq!(plan.message_rule_count("fixture.v1.LoginRequest"), 1);
    // no annotated fields → no entry (the all-`Safe field` no-op)
    assert_eq!(plan.message_rule_count("fixture.v1.LoginResponse"), 0);

    assert!(plan.operation_skipped("/fixture.v1.UserService/Update"));
    assert!(plan.operation_skipped("fixture.v1.UserService/Update"));
    assert!(!plan.operation_skipped("/fixture.v1.UserService/List"));
    assert!(!plan.operation_skipped("/fixture.v1.AuthService/Login"));
}

#[test]
fn fixed_string_sets_present_empty() {
    let p = pool("fixture.binpb");
    let plan = build_at("fixture.binpb");
    let mut msg = new_msg(&p, "fixture.v1.LoginRequest");
    set(&mut msg, "username", Value::String("bob".into()));
    set(&mut msg, "password", Value::String("hunter2".into()));
    plan.apply_message("fixture.v1.LoginRequest", &mut msg);
    assert_eq!(get(&msg, "password"), Value::String(String::new()));
    // The reference assigns `x.Password = &""` — the field STAYS
    // present, so EmitUnpopulated emits `"password": ""`.
    assert!(msg.has_field_by_name("password"));
    assert_eq!(get(&msg, "username"), Value::String("bob".into()));

    // Absent → untouched (the reference's nil-pointer guard).
    let mut absent = new_msg(&p, "fixture.v1.LoginRequest");
    set(&mut absent, "username", Value::String("bob".into()));
    plan.apply_message("fixture.v1.LoginRequest", &mut absent);
    assert!(!absent.has_field_by_name("password"));
}

#[test]
fn mask_and_email_follow_the_generated_helpers() {
    let p = pool("fixture.binpb");
    let plan = build_at("fixture.binpb");

    let mut msg = new_msg(&p, "fixture.v1.User");
    set(&mut msg, "mobile", Value::String("13812345678".into()));
    set(
        &mut msg,
        "email",
        Value::String("zhangsan@example.com".into()),
    );
    plan.apply_message("fixture.v1.User", &mut msg);
    // _redactMask("13812345678", 3, 4, "*") → 138 + **** + 5678
    assert_eq!(get(&msg, "mobile"), Value::String("138****5678".into()));
    // _redactEmail("zhangsan@example.com", 2, false, "*") → zh******@example.com
    assert_eq!(
        get(&msg, "email"),
        Value::String("zh******@example.com".into())
    );

    // `len(s) <= keep_first + keep_last` leaves the value unchanged.
    let mut short = new_msg(&p, "fixture.v1.User");
    set(&mut short, "mobile", Value::String("1234567".into()));
    plan.apply_message("fixture.v1.User", &mut short);
    assert_eq!(get(&short, "mobile"), Value::String("1234567".into()));

    // No `@` leaves the email unchanged.
    let mut bare = new_msg(&p, "fixture.v1.User");
    set(&mut bare, "email", Value::String("not-an-email".into()));
    plan.apply_message("fixture.v1.User", &mut bare);
    assert_eq!(get(&bare, "email"), Value::String("not-an-email".into()));

    // Short local part keeps its head without masking.
    let mut tiny = new_msg(&p, "fixture.v1.User");
    set(&mut tiny, "email", Value::String("ab@x.io".into()));
    plan.apply_message("fixture.v1.User", &mut tiny);
    assert_eq!(get(&tiny, "email"), Value::String("ab@x.io".into()));

    // Untouched fields absent → stay absent.
    let mut minimal = new_msg(&p, "fixture.v1.User");
    set(&mut minimal, "username", Value::String("bob".into()));
    plan.apply_message("fixture.v1.User", &mut minimal);
    assert!(!minimal.has_field_by_name("mobile"));
    assert!(!minimal.has_field_by_name("email"));
}

#[test]
fn element_item_masks_each_scalar() {
    let p = pool("fixture.binpb");
    let plan = build_at("fixture.binpb");
    let mut msg = new_msg(&p, "fixture.v1.User");
    set(
        &mut msg,
        "logins",
        Value::List(vec![
            Value::String("10.0.0.1".into()),
            Value::String("192.168.1.15".into()),
        ]),
    );
    plan.apply_message("fixture.v1.User", &mut msg);
    assert_eq!(
        get(&msg, "logins"),
        Value::List(vec![
            Value::String("10****.1".into()),
            Value::String("19********15".into()),
        ])
    );

    // `element = { empty: true }` clears the whole container.
    set(
        &mut msg,
        "audit_notes",
        Value::List(vec![Value::String("note".into())]),
    );
    plan.apply_message("fixture.v1.User", &mut msg);
    assert!(!msg.has_field_by_name("audit_notes"));
}

#[test]
fn element_nested_recurses_into_items_and_map_values() {
    let p = pool("fixture.binpb");
    let plan = build_at("fixture.binpb");

    let mut item = new_msg(&p, "fixture.v1.User");
    set(&mut item, "username", Value::String("bob".into()));
    set(&mut item, "mobile", Value::String("13812345678".into()));
    let mut roster_item = new_msg(&p, "fixture.v1.User");
    set(
        &mut roster_item,
        "email",
        Value::String("zhangsan@example.com".into()),
    );

    let mut response = new_msg(&p, "fixture.v1.ListUsersResponse");
    set(&mut response, "total", Value::U64(2));
    set(
        &mut response,
        "items",
        Value::List(vec![Value::Message(item)]),
    );
    set(
        &mut response,
        "roster",
        Value::Map(
            [(MapKey::String("bob".into()), Value::Message(roster_item))]
                .into_iter()
                .collect(),
        ),
    );
    plan.apply_message("fixture.v1.ListUsersResponse", &mut response);

    let Value::List(items) = get(&response, "items") else {
        panic!("items is a list");
    };
    let Value::Message(redacted) = &items[0] else {
        panic!("items hold messages");
    };
    assert_eq!(
        redacted
            .get_field_by_name("mobile")
            .expect("present")
            .as_ref(),
        &Value::String("138****5678".into())
    );
    assert_eq!(get(&response, "total"), Value::U64(2));

    let Value::Map(roster) = get(&response, "roster") else {
        panic!("roster is a map");
    };
    let Value::Message(map_value) = &roster[&MapKey::String("bob".into())] else {
        panic!("roster values are messages");
    };
    assert_eq!(
        map_value
            .get_field_by_name("email")
            .expect("present")
            .as_ref(),
        &Value::String("zh******@example.com".into())
    );
}

#[test]
fn unsupported_vocabulary_fails_the_build() {
    for case in [
        "regex",
        "hash",
        "condition",
        "message_apply",
        "kind_mismatch",
        "element_on_scalar",
        "nested_on_scalar",
    ] {
        let plan = RedactPlan::build(&pool(&format!("cases/{case}.binpb")));
        let err = plan.expect_err(case);
        assert!(err.to_string().contains("bad.v1.Bad."), "{err}");
    }
}

#[test]
fn file_and_service_level_refusals_name_the_descriptor() {
    let err = RedactPlan::build(&pool("autodetect.binpb")).unwrap_err();
    assert!(err.to_string().contains("autodetect.proto"), "{err}");

    let err = RedactPlan::build(&pool("internal.binpb")).unwrap_err();
    assert!(err.to_string().contains("internal.v1.S"), "{err}");
}

#[test]
fn a_pool_without_the_schema_yields_an_empty_plan() {
    // The extension descriptors come from the pool itself; a compile
    // closure without the vendored redact.proto builds an empty plan.
    let plan = build_at("bare.binpb");
    assert_eq!(plan.message_rule_count("bare.v1.B"), 0);
    assert!(!plan.operation_skipped("/bare.v1.Nope/Get"));
}
