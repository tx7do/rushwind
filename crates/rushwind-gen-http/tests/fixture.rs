//! Generator fixture test: a minimal annotated proto, compiled by protoc
//! (options preserved — protox would drop them), must produce the exact
//! route table row, error-table entry, binding-plan leaves, trait
//! signature, pool threading and auth-free split the real corpora rely
//! on. The vendored third_party under tests/ carries the annotation and
//! error-extension declarations the fixture compiles against.

use std::path::PathBuf;
use std::process::Command;

use rushwind_gen_http::{generate_from_bytes, CodegenConfig};

const FIXTURE: &str = r#"
syntax = "proto3";
package fixture.v1;
import "google/api/annotations.proto";
import "errors/errors.proto";
message Thing { string name = 1; }
message GetThingRequest { oneof query_by { string id = 1; } }
message ListThingsRequest { string name = 1; }
message ListThingsResponse { repeated Thing items = 1; }
enum FixtureErrorReason {
  option (errors.default_code) = 400;
  FOO = 0 [(errors.code) = 404];
}
service FixtureService {
  rpc Get (GetThingRequest) returns (Thing) {
    option (google.api.http) = { get: "/fixture/v1/things/{id}" };
  }
  rpc List (ListThingsRequest) returns (ListThingsResponse) {
    option (google.api.http) = { post: "/fixture/v1/things/list", body: "*" };
  }
}
"#;

fn compile_fixture() -> Vec<u8> {
    // Unique per invocation: the fixture tests run in parallel.
    static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmp = std::env::temp_dir().join(format!(
        "rushwind-gen-fixture-{}-{seq}.proto",
        std::process::id()
    ));
    std::fs::write(&tmp, FIXTURE).unwrap();
    let out = std::env::temp_dir().join(format!(
        "rushwind-gen-fixture-{}-{seq}.bin",
        std::process::id()
    ));
    let third_party = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/third_party");
    let status = Command::new("protoc")
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg("-I")
        .arg(tmp.parent().unwrap())
        .arg("-I")
        .arg(&third_party)
        .arg(&tmp)
        .status()
        .expect("protoc on PATH (the generator's descriptor inputs carry option bytes)");
    let bytes = std::fs::read(&out).unwrap_or_default();
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(&out);
    assert!(
        status.success() && !bytes.is_empty(),
        "protoc fixture compile"
    );
    bytes
}

/// The fixture deployment's knobs: contract types under a fictional
/// module path, the pool reached through a fictional accessor, the
/// List operation whitelisted (Get stays gated), and the redaction
/// plan reached through a fictional accessor.
fn config() -> CodegenConfig<'static> {
    CodegenConfig {
        proto_module_path: "fixture_proto::proto",
        pool_expr: "fixture_proto::pool()",
        auth_free: &[("fixture.v1.FixtureService", "List")],
        redact_plan_expr: Some("fixture_proto::redact_plan()"),
    }
}

#[test]
fn no_redact_plan_emits_a_literal_none() {
    let bytes = compile_fixture();
    let cfg = CodegenConfig {
        proto_module_path: "fixture_proto::proto",
        pool_expr: "fixture_proto::pool()",
        auth_free: &[],
        redact_plan_expr: None,
    };
    let src = generate_from_bytes(&bytes, &cfg).unwrap();
    let handles = src.matches("rushwind_http_binding::glue::handle(").count();
    assert!(handles > 0, "handle calls emitted");
    assert_eq!(
        src.matches("\n                    None,\n").count(),
        handles,
        "every call site threads a literal None"
    );
}

#[test]
fn fixture_route_error_table_trait_and_config_threading() {
    let bytes = compile_fixture();
    let src = generate_from_bytes(&bytes, &config()).unwrap();

    // Route row: path template verbatim, path var extracted, operation id,
    // input/output names, no body.
    assert!(
        src.contains(r#"path: "/fixture/v1/things/{id}","#),
        "route path"
    );
    assert!(src.contains("path_vars: &[\"id\"],"), "path var");
    assert!(
        src.contains("operation_id: \"/fixture.v1.FixtureService/Get\","),
        "operation id"
    );
    assert!(
        src.contains("input_fq: \"fixture.v1.GetThingRequest\","),
        "input fq"
    );
    assert!(
        src.contains("output_fq: \"fixture.v1.Thing\","),
        "output fq"
    );
    assert!(src.contains("body_star: false,"), "no body");

    // Body route.
    assert!(
        src.contains(r#"path: "/fixture/v1/things/list","#),
        "list path"
    );
    assert!(src.contains("body_star: true,"), "body star");

    // Error table entry from the (errors.code) annotation.
    assert!(
        src.contains(r#"("fixture.v1", "FOO", 404i32),"#),
        "error table"
    );

    // Binding plans: oneof member leaf records its oneof; the plain scalar
    // leaf records its json spelling.
    assert!(
        src.contains(r#"path_json: "id", path_proto: "id", kind: LeafKind::Str, repeated: false, map: None, oneof: Some("query_by")"#),
        "oneof leaf"
    );
    assert!(
        src.contains(r#"path_json: "name", path_proto: "name", kind: LeafKind::Str, repeated: false, map: None, oneof: None"#),
        "plain scalar leaf"
    );

    // Trait: one method per annotated proto method, typed through the
    // configured contract-module path, against the framework envelope.
    assert!(
        src.contains("pub trait FixtureServiceHandlers: Send + Sync {"),
        "trait"
    );
    assert!(
        src.contains("async fn get(&self, ctx: rushwind_http_binding::ctx::RequestContext, req: fixture_proto::proto::fixture::v1::GetThingRequest) -> Result<fixture_proto::proto::fixture::v1::Thing, rushwind_http_binding::envelope::StatusError>;"),
        "get signature"
    );

    // The lifecycle tail is the framework glue, with the configured pool
    // expression threaded first and the wire facts in the framework's
    // wire module.
    assert!(
        src.contains("                rushwind_http_binding::glue::handle("),
        "glue call"
    );
    assert!(
        src.contains("                    fixture_proto::pool(),"),
        "pool expression threaded"
    );
    assert!(
        src.contains("                    Some(fixture_proto::redact_plan()),"),
        "redaction plan threaded"
    );
    assert!(
        src.contains("                let wire = rushwind_http_binding::wire::RouteWire {"),
        "framework wire facts"
    );
    assert!(
        src.contains("        let bind_wire = rushwind_http_binding::wire::RouteWire {"),
        "bind wire facts"
    );

    // Mount emitters reference both routes; the whitelisted List lands on
    // the public router with a non-gated wrap, Get on the gated one.
    assert!(src.contains("fn mount_route_0(r: Router"), "mount 0");
    assert!(src.contains("fn mount_route_1(r: Router"), "mount 1");
    assert!(
        src.contains("pub fn mount_fixture_service(router_pub: Router, router_gate: Router"),
        "mount fn"
    );
    assert!(
        src.contains("        r_gate = mount_route_0(r_gate, Arc::clone(&svc), wrap);"),
        "route 0 gated"
    );
    assert!(
        src.contains("        r_pub = mount_route_1(r_pub, Arc::clone(&svc), wrap);"),
        "route 1 public"
    );
    assert!(
        src.contains("        let mr = wrap(axum::routing::on(m, h), &bind_wire, false);"),
        "wrap composition, whitelisted"
    );
    assert!(
        src.contains("        let mr = wrap(axum::routing::on(m, h), &bind_wire, true);"),
        "wrap composition, gated"
    );
    assert!(
        !src.contains("admin_"),
        "no admin paths leak into generic output"
    );

    // Null placeholder: struct, impl and constructor, every method
    // answering the Unknown error shape.
    assert!(
        src.contains("pub struct NullFixtureService;"),
        "null struct"
    );
    assert!(
        src.contains("impl crate::gen::services::FixtureServiceHandlers for NullFixtureService"),
        "null impl"
    );
    // The closing sequence between the last method and the constructor:
    // method brace, impl brace, ctor — a dropped impl brace once broke the
    // full corpus build while the two-method fixture passed.
    assert!(
        src.contains("        }\n    }\n    pub fn null_fixture_service()"),
        "null impl closing braces"
    );
    assert!(
        src.contains(
            "pub fn null_fixture_service() -> Arc<dyn crate::gen::services::FixtureServiceHandlers>"
        ),
        "null ctor"
    );
    assert!(
        src.contains("Err(rushwind_http_binding::envelope::internal_error(\"not implemented\"))"),
        "null body"
    );
}
