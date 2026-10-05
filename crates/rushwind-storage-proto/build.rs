//! Code generation for the storage wire contract.
//!
//! protox compiles `proto/rushwind/storage/v1/query.proto` (repo root — the
//! contract source of truth, shared across languages) into a file descriptor
//! set; prost generates the Rust types; pbjson generates the protojson serde
//! impls. Well-known types are externed to `pbjson_types` so
//! `google.protobuf.Value` and `FieldMask` carry protojson semantics.
//!
//! The proto tree lives outside the crate directory for cross-language
//! sharing, which is why this crate is `publish = false`; when publishing
//! becomes real, the proto files move into the crate.

use std::fs;
use std::path::PathBuf;

use protox::prost::Message as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("proto");
    let query_proto = proto_root.join("rushwind/storage/v1/query.proto");

    println!("cargo:rerun-if-changed={}", query_proto.display());

    let file_descriptors = protox::compile([query_proto.as_path()], [proto_root.as_path()])?;

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("descriptor.bin");
    fs::write(&descriptor_path, file_descriptors.encode_to_vec())?;

    let mut config = prost_build::Config::new();
    config
        .file_descriptor_set_path(&descriptor_path)
        // Generate from protox's descriptor set verbatim — without this,
        // prost-build would invoke protoc on the request files (and
        // overwrite the descriptor set with protoc's own output),
        // dragging a system protoc into the build for nothing.
        .skip_protoc_run()
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types");
    config.compile_protos(&[query_proto.as_path()], &[proto_root.as_path()])?;

    pbjson_build::Builder::new()
        .register_descriptors(&fs::read(&descriptor_path)?)?
        .build(&["."])?;

    Ok(())
}
