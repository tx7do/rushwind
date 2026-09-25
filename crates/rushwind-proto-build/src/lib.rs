//! The contract build engine behind the deployment proto crates — the
//! build.rs both downstream repos ran by copy-paste, extracted once.
//!
//! Two faces of the contract, per the deployments' settled shape:
//!
//! * the **annotated descriptor set** — the FULL compile closure
//!   including the annotation declarations (google.api.http,
//!   errors.code, redact, validate, gnostic) — produced by `buf build`
//!   over the api workspace (NOT protox: protox's serializer drops
//!   custom-option bytes, which are the entire point of this set).
//!   Requires `buf` on PATH.
//! * the **Rust types** — prost + pbjson from a FILTERED descriptor set
//!   built by protox: only data-carrying files (the contract tree's
//!   own top-level modules, the vendored data files, the well-known
//!   types referenced as field types). Annotation-only files are
//!   dropped so prost/pbjson never emit types for them.
//!
//! Then the [`rushwind_gen_http`] route/binding/trait/mount surface —
//! once per configured [`Face`], each optionally sliced from the
//! annotated closure by [`slice::sub_closure`]'s byte-verbatim
//! reachability walk and re-pointed from `crate::gen::` to the face's
//! own module.
//!
//! A deployment build.rs reduces to a [`Build`] value: the manifest dir,
//! the vendored data files, the auth-free whitelist, the gen config, and
//! the face list.

pub mod slice;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use protox::prost::Message as _;

/// The deployment's contract build: everything the engine needs from
/// the caller, expressed as config.
pub struct Build {
    /// The proto crate's own directory (`env!("CARGO_MANIFEST_DIR")` in
    /// the deployment's build.rs) — the engine derives the backend root,
    /// `api/protos` and `api/third_party` from it.
    pub manifest_dir: PathBuf,
    /// Vendored data-carrying files (relative to `api/third_party`) —
    /// real runtime types (the paging envelope, HttpBody) that
    /// participate in type generation.
    pub vendored_data_files: &'static [&'static str],
    /// The deployment's auth-free whitelist — single-sourced from the
    /// crate's `src/auth_free.rs`, included at build-script scope.
    pub auth_free: &'static [(&'static str, &'static str)],
    /// The generated mounts' redaction-plan expression (`None` threads
    /// a literal `None` — no static redaction).
    pub redact_plan_expr: Option<&'static str>,
    /// The generated types' Rust module path (the deployments' generated
    /// types resolve through `crate::proto`).
    pub proto_module_path: &'static str,
    /// The descriptor-pool expression the mounts hand the glue.
    pub pool_expr: &'static str,
    /// The tonic service generator rides the type pass (domain gRPC
    /// server/client faces beside the message types). Requires the
    /// crate's `tonic` feature.
    pub tonic: bool,
    /// The gen faces: one entry per output module.
    pub faces: &'static [Face],
}

/// One generated route/binding/trait/mount module.
pub struct Face {
    /// Diagnostics label (`admin`, `app`, …).
    pub label: &'static str,
    /// The output file name inside `OUT_DIR` (`admin_gen.rs`, …).
    pub module_file: &'static str,
    /// The root prefix slicing the annotated closure to this face's
    /// sub-closure (`admin/service/v1/`); `None` generates over the
    /// full closure.
    pub root_prefix: Option<&'static str>,
    /// The generated self-references (`crate::gen::` — the generator's
    /// hardcoded module path) are rewritten to this path when present;
    /// `None` keeps `crate::gen::` (the single-face deployment's own
    /// module).
    pub module_path: Option<&'static str>,
}

/// Runs the full contract build. See the crate docs for the shape.
pub fn run(build: Build) -> Result<(), Box<dyn std::error::Error>> {
    if build.tonic && !cfg!(feature = "tonic") {
        return Err("tonic faces requested but the `tonic` feature is off".into());
    }
    // The proto crate sits at backend/crates/proto — two levels up is
    // backend/, and the api workspace buf runs in sits beside it.
    let backend_root = build.manifest_dir.join("../..");
    let proto_root = backend_root.join("api/protos");
    let third_party_root = backend_root.join("api/third_party");

    // Contract files + vendored data files: the codegen request set.
    let mut compile_files: Vec<PathBuf> = Vec::new();
    collect_protos(&proto_root, &mut compile_files)?;
    if compile_files.is_empty() {
        return Err(format!("no contract protos found under {}", proto_root.display()).into());
    }
    for rel in build.vendored_data_files {
        let p = third_party_root.join(rel);
        if !p.is_file() {
            return Err(format!("vendored data file missing: {}", p.display()).into());
        }
        compile_files.push(p);
    }

    // Canonical input order: directory enumeration order is filesystem-defined
    // (NTFS yields name order, ext4 hash order); the sort keeps the protox
    // types face deterministic across platforms. (The annotated closure no
    // longer depends on this list at all — buf build emits the whole
    // workspace in its own sorted order.)
    compile_files.sort();

    // The contract tree's own top-level modules — derived from the tree
    // itself so a new module directory needs no whitelist edit. The types
    // filter keeps a file when its top-level segment is one of these
    // modules, or it is an explicitly vendored data file, or a well-known
    // import.
    let mut contract_tops: Vec<String> = fs::read_dir(&proto_root)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    contract_tops.sort();
    let is_types_kept = |name: &str| -> bool {
        match name.split_once('/') {
            Some((top, _)) => {
                contract_tops.iter().any(|t| t == top)
                    || build.vendored_data_files.contains(&name)
                    || name.starts_with(WELL_KNOWN_PREFIX)
            }
            None => {
                build.vendored_data_files.contains(&name) || name.starts_with(WELL_KNOWN_PREFIX)
            }
        }
    };

    let includes = [proto_root.as_path(), third_party_root.as_path()];

    // 1. The annotated full closure via buf build (option bytes preserved,
    //    imports resolved through the api/buf.yaml workspace). Output
    //    ordering is buf-determined (sorted), keeping the generated ROUTES
    //    table platform-independent.
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let annotated_path = out_dir.join("annotated_descriptor.bin");
    let mut cmd = Command::new("buf");
    cmd.current_dir(backend_root.join("api"))
        .args(["build", "--exclude-source-info", "--output"])
        .arg(&annotated_path);
    let output = cmd.output().map_err(|e| format!("buf: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "buf build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let annotated_size = fs::metadata(&annotated_path).map(|m| m.len()).unwrap_or(0);

    // 2. The types face via protox: the full closure (option bytes unused
    //    here) filtered to data-carrying files.
    let full_fds = protox::compile(&compile_files, includes)?;

    // Filtered set for type generation: drop annotation-only files, prune
    // dependency edges to kept files.
    let mut types_fds = full_fds.clone();
    types_fds.file.retain(|f| is_types_kept(f.name()));
    for f in &mut types_fds.file {
        let deps: Vec<String> = f.dependency.to_vec();
        f.dependency = deps
            .into_iter()
            .filter(|d| is_types_kept(d.as_str()))
            .collect();
    }

    // Filtered closure → types_descriptor.bin (prost + pbjson input).
    let types_path = out_dir.join("types_descriptor.bin");
    fs::write(&types_path, types_fds.encode_to_vec())?;

    // Sweep stale codegen artifacts (earlier runs may have written packages
    // that the filter has since dropped).
    for entry in fs::read_dir(&out_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let _ = fs::remove_file(&path);
        }
    }

    eprintln!(
        "[proto-build] annotated closure: {annotated_size} bytes (buf build); types set: {} files (annotation declarations dropped)",
        types_fds.file.len()
    );

    // prost type generation, reading the FILTERED descriptor set verbatim.
    // skip_protoc_run is essential: without it prost-build would invoke
    // protoc on the request files and OVERWRITE types_descriptor.bin with
    // the full closure, resurrecting the annotation packages.
    let mut config = prost_build::Config::new();
    config
        .file_descriptor_set_path(&types_path)
        .skip_protoc_run()
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .include_file("proto_include.rs");
    if build.tonic {
        #[cfg(feature = "tonic")]
        tonic_generator(&mut config);
    }
    config.compile_protos(&compile_files, &includes)?;

    // pbjson serde impls: data packages of the filtered set only. The package
    // list is derived from the set itself (dir names do not map 1:1 to
    // package names); well-knowns are externed and excluded.
    let mut packages: Vec<String> = types_fds
        .file
        .iter()
        .filter(|f| !is_well_known(f.name()))
        .map(|f| format!(".{}", f.package()))
        .collect();
    packages.sort();
    packages.dedup();
    let pkg_refs: Vec<&str> = packages.iter().map(|s| s.as_str()).collect();
    pbjson_build::Builder::new()
        .register_descriptors(&fs::read(&types_path)?)?
        .build(&pkg_refs)?;

    // 3. The generated route/binding/trait/mount surface: the framework
    //    generator (rushwind-gen-http) over the annotated closure — once
    //    per configured face.
    let annotated = fs::read(&annotated_path)?;
    let cfg = rushwind_gen_http::CodegenConfig {
        proto_module_path: build.proto_module_path,
        pool_expr: build.pool_expr,
        auth_free: build.auth_free,
        redact_plan_expr: build.redact_plan_expr,
    };
    for face in build.faces {
        let bytes = match face.root_prefix {
            Some(prefix) => {
                let sub = slice::sub_closure(&annotated, prefix).map_err(|e| e.to_string())?;
                eprintln!(
                    "[proto-build] {} face sub-closure: {} bytes (full: {})",
                    face.label,
                    sub.len(),
                    annotated.len()
                );
                sub
            }
            None => annotated.clone(),
        };
        let src = match rushwind_gen_http::generate_from_bytes(&bytes, &cfg) {
            Ok(src) => src,
            Err(e) => panic!("gen-rust code generation failed ({} face): {e}", face.label),
        };
        let src = match face.module_path {
            Some(module) => src.replace("crate::gen::", module),
            None => src,
        };
        fs::write(out_dir.join(face.module_file), src)?;
    }

    // Re-run on any contract change (and on the auth-free table).
    println!(
        "cargo:rerun-if-changed={}",
        build.manifest_dir.join("src/auth_free.rs").display()
    );
    println!("cargo:rerun-if-changed={}", third_party_root.display());
    println!("cargo:rerun-if-changed={}", proto_root.display());

    Ok(())
}

/// The tonic service generator — domain gRPC server/client faces beside
/// the message types, one type tree, zero duplication (feature-gated:
/// the deployment opts in via the crate's `tonic` feature).
#[cfg(feature = "tonic")]
fn tonic_generator(config: &mut prost_build::Config) {
    config.service_generator(
        tonic_prost_build::configure()
            .build_server(true)
            .build_client(true)
            // The trait methods default to Unimplemented — the core
            // deployment overrides them service by service.
            .generate_default_stubs(true)
            .service_generator(),
    );
}

/// Well-known types referenced as field types by the contract tree.
/// Externed to `pbjson_types` by the prost config; kept in the filtered
/// descriptor set so references resolve, excluded from pbjson output.
const WELL_KNOWN_PREFIX: &str = "google/protobuf/";

fn is_well_known(name: &str) -> bool {
    name.starts_with(WELL_KNOWN_PREFIX)
}

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_protos(&path, out)?;
        } else if path.extension().map(|e| e == "proto").unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(())
}
