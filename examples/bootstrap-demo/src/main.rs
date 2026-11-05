//! Config-driven assembly demo.
//!
//! One YAML document assembles a storage engine (memory, with the schema
//! captured in the factory closure), a CRUD HTTP edge over it, and an
//! HTTP server with two route packs, all under the RushWind lifecycle.
//! `GET /health` is static; `GET /wired` proves the configured storage
//! reached the route pack; `POST/GET /widgets` is the storage line's
//! working CRUD edge.
//!
//! The crud edge is **deny by default**: without a viewer policy every
//! request sees nothing. The demo opts into `viewer: all` explicitly —
//! a real deployment does NOT do this; it registers its own `"crud"`
//! api pack that derives a `Viewer` from the request's authenticated
//! claims (see `Bootstrap::api_pack` and `CrudApi::with_viewer`).
//!
//! Try it: `cargo run -p bootstrap-demo`, then
//! `curl http://<printed-endpoint>/health` and `/wired`, then Ctrl+C.
//!
//! File-based configs work the same way: `Bootstrap::from_yaml_path`.

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use rushwind_bootstrap::{Bootstrap, BootstrapError, RouteInput, RouteSurface};
use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_transport::StopSignal;

const CONFIG: &str = r#"
app:
  name: bootstrap-demo
  version: v0.1.0
  stop_timeout_secs: 5
storage:
  engine: memory
  settings: {}
storage_endpoints:
  - nest: /widgets
    api: crud
    # Deny by default without this line — the demo is all-access on
    # purpose. A real deployment omits `viewer` and derives the Viewer
    # from authn claims via its own api pack (Bootstrap::api_pack).
    viewer: all
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: health
      - name: wired
"#;

/// The application schema — captured by the storage factory, because the
/// schema is application knowledge, not configuration.
fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .build()
        .expect("schema is valid")
}

fn assemble() -> Result<Bootstrap, BootstrapError> {
    let bootstrap = Bootstrap::from_yaml_str(CONFIG)?.storage_factory("memory", |_settings| {
        Box::pin(async move {
            let repo =
                MemoryRepo::new(schema()).map_err(|e| BootstrapError::Failed(e.to_string()))?;
            Ok(Arc::new(repo) as Arc<dyn Repository>)
        })
    });
    Ok(bootstrap
        .route_pack("health", |_, _| {
            Ok(RouteSurface::new(
                Router::new().route("/health", get(|| async { "ok" })),
            ))
        })
        .route_pack("wired", |_settings, input: RouteInput| {
            // Proves the configured storage reaches route packs.
            let repository = input.repository.clone();
            Ok(RouteSurface::new(Router::new().route(
                "/wired",
                get(move || {
                    let repository = repository.clone();
                    async move { format!("repo={}", repository.is_some()) }
                }),
            )))
        }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrapped = assemble()?.build().await?;
    for endpoint in &bootstrapped.endpoints {
        println!("[demo] serving on {endpoint}");
    }
    println!(
        "[demo] storage configured: {}; crud edge at /widgets",
        bootstrapped.repository.is_some()
    );

    let result = bootstrapped.app.run(StopSignal::new()).await;
    println!("[demo] lifecycle finished: {result:?}");
    Ok(())
}
