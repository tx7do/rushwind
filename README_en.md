<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

**English** | [中文](./README.md) | [日本語](./README_ja.md)

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.81+-DEA584?logo=rust)](https://www.rust-lang.org/)
[![CI](https://github.com/tx7do/rushwind/actions/workflows/ci.yml/badge.svg)](https://github.com/tx7do/rushwind/actions/workflows/ci.yml)

**The Rust microservice framework — core and the full plugin fleet, in one repository.**

</div>

---

## What RushWind is

Most ecosystems split framework and plugins across repositories: the framework repo handles lifecycle and contracts, while the "plugins" — transports, storage, messaging — live in separate repos whose versions drift and whose gates differ. RushWind is two-in-one: **the framework core and every plugin domain evolve in the same workspace**, under the same conformance-suite standard and the same acceptance bar — a green `cargo test` is the definition of conformant.

- **The framework half**: a lifecycle-orchestration core + the transport contract layer + nine protocol-stack adapters (axum / WebSocket / QUIC / WebTransport / HTTP/3 / gRPC / MQTT / SSE / cron).
- **The plugin half**: 20 capability domains — storage, vector retrieval, security (authn / authz / password hashing), config, registry, observability (metrics / tracing / health), resilience (retry / rate limiting / circuit breaking), cache, distributed locks, messaging, transactions, task queues, encoding, scripting, AI, object storage.

Two-in-one does not mean batteries-included — the single repo is a development view, not a link view: engines you don't use never enter your dependency tree, and domains connect only through their contract crates. The core still contains no logging, no registry, no config center: those are bricks, not the baseplate.

## Design philosophy

> **Not a batteries-included framework. A box of bricks.**

The framework core does exactly one thing: **reliable multi-server lifecycle orchestration** — concurrent startup, cascading shutdown, per-phase time budgets, observable outcomes. Contracts first: every capability domain is a contract crate; every concrete engine and every protocol stack is a separate adapter crate that the user snaps in as needed.

Everything is designed native to Rust's ownership, cancellation, and error models; normative details of contract semantics, budgets, and shutdown behavior live in [docs/architecture.md](./docs/architecture.md) (Chinese). The data-access layer extends the same philosophy: `rushwind-storage` drives many storage engines with one Repository contract — one adapter crate per engine, pulled in as needed.

## Quick start

Four crates are needed: `rushwind-core`, `rushwind-transport`, `rushwind-transport-axum`, `rushwind-http` (plus tokio and axum). Thirty lines get the first HTTP service with the unified middleware stack running:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_core::App;
use rushwind_http::HttpEdge;
use rushwind_transport::StopSignal;
use rushwind_transport_axum::AxumServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // HttpEdge: the default request middleware stack (panic recovery / request id / access log)
    let router = HttpEdge::new().wrap(
        Router::new().route("/hello", get(|| async { "hello rushwind" })),
    );
    let server = AxumServer::new(SocketAddr::from(([127, 0, 0, 1], 0)), router)?;
    println!("listening on {}", server.endpoint()?);

    // App: the orchestrator — concurrent startup, cascading shutdown, per-phase budgets
    let app = App::builder()
        .name("hello-rushwind")
        .version("0.1.0")
        .erased_server(Arc::new(server))
        .stop_timeout(Duration::from_secs(5))
        .build();

    // Ctrl+C / SIGTERM / app.stop() all land in a graceful shutdown
    app.run(StopSignal::new()).await?;
    Ok(())
}
```

The full walkthrough (including the three lifecycle primitives `Server` / `StopSignal` / `App`) is tutorial [01 · Lifecycle and the first service](./docs/tutorials/01-hello-rushwind.md) (Chinese); the whole 13-chapter series is at [docs/tutorials](./docs/tutorials/README.md). Runnable examples live in [examples/](./examples). As the project grows there is no hand-written assembly: `rushwind-bootstrap` assembles every domain's engines, the HTTP server, and the cron transport from one YAML into a single lifecycle (tutorial [02 · The assembler and configuration](./docs/tutorials/02-bootstrap-and-config.md) (Chinese), full example [`examples/bootstrap-demo`](./examples/bootstrap-demo)).

## Reference implementations and ecosystem

Three official repositories beyond the framework form the RushWind development chain. The two deployment shapes — monolith and microservices — each have a runnable-from-zero reference implementation:

| Repository | Role |
|:---|:---|
| [rushwind-toolkit](https://github.com/tx7do/rushwind-toolkit) | The ecosystem toolbox: the `rush` CLI + the `rush-gen` core library — full-chain domain-entity backend generation, database introspection that reverse-derives entity specs, frontend CRUD page generation, project scaffolding (memory/sqlite/postgres variants), contract manifest verification, differential testbeds |
| [rushwind-admin](https://github.com/tx7do/rushwind-admin) | The monolith reference implementation: a contract-driven enterprise admin backend — a single backend (REST + SSE) serving three frontends (Vue3 Vben / Vue3 Element Plus / React19), with multi-tenant RBAC, audit retention, TOTP MFA, and MLPS 2.0 compliance |
| [rushwind-cms](https://github.com/tx7do/rushwind-cms) | The minimal three-service microservice reference implementation: a Rust full-stack headless CMS — core-service (the domain layer, gRPC inward) + the admin / app BFFs (REST + SSE outward), one proto contract producing both the REST and gRPC faces |

## Repository layout

The first two sections are the framework (orchestration + transport contracts + the HTTP face); everything after is a plugin domain — one contract crate per domain + an engine matrix pulled in as needed, each passing the conformance suite.

### Core and transports

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-core` | Lifecycle orchestration: concurrent startup, cascading shutdown, per-phase time budgets, observable outcomes |
| `crates/rushwind-transport` | Contract layer: the `Server` trait, `StopSignal`, `Instance`, `ServerError` |
| `crates/rushwind-transport-axum` | axum adapter: plugs a `Router` into the lifecycle; graceful-shutdown mapping in the architecture doc |
| `crates/rushwind-transport-ws` | WS session router builder: gate chain + admission policy + session shutdown bus; contract in [session-middleware.md](./docs/session-middleware.md) (Chinese) |
| `crates/rushwind-transport-quic` | QUIC adapter: quinn accept loop in the lifecycle, full session chain (peer-address admission, per-remote-IP session quota); `stop()` is a real release (Endpoint::close) |
| `crates/rushwind-transport-webtransport` | WebTransport adapter: wtransport endpoint in the lifecycle, full session chain (HTTP-family gate chain at session request time, atomic admission, handshake deadline); `stop()` is a real release |
| `crates/rushwind-transport-h3` | HTTP/3 adapter: h3/h3-quinn request service in the lifecycle, gate chain at request time (refusal maps to a status response) and connection-level atomic admission; `stop()` is a real release |
| `crates/rushwind-transport-grpc` | gRPC transport: tonic 0.14 serving caller-supplied `Routes` (proto-build's tonic generator compiles domain services; the deployment overrides stubs), eager bind at listener construction, graceful drain on the stop signal, endpoint `grpc://host:port` |
| `crates/rushwind-transport-mqtt` | MQTT consumption bridge: subscribes to an external broker (per-subscription handler registration + canonical wildcard dispatch), reconnect backoff + subscription rebuild, serial pump into the handler |
| `crates/rushwind-transport-sse` | SSE notification transport: the `/events` wire contract (CORS preflight, three-source token, 401 plain-text error line, anti-cross-user stream check, per-user filtered broadcast stream) + in-process Hub + bootstrap factory; authorization pluggable via the `Gate` trait |
| `crates/rushwind-transport-cron` | cron transport: a minute-aligned periodic scheduler inside the `Server` lifecycle, firing registered jobs whenever their cron spec matches the wall clock |

### HTTP face

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-http` | HTTP edge: the gRPC-aligned error envelope `HttpError` — code/reason/message/details, with built-in `AuthnError`/`StorageError` conversions; the request middleware stack (recovery / request-id / logging / CORS / timeout) and the `HttpEdge` assembler; `with_authn` / `with_authorization` wire the authn/authz contracts onto axum routes, with the `Authenticated` extractor; feature-gated `/healthz`+`/readyz` and `/metrics` mounts — design in [docs/http-edge.md](./docs/http-edge.md) (Chinese) |
| `crates/rushwind-http-binding` | proto-HTTP wire-format contract (caller supplies the descriptor pool): form binder (dot paths, two field-name spellings, map/list/oneof structure, well-known leaf whitelist), Content-Type codec resolution, pre-authn bind layer, protojson response codec, per-route lifecycle tail, four-field status error envelope |
| `crates/rushwind-gen-http` | Descriptor-driven proto-HTTP route-surface code generator: per-binding route table + form binding plan, (package, reason)→HTTP status error table, one trait + empty placeholder impl per service, public/gated mount emitters |
| `crates/rushwind-proto-build` | Contract build engine (build-dep): buf comment closure (option byte fidelity) + protox type-surface filtering + prost/pbjson (optional tonic) + gen-http face emission (byte-level sub-closure slicing); a deployment's build.rs shrinks to configuration |
| `crates/rushwind-redact` | Static response redaction: `(redact.v1)` options compile fail-closed into a plan; dynamic messages are rewritten in place before serialization (mask/email/fixed value/element nesting/method_skip), semantically anchored to protoc-gen-go-redact output |

### Storage domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-storage` | Storage contract: the `Repository` trait, three pagination modes (Page/Offset/Token), filter trees, the five-level Viewer tenancy model, FieldMask, audit hooks |
| `crates/rushwind-storage-memory` | In-memory reference engine: the semantic baseline for filters/sorting/cursors, zero dependencies |
| `crates/rushwind-storage-seaorm` | SeaORM engine: SQLite/PostgreSQL/MySQL backends all enabled, three-dialect SQL snapshots pin rendering, SQLite passes the conformance suite, the live suite runs in CI containers |
| `crates/rushwind-storage-seaorm-support` | Typed entity-service bridge: `PagingRequest` select assembly (query-syntax filtering/orderBy/three-state slicing, column-type-aware binding), SeaORM failure→envelope, entity time↔proto Timestamp |
| `crates/rushwind-storage-seaorm-migration` | Schema-migration machinery for SeaORM services: create-table statements derived from entity definitions, applied as tracked sea-orm-migration migrations |
| `crates/rushwind-storage-mongodb` | MongoDB engine: FilterExpr→BSON translation unit-tested offline, LIKE family compiled to escaped regexes, live suite in a CI container |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch engine: REST + refresh-on-write, `.keyword` exact matching, atomic bulk writes |
| `crates/rushwind-storage-opensearch` | OpenSearch engine: thin reuse of the ES wire format (wire-compatible) |
| `crates/rushwind-storage-cassandra` | Cassandra engine: bucket-pinned partitions + contract-side evaluator filtering, LWT atomic batch writes |
| `crates/rushwind-storage-influxdb` | InfluxDB engine: measurement as table, id as a series tag, InfluxQL deletes |
| `crates/rushwind-storage-clickhouse` | ClickHouse engine: SQL over HTTP, mutations_sync for read-your-writes, probe-based conflict detection |
| `crates/rushwind-storage-cache` | Cache-aside decorator: SingleFlight stampede merging, cache keys carry viewer scope, generation guards against stale backfill |
| `crates/rushwind-storage-soft-delete` | Soft-delete decorator: tombstone writes, filtered on every read path, restore/purge, engine-agnostic |
| `crates/rushwind-storage-observe` | Observability decorator: one `tracing` span per call (table/op/outcome); OTel export is the subscriber's choice |
| `crates/rushwind-storage-tree` | Tree queries: children/roots/ancestors/subtree, contract-level traversal + cycle detection, works with any engine |
| `crates/rushwind-storage-proto` | proto wire contract: generated from `proto/rushwind/storage/v1/query.proto` (prost + pbjson), 29-operator mapping + AIP text parsing |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive macros: DTO↔Record mapping generated at compile time; scalar widening, `#[record(as_text)]` enums, `#[record(with = "…")]` custom conversions, `#[record(rename)]` column names |
| `crates/rushwind-storage-axum` | HTTP endpoint layer: mount any Repository as CRUD routes, dual list-query entry points (protojson `q` / AIP `filter`), viewer hooks close the tenancy loop |

### Vector retrieval domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-vector` | Vector retrieval contract (RAG / semantic search): unified `VectorQuery`, score is always higher-is-similar (cosine `1−d` / L2 `1/(1+d)` / raw dot product), non-paginated TopK, dimension discipline, pgvector text codec — pure types and pure functions |
| `crates/rushwind-vector-memory` | In-memory reference engine: brute-force top-k + dimension pinning/refusal, the semantic baseline for external engine adapters, with a criterion benchmark (the search-cost baseline) |
| `crates/rushwind-vector-qdrant` | Qdrant engine: hand-written reqwest REST, no SDK — metric-suffixed named vectors (`{field}__cosine/euclidean/dot`, so server-side top-k is correct for every contract metric at a documented 3× storage cost; existing bare-field single-metric collections run in single-metric mode), dimension discipline read from server config, free-string ids via deterministic SHA-1→UUID mapping with the original id carried back in the payload |
| `crates/rushwind-vector-milvus` | Milvus engine: hand-written reqwest over the v2 RESTful API — three metric-suffixed collections (server-side top-k correct for every contract metric), dimension discipline from describe, cross-field deletes via a listCollections prefix scan |
| `crates/rushwind-vector-weaviate` | Weaviate engine: hand-written reqwest over REST batch + GraphQL nearVector — three metric-suffixed classes (CamelCase sanitization), an engine-side dimension registry (Weaviate's schema reports no dimensions), the dot face honestly declared as the order-preserving cosine face |
| `crates/rushwind-vector-neo4j` | Neo4j engine: hand-written reqwest over HTTP-transaction Cypher — three metric-suffixed labels + vector indexes (dimensions pinned server-side, error prose mapped to the typed mismatch), kNN queries, label-registry cross-field deletes; cosine similarity and raw dot product pass through |

### Security domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-authn` | Authentication contract: the `Authenticator` trait (extract/verify halves), the `AuthClaims` claim bundle, an error taxonomy, the `AuthenticationGate` chain admission layer — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authn-apikey` | API-key engine: static key set / per-key claims / verification callback |
| `crates/rushwind-authn-basicauth` | Basic-Auth engine: RFC 7617 credentials against a static user table or a verification callback |
| `crates/rushwind-authn-hmac` | HMAC engine: keyID.timestamp.signature verification with a clock-skew window |
| `crates/rushwind-authn-jwt` | JWT engine: minting and verification across the HS/RS/PS/ES/EdDSA families |
| `crates/rushwind-authn-oidc` | OpenID Connect engine: discovery + JWKS fetched at construction and cached per kid, verification local and synchronous (chain-eligible), refresh explicit or as a background task, iss/aud/exp strictly pinned, minting refused |
| `crates/rushwind-authn-noop` | Noop engine: admits everything, mints empty credentials |
| `crates/rushwind-authn-presharedkey` | Pre-shared-key engine: set-membership verification, minting as random draw |
| `crates/rushwind-authn-session` | Session engine: opaque session IDs + a pluggable SessionStore |
| `crates/rushwind-authn-gate` | Gate session stage: the `AccessTokenChecker` contract (Redis allowlist/blocklist) and the authenticate-then-check stage head, trace-id parsing, unsigned bearer sniffing; envelope rendering stays on the deployment's error table |
| `crates/rushwind-authz` | Authorization contract: the `Engine` trait (single decision + three batch filters), the Subject/Action/Resource/Project model, policy JSON interop — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authz-acl` | ACL engine: ordered allow/deny rules + wildcard matching, default deny, deny wins |
| `crates/rushwind-authz-rbac` | RBAC engine: role→permission and user→role tables, transitive inheritance with cycle detection |
| `crates/rushwind-authz-noop` | Noop engine: single decisions always pass, batch filters always empty |
| `crates/rushwind-password` | Password hashing contract: `PasswordHasher` in one PHC-string face — verification is driven by the parameters embedded in the existing hash, and `needs_rehash` is the other half of the upgrade loop |
| `crates/rushwind-password-argon2` | Argon2id engine: OWASP default cost (m=19456,t=2,p=1); the legacy face (argon2i/v16) verifies but reads as needing rehash |
| `crates/rushwind-password-bcrypt` | bcrypt engine: MCF string + constant-time verification; the 72-byte truncation is the algorithm's own behavior (docs and tests both say so honestly) |
| `crates/rushwind-password-scrypt` | scrypt engine: OWASP defaults (ln=17,r=8,p=1); existing hashes' embedded ln/r/p drive verification |
| `crates/rushwind-password-pbkdf2` | PBKDF2-HMAC-SHA256 engine: OWASP default 600k rounds; SHA-512 verification allowed, SHA-1 refused as InvalidHash |

### Config domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-config` | Config source contract: the `Source` trait (load + optional watch/watch_value capability defaults), the `SignalStream`/`ValueStream` stream contracts, `FallbackSource` priority composition (first answer wins + merged effective-value change stream, no task boundary) |
| `crates/rushwind-config-env` | Environment variable engine: default keys + prefix parsing; unset variables are "absent", not errors |
| `crates/rushwind-config-file` | File engine: whole-file reads + parent-directory watching (safe against editors' atomic renames), event-burst merging, stale-content suppression, stream stops on Drop |
| `crates/rushwind-config-http` | HTTP config source: GET where the URL is the key + a polling ValueWatcher |
| `crates/rushwind-config-etcd` | etcd config source: GET by key + native Watch, signal/push dual mode |
| `crates/rushwind-config-consul` | Consul KV config source: GET by key + push-style watch via blocking queries |
| `crates/rushwind-config-embedded` | Compile-time embedded source: a static key→bytes tree fed by `include_bytes!`/`include_str!`, zero I/O, not watchable (fixed at link time) |
| `crates/rushwind-config-apollo` | Apollo config source: configs REST reads (canonical JSON, 404 = absent) + notifications/v2 long-polling watch, hand-written reqwest, no SDK |
| `crates/rushwind-config-nacos` | Nacos config source: v1 cs/configs REST reads (payload passthrough, 404 = absent, optional tenant/accessToken) + polling watch, hand-written reqwest, no SDK |
| `crates/rushwind-config-kubernetes` | Kubernetes ConfigMap config source: v1 configmaps REST reads (canonical key-sorted JSON, 404 = absent, optional namespace/bearer token) + polling watch, hand-written reqwest, no SDK |

### Registry domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-registry` | Registry contract: the `Registrar` + `Discovery` trait pair, key layout / wire format pinned byte-level by golden tests |
| `crates/rushwind-registry-etcd` | etcd adapter: register + discover, lease TTL + self-healing keepalive, handle Drop falls back to expiry; the live suite (CI etcd container) pins interoperability |
| `crates/rushwind-registry-consul` | Consul adapter: registration and discovery over the agent HTTP API |
| `crates/rushwind-registry-eureka` | Eureka adapter: registration and discovery over the eureka v2 REST API |
| `crates/rushwind-registry-kubernetes` | Kubernetes adapter: in-cluster pod-label registration + pod-watch discovery over the kube client |
| `crates/rushwind-registry-nacos` | Nacos adapter: registration and discovery over the nacos-sdk naming client |
| `crates/rushwind-registry-polaris` | Polaris adapter: registration and discovery over the polaris v1 HTTP client API |
| `crates/rushwind-registry-servicecomb` | ServiceComb center adapter: registration, heartbeats, and WebSocket watch over the v4 registration API |
| `crates/rushwind-registry-zookeeper` | ZooKeeper adapter: registration and discovery over the ZooKeeper protocol |

### Observability domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-metrics` | Metrics contract: the `Metrics` trait (counter add / histogram record / gauge set), label canonical ordering, recording never fails the caller |
| `crates/rushwind-metrics-prometheus` | Prometheus engine: lazy registration by name + per-class cache tables, `encode()` renders the text format for the /metrics route |
| `crates/rushwind-metrics-otel` | OTel engine: OTLP export (gRPC / HTTP binary protobuf), lazy instrument cache, gauge via an up-down counter stand-in |
| `crates/rushwind-metrics-datadog` | Datadog engine: hand-written DogStatsD wire protocol over UDP, label ordering, sample-rate suffix, optional batch buffering |
| `crates/rushwind-tracer` | Tracing contract: OTLP tracer-provider assembly + W3C trace-context carrier helpers |
| `crates/rushwind-health` | Health check contract: Status/Result/Checker + an aggregator with per-check timeouts + TCP/HTTP checkers + an axum handler |

### Resilience domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-retry` | Composable retry: exponential backoff + jitter, retry predicates, total timeout |
| `crates/rushwind-ratelimit` | Rate-limit contract: the algorithm-agnostic `Limiter` trait |
| `crates/rushwind-ratelimit-tokenbucket` | Token bucket engine: rate refill + burst capacity, Allow/Wait/Close, injectable clock (accounting on the injected clock) |
| `crates/rushwind-ratelimit-bbr` | BBR-style adaptive engine: sliding-window throughput estimate + inflight cap, injectable clock (window rotation on the injected clock) |
| `crates/rushwind-ratelimit-slidingwindow` | Sliding-window log engine: at most `limit` admissions in any sliding window, exact boundaries with no fixed-window double-budget seam, log memory bounded by `limit`, injectable clock |
| `crates/rushwind-ratelimit-leakybucket` | Leaky bucket (traffic shaping) engine: backlog drains at constant rate, admission throttled to `drain_rate`, idleness accrues nothing (the opposite of the token bucket's burst semantics), injectable clock |
| `crates/rushwind-circuitbreaker` | Circuit breaker contract: `State` + the `CircuitBreaker` trait (Allow/MarkSuccess/MarkFailure/Execute/State/Close) |
| `crates/rushwind-circuitbreaker-vegas` | Vegas-style engine: latency-inflation probing |
| `crates/rushwind-circuitbreaker-sres` | Google SRE probabilistic engine: graceful acceptance-rate decay instead of hard open/hard closed |
| `crates/rushwind-circuitbreaker-hystrix` | Hystrix-style engine: error-rate threshold + sleep window + half-open probe |

### Cache domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-cache` | KV cache contract: get/set/SetNX/batch + TTL |
| `crates/rushwind-cache-local` | In-process KV engine: TTL lazy expiry + capacity eviction |
| `crates/rushwind-cache-redis` | Redis engine: GET/SET/SETNX/DEL/EXISTS + MGET and pipelined batching |

### Lock domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-lock` | Distributed lock contract: try-once acquisition + ownership token + enforced TTL (no forever-lock) + owner-checked release/extend |
| `crates/rushwind-lock-memory` | In-process engine: a TTL-lazily-expiring map + token checks — single-process deployments and tests, same suite semantics as the distributed engines |
| `crates/rushwind-lock-redis` | Redis engine: `SET NX PX` acquisition + token-compared Lua atomic release/extend, live suite on the CI redis container |

### Messaging, transactions, and tasks

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-broker` | Messaging contract: the `Broker` / `Subscriber` traits, Message/Event shapes, JSON handler helpers, `AckMode::{Auto, Manual}` and the `Event::settle_with` dual-hook settlement, `NackAction::{Requeue, Drop}`, the `with_error_handler` observation point |
| `crates/rushwind-broker-kafka` | Kafka engine: per-topic producers + consumer-group subscriptions on samsa, a pure-Rust protocol client (no librdkafka C toolchain); batch offset auto-commit, no ack surface (honestly documented) |
| `crates/rushwind-broker-pulsar` | Pulsar engine: pulsar-rs multi-topic producers + Shared consumers with a fixed subscription name; Manual settlement rides an mpsc channel back to the pump (ack_with_id/nack_with_id) |
| `crates/rushwind-broker-gcpubsub` | Google Cloud Pub/Sub engine: v1 REST over hand-written reqwest — publish/pull/acknowledge; Manual settlement via ack deadline zero (zero means immediately redeliverable), token supplied and refreshed by the deployment |
| `crates/rushwind-broker-sqs` | Amazon SQS engine: Query API over hand-written reqwest + hand-written SigV4 (a frozen reference vector pins the signing chain) — SendMessage/ReceiveMessage/DeleteMessage/ChangeMessageVisibility; Manual settlement = delete or visibility zero |
| `crates/rushwind-broker-rabbitmq` | RabbitMQ engine: AMQP 0-9-1 publish/subscribe on lapin over the amq.topic exchange; the AMQP acker/nacker hangs directly off the settlement dual hooks, nack requeues per `NackAction` |
| `crates/rushwind-broker-nats` | NATS engine: core-NATS publish/subscribe on async-nats; at-most-once, no ack surface (honestly documented) |
| `crates/rushwind-broker-redis` | Redis engine: pub/sub on a dedicated subscription connection per topic; no acknowledgment surface (honestly documented) |
| `crates/rushwind-broker-mqtt` | MQTT engine: publish/subscribe on rumqttc with automatic re-subscribe on reconnect; the PUBACK hangs off the ack hook and the nack hook is "withhold the PUBACK" (MQTT 3.1.1 has no NACK frame) |
| `crates/rushwind-broker-stomp` | STOMP engine: a minimal STOMP 1.2 client over bare TCP, targeting the RabbitMQ stomp plugin; `ack:client` subscriptions, ACK/NACK frames as the two native actions |
| `crates/rushwind-transaction` | Distributed transaction contract: the minimal client face over engines |
| `crates/rushwind-transaction-dtm` | DTM engine: saga / TCC / two-phase messaging / XA over the DTM HTTP protocol (reqwest) |
| `crates/rushwind-apalis-postgres` | The Postgres storage backend for task queues (apalis): SKIP LOCKED claiming, visibility timeouts, orphan recovery |

### Encoding domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-encoding` | Encoding contract: serde marshaling behind named registered codecs |
| `crates/rushwind-encoding-json` | JSON engine: serde_json enrolled in the named codec registry |
| `crates/rushwind-encoding-msgpack` | MessagePack engine: rmp-serde |
| `crates/rushwind-encoding-yaml` | YAML engine: serde_yaml |
| `crates/rushwind-encoding-toml` | TOML engine: toml |
| `crates/rushwind-encoding-cbor` | CBOR engine: ciborium |
| `crates/rushwind-encoding-bson` | BSON engine: bson |
| `crates/rushwind-encoding-xml` | XML engine: quick-xml |
| `crates/rushwind-encoding-proto` | Protobuf engine: prost binary proto as a typed sidecar |

### Scripting domain

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-script` | Script engine contract: the capability-split trait family (six aggregated capabilities — loader / executor / global / function / module / watch — plus four independent ones: sandbox / runtime-hook / sync / quota), probe methods as the capability detection surface, the `ScriptValue` data bridge, name-keyed factory registry, `EnginePool` / `AutoGrowEnginePool`, `Manager`; the source contract `ScriptSource` / `SignalStream` with local carriers and compositions (memory, file mtime polling, static tree + prefix, dual-strategy multi-source aggregation, TTL-invalidated watch cache, transform chains) |
| `crates/rushwind-script-wasm` | Wasm engine: module instantiation and `_start` export invocation on wasmi, a pure interpreter; scalar global access and strictly signature-marshaled export-function calls; async host functions honestly refused (no bridge in a synchronous interpreter), remaining capabilities always refused |
| `crates/rushwind-script-cel` | CEL engine: expression compilation and evaluation on cel-rust, the `ScriptValue` variable bridge, maps flattened into prefixed globals |
| `crates/rushwind-script-lua` | Lua engine: mlua's vendored Lua 5.4, stdlib-whitelist sandbox, host-function registration, instruction-quota hook with true interruption plus post-hoc timeout checks, watch reload |
| `crates/rushwind-script-javascript` | JavaScript engine: boa on a dedicated actor thread (command channel + one-shot replies, serial execution), global/module/script-function bridging, result-array semantics, post-hoc quota checks |
| `crates/rushwind-script-starlark` | Starlark engine: module evaluation in starlark-rust's standard dialect, host-environment injection, script-function calls, JSON-serialized value readback, watch re-queueing |
| `crates/rushwind-script-config` | Config-source bridge: any config-domain `Source` adapted as a script `ScriptSource` — absence maps to not-found, error taxonomy bridged (NotWatchable→capability unsupported), signal streams passed through |

### AI and object storage

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-ai` | AI model contract: three trait faces — chat completion + streaming (with fully assembled tool-call events) + embeddings, object-safe (`Arc<dyn …>` is the deployment shape) |
| `crates/rushwind-ai-openai` | OpenAI-compatible engine: chat / streaming / embeddings over reqwest (SSE tool-call deltas assembled by index), covering OpenAI, Qwen, Ollama |
| `crates/rushwind-oss` | Object storage contract: put/get/delete/list and presign on S3-compatible storage, plus the four multipart-upload faces (begin/put/complete/abort, part numbers 1..=10000, contiguous assembly; presign available only from signature-authority engines, default Unsupported) |
| `crates/rushwind-oss-s3` | S3 engine: SigV4-signed REST over reqwest, covering AWS S3 and MinIO |
| `crates/rushwind-oss-local` | Local-disk engine: one instance per bucket, directories materialized on put, missing reads are NotFound, idempotent deletes, key-traversal refused; multipart via a staging directory |

### Assembly and testing

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-bootstrap` | Config-driven assembly: YAML → engines of every domain (storage / registry / authn / authz / broker / cache / circuit breaker / rate limit / lock / vector retrieval / AI / object storage / metrics / config sources / scripting) + HTTP server (route packages, edge middleware stack and CORS, per-subtree authn/authz wrapping, feature-gated health and metrics mounts) + the cron transport, all funneled into a single lifecycle |
| `crates/rushwind-testkit` | Cross-adapter conformance suites (lifecycle / storage / password / vector / lock, feature-gated) — any transport or engine must pass the whole suite |

### Examples

| Location | Responsibility |
|:---|:---|
| `examples/multi-server` | Two-server lifecycle demo (cascading shutdown, phase ordering) |
| `examples/axum-admin` | axum adapter demo: health routes + signal-driven graceful shutdown |
| `examples/ws-gateway` | WS gateway demo: gate refusal + session cap + lifecycle-cascaded session close |
| `examples/quic-gateway` | QUIC gateway demo: loopback gate + session cap + handshake deadline + endpoint-cascaded close |
| `examples/mqtt-ingest` | MQTT consumption demo: against an external broker, signal-driven clean exit |
| `examples/bootstrap-demo` | Assembly demo: one YAML + memory engine factories + a route package, a full service up |
| `examples/storage-basics` | The same Repository code over memory and SQLite engines, line-for-line identical output |
| `examples/vector-multi-engine` | Vector retrieval demo: one search code path over memory + the four external engines qdrant/milvus/weaviate/neo4j (each enabled by its `VECTOR_*_URL` env) |
| `examples/broker-manual-ack` | `AckMode::Manual` end to end (MQTT): json_handler acks the well-formed, the poison payload nacks requeue, `with_error_handler` observes without killing the subscription |
| `examples/oss-multipart` | Multipart upload demo: out-of-order parts assemble by part number, abort leaves nothing, the S3 half mints a presigned GET fetched credential-free |
| `examples/apalis-postgres-demo` | Task queue demo: the full deliver/schedule/consume cycle on Postgres storage (claiming, retry backoff, dead letters, orphan recovery) |

## Lifecycle

```text
┌─ Startup: every Server runs concurrently ─────────────────────────┐
│  Trigger set: OS signals / internal stop() / external signals /    │
│               any Server exiting                                   │
└────────────────────────────┬──────────────────────────────────────┘
                             ▼
        Phase 2: before hooks (in order, each with its own budget)
                             ▼
        Phase 3: every Server.stop runs concurrently (own budget, panic isolation)
                             ▼
        Phase 4: after hooks (in order, each with its own budget)
                             ▼
        Outcome: observable via outcome() / subscribe_done()
```

Every phase's budget is minted fresh **at the moment that phase begins**, never inherited from earlier context. A Server that ignores the stop signal is **dropped** at the drain deadline (Rust's drop is cancellation); a hung `stop()` is cut off by the deadline and recorded as `Timeout`. Normative details in [docs/architecture.md](./docs/architecture.md) (Chinese).

## Docs and tutorials

| Document | Content |
|:---|:---|
| [docs/architecture.md](./docs/architecture.md) | Architecture and contract semantics: budgets, shutdown mapping, conformance-suite case inventory (Chinese) |
| [docs/http-edge.md](./docs/http-edge.md) | HTTP edge and middleware stack design (Chinese) |
| [docs/session-middleware.md](./docs/session-middleware.md) | WS/QUIC session middleware contract (Chinese) |
| [docs/security-authn-authz.md](./docs/security-authn-authz.md) | Authn/authz layer contracts and threat-surface notes (Chinese) |
| [docs/threat-model.md](./docs/threat-model.md) | Threat model (Chinese) |
| [docs/tutorials](./docs/tutorials/README.md) | The 13-chapter tutorial series: lifecycle → assembly & config → storage contract → query syntax → HTTP stack → authn/authz → messaging → cache → registry & resilience → observability → scripting → transports → testing (Chinese) |

## For adapter authors

A new transport = implement `Server` + pass the conformance suite. Two things:

```rust
// crates/rushwind-transport-<your stack>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

A new storage engine likewise = implement `Repository` + pass the storage suite:

```rust
// crates/rushwind-storage-<your engine>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

A green `cargo test` is the definition of conformant. Contract semantics and the suite case inventory are in [docs/architecture.md](./docs/architecture.md); the contribution process is in [CONTRIBUTING.md](./CONTRIBUTING.md) (Chinese).

## Security

- A malicious `stop()` cannot wedge the process: every phase has a hard budget
- Server panics are isolated into records; sibling servers' cleanup is never skipped
- Vulnerability reporting in [SECURITY.md](./SECURITY.md), the threat model in [docs/threat-model.md](./docs/threat-model.md), authn/authz contracts and threat-surface notes in [docs/security-authn-authz.md](./docs/security-authn-authz.md)

## License

[MIT License](./LICENSE)
