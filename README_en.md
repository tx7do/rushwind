<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

**English** | [中文](./README.md) | [日本語](./README_ja.md)

</div>

---

## Design philosophy

> **Not a batteries-included framework. A box of bricks.**

RushWind does exactly one thing: **reliable multi-server lifecycle orchestration**. The core defines contracts — a transport trait, a shutdown signal, an instance model — and every concrete protocol stack is a separate adapter crate that the user snaps in as needed. The core contains no logging, no registry, no config center: those are bricks, not the baseplate.

Everything is designed native to Rust's ownership, cancellation and error models; normative details of contract semantics, budgets and shutdown behavior live in [docs/architecture.md](./docs/architecture.md) (Chinese). The data-access layer extends the same philosophy: the `rushwind-storage` contract — one Repository contract driving many engines — with one adapter crate per engine, pulled in as needed.

## Status

The capability surface is fully landed: **124 crates + 8 examples**, spanning lifecycle orchestration, transport, HTTP, storage, security (password hashing included), config, registry, observability, resilience, cache, messaging, transactions, tasks, encoding, scripts, AI, object storage, vector retrieval, and distributed locks. Each domain = one contract crate + an engine matrix pulled in as needed; every engine and adapter must pass the conformance suite — a green `cargo test` is the definition of conformant. Each domain evolves on its own, at the Rust ecosystem's own pace.

Still on the roadmap: kcp (for legacy interop), and a standalone `rushwind-protocols` repo.

## Layout

Grouped by domain. Contract crates presuppose no engine; engines and adapters are pulled in as needed, each passing the conformance suite.

### Core & transport

| Path | Role |
|:---|:---|
| `crates/rushwind-core` | lifecycle orchestration: concurrent start, cascading shutdown, bounded phases, outcome observation |
| `crates/rushwind-transport` | contracts: `Server` trait, `StopSignal`, `Instance`, `ServerError` |
| `crates/rushwind-transport-axum` | axum adapter: serves a `Router` under the lifecycle; shutdown mapping in the architecture doc |
| `crates/rushwind-transport-ws` | WS session route builder: gates + admission policy + session-shutdown bus; contracts in the session-middleware doc [session-middleware.md](./docs/session-middleware.md) (Chinese) |
| `crates/rushwind-transport-quic` | QUIC adapter: quinn accept loop under the lifecycle, full session chain; `stop()` is a real release (Endpoint::close) |
| `crates/rushwind-transport-webtransport` | WebTransport adapter: wtransport endpoint under the lifecycle with the full session chain (HTTP-family gates at session-request time, atomic admission, handshake deadline); `stop()` is a real release |
| `crates/rushwind-transport-h3` | HTTP/3 adapter: h3/h3-quinn request serving under the lifecycle with request-time gates (rejections mapped to status responses) and per-connection atomic admission; `stop()` is a real release |
| `crates/rushwind-transport-mqtt` | MQTT consumer bridge: subscribes to an external broker (per-subscription handler registration, spec-compliant wildcard dispatch), reconnect backoff + subscription re-establishment, serial pump into the handler |

### HTTP surface

| Path | Role |
|:---|:---|
| `crates/rushwind-http` | the HTTP edge: a gRPC-aligned error envelope `HttpError` — code/reason/message/details with built-in `AuthnError`/`StorageError` conversions; the request middleware stack recovery / request-id / logging / CORS / timeout and the `HttpEdge` assembler; `with_authn` / `with_authorization` bridging the security contracts onto axum routes with the `Authenticated` extractor; feature-gated `/healthz`+`/readyz` and `/metrics` mounts, design in [docs/http-edge.md](./docs/http-edge.md) (Chinese) |
| `crates/rushwind-http-binding` | the proto-HTTP wire contract, parameterized by a caller-supplied descriptor pool: the form binder (dotted paths, both field-name spellings, map/list/oneof structure, the well-known leaf whitelist), Content-Type codec resolution, the pre-auth bind layer, the protojson response codec, the per-route lifecycle tail, and the four-field status error envelope |
| `crates/rushwind-gen-http` | the descriptor-driven code generator for the proto-HTTP route surface: per-binding route tables with form-binding plans, (package, reason) → HTTP status error tables, one service trait per annotated proto service with null placeholder impls, and the public/gated mount emitters |

### Storage domain

| Path | Role |
|:---|:---|
| `crates/rushwind-storage` | storage contracts: `Repository` trait, three paging strategies (Page/Offset/Token), filter tree, five-level viewer tenancy, field mask, audit hook |
| `crates/rushwind-storage-memory` | in-memory reference engine: the semantic baseline for filters/sorting/cursors, zero dependencies |
| `crates/rushwind-storage-seaorm` | SeaORM engine: SQLite/PostgreSQL/MySQL all enabled, per-dialect SQL pinned by snapshot tests, SQLite passes the suite, live suites run in CI containers |
| `crates/rushwind-storage-mongodb` | MongoDB engine: FilterExpr→BSON translation unit-tested offline, LIKE family compiled to escaped regex, live suite in CI containers |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch engine: REST with refresh-on-write, `.keyword` exact matching, atomic bulk with rollback |
| `crates/rushwind-storage-opensearch` | OpenSearch engine: thin reuse of the ES wire shape (wire-compatible) |
| `crates/rushwind-storage-cassandra` | Cassandra engine: bucket-fixed partition + contract-evaluator filtering, LWT atomic batches |
| `crates/rushwind-storage-influxdb` | InfluxDB engine: measurement as table, id as series tag, InfluxQL deletes |
| `crates/rushwind-storage-clickhouse` | ClickHouse engine: SQL over HTTP, mutations_sync for read-your-writes, probe-based conflict detection |
| `crates/rushwind-storage-cache` | cache-aside decorator: singleflight miss-coalescing, scope-aware cache keys, generation-guarded invalidation |
| `crates/rushwind-storage-soft-delete` | soft-delete decorator: tombstone writes, filtered on every read path, restore/purge — engine-agnostic |
| `crates/rushwind-storage-observe` | observability decorator: one `tracing` span per call (table/op/outcome); OTel export is a subscriber choice |
| `crates/rushwind-storage-tree` | tree queries: children/roots/ancestors/subtree as contract-level traversal with cycle guards, on any engine |
| `crates/rushwind-storage-proto` | the proto-defined wire contract: generated from `proto/rushwind/storage/v1/query.proto` (prost + pbjson), 29-operator mapping + AIP text parsing |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive macros: DTO↔Record mapping generated at compile time; widened scalars, `#[record(as_text)]` enums, `#[record(with = "…")]` custom conversions, `#[record(rename)]` columns |
| `crates/rushwind-storage-axum` | the HTTP endpoint layer: any Repository as CRUD routes, list queries via protojson `q` or AIP `filter`, viewer hook for tenancy |

### Vector Retrieval

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-vector` | vector-retrieval contract (RAG / semantic search): unified `VectorQuery`, scores always bigger-is-more-similar (cosine `1−d` / L2 `1/(1+d)` / raw dot), top-k not pagination, dimension discipline, pgvector text codec — types and pure functions only (porting go-crud's vector semantics) |
| `crates/rushwind-vector-memory` | in-memory reference engine: brute-force top-k + dimension fixing/refusal, the semantic baseline for external engine adapters; the external engine family (qdrant/milvus/weaviate, …) is scoped per engine when needed |

### Security domain

| Path | Role |
|:---|:---|
| `crates/rushwind-authn` | the authentication contract: `Authenticator` trait (extraction/validation halves), the `AuthClaims` bag, the error taxonomy, and `AuthenticationGate` — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authn-apikey` | API-key engine: static key set / per-key claims / validator callback |
| `crates/rushwind-authn-basicauth` | basic-auth engine: RFC 7617 credentials against a static user table or a validator callback |
| `crates/rushwind-authn-hmac` | HMAC engine: keyID.timestamp.signature verification with a clock-skew window |
| `crates/rushwind-authn-jwt` | JWT engine: mint and verify over the HS/RS/PS/ES/EdDSA families |
| `crates/rushwind-authn-noop` | noop engine: accepts everything, mints nothing |
| `crates/rushwind-authn-presharedkey` | preshared-key engine: set-membership check, mint by random draw |
| `crates/rushwind-authn-session` | session engine: opaque session IDs against a pluggable session store |
| `crates/rushwind-authz` | the authorization contract: the `Engine` trait (single verdict + three bulk filters), the Subject/Action/Resource/Project model, the JSON policy interchange — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authz-acl` | ACL engine: ordered allow/deny rules with wildcard matching, default-deny, deny-overrides |
| `crates/rushwind-authz-rbac` | RBAC engine: role→permission and user→role maps, transitive inheritance with cycle guards |
| `crates/rushwind-authz-noop` | noop engine: every verdict passes, every filter returns empty |

### Config domain

| Path | Role |
|:---|:---|
| `crates/rushwind-config` | the configuration-source contract: the `Source` trait (load plus defaulted watch/watch_value capabilities), the `SignalStream`/`ValueStream` stream contracts, and `FallbackSource` — priority composition where the first answer wins and change streams merge into the effective value, no task boundaries |
| `crates/rushwind-config-env` | environment-variable engine: default key + prefix resolution; an unset variable is absence, not an error |
| `crates/rushwind-config-file` | file engine: whole-file reads + parent-directory watching (editor atomic-rename safe), event bursts coalesced, stale values suppressed by content, stream drop stops the watch |
| `crates/rushwind-config-http` | HTTP config source: GET by URL key + a polling ValueWatcher |
| `crates/rushwind-config-etcd` | etcd config source: GET by key + native watch, signal and push modes |
| `crates/rushwind-config-consul` | Consul KV config source: GET by key + push-mode watch via blocking queries |
| `crates/rushwind-config-embedded` | compile-time embedded source: a static key→bytes tree fed by include_bytes!/include_str! — zero I/O, not watchable (fixed at link time) |
| `crates/rushwind-config-apollo` | Apollo config source: configs REST read (canonical JSON, 404=absent) + notifications/v2 long-poll watch, hand-written over reqwest, no SDK |

### Registry domain

| Path | Role |
|:---|:---|
| `crates/rushwind-registry` | the registry contract: the `Registrar` + `Discovery` trait pair, key layouts and wire format pinned byte-for-byte by golden tests |
| `crates/rushwind-registry-etcd` | etcd adapter: registration + discovery, lease TTL + self-healing keepalive, handle drop falls back to expiry; live suite (CI etcd container) pins the interop |
| `crates/rushwind-registry-consul` | Consul adapter: registration and discovery over the agent HTTP API |
| `crates/rushwind-registry-eureka` | Eureka adapter: registration and discovery over the eureka v2 REST API |
| `crates/rushwind-registry-kubernetes` | Kubernetes adapter: in-cluster pod-label registration and pod-watch discovery via the kube client |
| `crates/rushwind-registry-nacos` | Nacos adapter: registration and discovery over the nacos-sdk naming client |
| `crates/rushwind-registry-polaris` | Polaris adapter: registration and discovery over the polaris v1 HTTP client API |
| `crates/rushwind-registry-servicecomb` | ServiceComb service-center adapter: registration, heartbeats, and WebSocket watch over the v4 registry API |
| `crates/rushwind-registry-zookeeper` | ZooKeeper adapter: registration and discovery over the ZooKeeper protocol |

### Observability domain

| Path | Role |
|:---|:---|
| `crates/rushwind-metrics` | the metrics contract: the `Metrics` trait (counter adds / histogram records / gauge sets), canonicalized label order, recording never fails the caller |
| `crates/rushwind-metrics-prometheus` | Prometheus engine: lazy per-name registration with per-kind cache tables, `encode()` renders the text format for a metrics route |
| `crates/rushwind-metrics-otel` | OTel engine: OTLP export (gRPC / HTTP binary protobuf), lazily-created cached instruments, the gauge as an up-down counter |
| `crates/rushwind-metrics-datadog` | Datadog engine: hand-rolled DogStatsD line protocol over UDP, sorted tags, sample-rate suffix, optional batch buffering |
| `crates/rushwind-tracer` | the tracing contract: OTLP tracer-provider setup + W3C trace-context carrier helpers |
| `crates/rushwind-health` | the health-check contract: Status/Result/Checker, an aggregator with per-check timeouts, TCP and HTTP checkers, an axum handler |

### Resilience domain

| Path | Role |
|:---|:---|
| `crates/rushwind-retry` | composable retry: exponential backoff with jitter, retry predicates, total timeout |
| `crates/rushwind-ratelimit` | the rate-limiting contract: the algorithm-agnostic `Limiter` trait |
| `crates/rushwind-ratelimit-tokenbucket` | token-bucket engine: refill at rate + burst capacity, Allow/Wait/Close, injectable clock for accounting |
| `crates/rushwind-ratelimit-bbr` | BBR-inspired adaptive engine: sliding-window throughput estimation + an inflight cap |
| `crates/rushwind-circuitbreaker` | the circuit-breaker contract: `State` + the `CircuitBreaker` trait (Allow/MarkSuccess/MarkFailure/Execute/State/Close) |
| `crates/rushwind-circuitbreaker-vegas` | Vegas-style engine: latency-inflation probing |
| `crates/rushwind-circuitbreaker-sres` | Google-SRE probabilistic engine: graceful acceptance decay instead of hard open/close |
| `crates/rushwind-circuitbreaker-hystrix` | Hystrix-style engine: error-rate threshold + sleep window + half-open trial |

### Cache domain

| Path | Role |
|:---|:---|
| `crates/rushwind-cache` | the KV cache contract: get/set/SetNX/batch with TTLs |
| `crates/rushwind-cache-local` | in-process KV engine: TTL'd entries with lazy expiry + capacity eviction |
| `crates/rushwind-cache-redis` | Redis engine: GET/SET/SETNX/DEL/EXISTS + MGET and pipelined sets |

### Locks

| Location | Responsibility |
|:---|:---|
| `crates/rushwind-lock` | distributed-lock contract: try-once acquire + ownership tokens + mandatory TTLs (no forever-locks) + ownership-checked release/extend |
| `crates/rushwind-lock-memory` | in-process engine: a lazily-expiring map + token checks — single-process deployments and tests, same conformance suite as the distributed engines |
| `crates/rushwind-lock-redis` | Redis engine: `SET NX PX` acquire + token-matched atomic Lua release/extend; the live suite runs on CI's redis container |

### Messaging, transactions & tasks

| Path | Role |
|:---|:---|
| `crates/rushwind-broker` | the message-broker contract: `Broker` / `Subscriber` traits, the Message and Event shapes, JSON handler helpers |
| `crates/rushwind-broker-kafka` | Kafka engine: per-topic producers + consumer-group subscriptions over samsa, the pure-Rust Kafka protocol client (no librdkafka C toolchain) |
| `crates/rushwind-broker-pulsar` | Pulsar engine: pulsar-rs multi-topic producers + Shared-type consumers with a fixed subscription name |
| `crates/rushwind-broker-rabbitmq` | RabbitMQ engine: AMQP 0-9-1 publish/subscribe over lapin against the amq.topic exchange |
| `crates/rushwind-broker-nats` | NATS engine: core-NATS publish/subscribe over async-nats |
| `crates/rushwind-broker-redis` | Redis engine: pub/sub over a dedicated subscriber connection per topic |
| `crates/rushwind-broker-mqtt` | MQTT engine: publish/subscribe over rumqttc with automatic resubscribe on reconnect |
| `crates/rushwind-broker-stomp` | STOMP engine: a minimal STOMP 1.2 client over raw TCP, talking to RabbitMQ's stomp plugin |
| `crates/rushwind-transaction` | the distributed-transaction contract: a minimal client surface over engines |
| `crates/rushwind-transaction-dtm` | DTM engine: saga / TCC / 2-phase message / XA over DTM's HTTP protocol on reqwest |
| `crates/rushwind-apalis-postgres` | a Postgres storage backend for the apalis task queue: SKIP LOCKED claims, visibility timeouts, orphan recovery |

### Encoding domain

| Path | Role |
|:---|:---|
| `crates/rushwind-encoding` | the codec contract: serde marshaling behind named, registered codecs |
| `crates/rushwind-encoding-json` | JSON engine: serde_json behind the named-codec registry |
| `crates/rushwind-encoding-msgpack` | MessagePack engine: rmp-serde |
| `crates/rushwind-encoding-yaml` | YAML engine: serde_yaml |
| `crates/rushwind-encoding-toml` | TOML engine: toml |
| `crates/rushwind-encoding-cbor` | CBOR engine: ciborium |
| `crates/rushwind-encoding-bson` | BSON engine: bson |
| `crates/rushwind-encoding-xml` | XML engine: quick-xml |
| `crates/rushwind-encoding-proto` | Protobuf engine: binary proto over prost, as a typed sidecar |

### Script domain

| Path | Role |
|:---|:---|
| `crates/rushwind-script` | the script-engine contract: the capability-split trait family (loader / executor / global / function / module / watch aggregated; sandbox / runtime-hook / sync / quota standalone), probe methods as the capability surface, the `ScriptValue` data bridge, the name-keyed factory registry, `EnginePool` / `AutoGrowEnginePool`, the `Manager`; the source contract `ScriptSource` / `SignalStream` with its local carriers and compositions (memory, file mtime polling, static tree + prefix joining, dual-strategy multi-source, TTL'd invalidation cache, transform chains) |
| `crates/rushwind-script-wasm` | the Wasm engine: module instantiation with `_start` invocation over the wasmi pure interpreter, an empty import surface, every other capability refused outright |
| `crates/rushwind-script-cel` | the CEL engine: expression compilation and evaluation over cel-rust, the `ScriptValue` variable bridge, maps flattened into prefixed globals |
| `crates/rushwind-script-lua` | the Lua engine: mlua's vendored Lua 5.4 with the standard-library allow-list sandbox, host-function registration, a genuine instruction-quota hook interrupt plus post-hoc timeout checks, and watch reloads |
| `crates/rushwind-script-javascript` | the JavaScript engine: boa running on a dedicated actor thread (command channel + one-shot replies, serialized execution), globals/modules/script-function bridging, the result-array shape, post-hoc quota checks |
| `crates/rushwind-script-starlark` | the Starlark engine: starlark-rust standard-dialect module evaluation, host-environment injection, script-function invocation, value readback through the JSON serializer, and watch requeueing |
| `crates/rushwind-script-config` | the config-source bridge: any configuration-domain `Source` adapted into a script `ScriptSource` — absence mapped onto not-found, the error-taxonomy bridge (NotWatchable → capability-not-supported), a pass-through signal stream |

### AI & object storage

| Path | Role |
|:---|:---|
| `crates/rushwind-ai` | the AI-model contract: chat completions over OpenAI-compatible endpoints |
| `crates/rushwind-ai-openai` | the OpenAI-compatible engine: chat / streaming / embeddings over reqwest — OpenAI, Qwen, Ollama |
| `crates/rushwind-oss` | the object-storage contract: put/get/delete over S3-compatible stores |
| `crates/rushwind-oss-s3` | the S3 engine: SigV4-signed REST over reqwest, covering AWS S3 and MinIO |

### Assembly & testing

| Path | Role |
|:---|:---|
| `crates/rushwind-bootstrap` | config-driven assembly: YAML → storage engine + HTTP servers + route packs, under one lifecycle |
| `crates/rushwind-testkit` | cross-adapter conformance suites (lifecycle / storage / password / vector / lock, feature-gated) — every transport and engine must pass them in full |

### Examples

| Path | Role |
|:---|:---|
| `examples/multi-server` | two-server lifecycle demo (cascade, phase ordering) |
| `examples/axum-admin` | axum adapter demo: health route + signal-driven graceful shutdown |
| `examples/ws-gateway` | WS gateway demo: gate rejection + session cap + lifecycle-aligned session teardown |
| `examples/quic-gateway` | QUIC gateway demo: loopback gate + session cap + handshake deadline + endpoint-level teardown |
| `examples/mqtt-ingest` | MQTT consumer demo: point at an external broker, clean signal-driven exit |
| `examples/bootstrap-demo` | assembly demo: one YAML + memory factory + route packs, a full service |
| `examples/storage-basics` | the same repository code over in-memory and SQLite engines, line-for-line identical output |
| `examples/apalis-postgres-demo` | task-queue demo: the whole push/schedule/consume story over the Postgres storage (claims, retry backoff, dead-letter, orphan recovery) |

## Lifecycle

```text
┌─ start: all servers run concurrently ──────────────────┐
│  trigger set: OS signals / internal stop() /            │
│  external signals / any server exiting                  │
└────────────────────────┬──────────────────────────┘
                         ▼
   phase 2: before hooks (sequential, each with its own budget)
                         ▼
   phase 3: all Server.stop run concurrently (independent budgets, panic isolation)
                         ▼
   phase 4: after hooks (sequential, each with its own budget)
                         ▼
   endgame: outcome() / subscribe_done() observable from outside
```

Every phase's budget is minted **at the moment that phase begins**, never inherited from an earlier context. A server that ignores the shutdown signal is **dropped** at the drain deadline (Rust's drop is cancellation); a hung `stop()` is cut off by the deadline and recorded as `Timeout`. Normative details in [docs/architecture.md](./docs/architecture.md) (Chinese).

## For adapter authors

A new transport = implement `Server` + pass the conformance suite. Two things:

```rust
// crates/rushwind-transport-<your-stack>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

A storage engine works the same way = implement `Repository` + pass the storage suite:

```rust
// crates/rushwind-storage-<your-engine>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

A green `cargo test` is the definition of conformant; CI enforces it for every adapter crate.

## Development gates

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs the same gates on a Linux/Windows/macOS matrix. The entire workspace is `#![forbid(unsafe_code)]`.

## Security

- A malicious `stop()` cannot wedge the process: every phase has a hard budget
- A panicking server is isolated and recorded; it never skips sibling teardown
- Vulnerability reporting in [SECURITY.md](./SECURITY.md), the threat model in [docs/threat-model.md](./docs/threat-model.md) (Chinese), the authn/authz contract and threat-surface notes in [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese)

## License

[MIT License](./LICENSE)
