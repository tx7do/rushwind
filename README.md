<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind · 锐风" width="128">

# RushWind · 锐风

[English](./README_en.md) | **中文** | [日本語](./README_ja.md)

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.81+-DEA584?logo=rust)](https://www.rust-lang.org/)
[![CI](https://github.com/tx7do/rushwind/actions/workflows/ci.yml/badge.svg)](https://github.com/tx7do/rushwind/actions/workflows/ci.yml)

</div>

---

## 设计哲学

> **不是全家桶，而是积木盒。**

RushWind 只做一件事：**可靠的多服务器生命周期编排**。核心定义契约——传输 trait、停机信号、实例模型——每一个具体协议栈都是独立的适配器 crate，由使用者按需拼装。核心不含日志、不含注册中心、不含配置中心：那些是积木，不是底板。

一切按 Rust 的所有权、取消与错误模型原生设计，契约语义与预算、停机行为的规范细节见 [docs/architecture.md](./docs/architecture.md)。数据访问层是同一哲学的延伸：`rushwind-storage` 用一套 Repository 契约驾驭多种存储引擎——每引擎一个适配器 crate，按需引入。

## 当前状态

能力面已全部落地：**104 个 crate + 8 个示例**，覆盖生命周期编排与传输、HTTP、存储、安全、配置、注册中心、可观测、弹性、缓存、消息、事务、任务、编码、脚本、AI、对象存储等全部能力域。每个域 = 一份契约 crate + 按需引入的引擎矩阵；引擎与适配器一律过一致性套件，`cargo test` 全绿即合规。各域独立演进，按 Rust 生态自身的节奏生长。

路线图上仅剩：kcp（存量互操作时）、`rushwind-protocols` 独立仓。

## 仓库布局

按域分组。契约 crate 不预设引擎；引擎与适配器按需引入，各自过一致性套件。

### 核心与传输

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-core` | 生命周期编排：并发启动、级联停机、分阶段限时、结果观察 |
| `crates/rushwind-transport` | 契约层：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum 适配器：`Router` 接入生命周期，优雅停机映射见架构文档 |
| `crates/rushwind-transport-ws` | WS 会话路由构建器：门链 + 准入策略 + 会话停机总线，契约见会话中间件文档 [session-middleware.md](./docs/session-middleware.md) |
| `crates/rushwind-transport-quic` | QUIC 适配器：quinn 接受循环接入生命周期，全套会话链；`stop()` 为真实释放（Endpoint::close） |
| `crates/rushwind-transport-webtransport` | WebTransport 适配器：wtransport 端点接入生命周期，全套会话链（会话请求时刻的 HTTP 族门链、原子准入、握手截止），`stop()` 为真实释放 |
| `crates/rushwind-transport-h3` | HTTP/3 适配器：h3/h3-quinn 请求服务接入生命周期，请求时刻门链（拒绝映射为状态响应）与连接级原子准入，`stop()` 为真实释放 |
| `crates/rushwind-transport-mqtt` | MQTT 消费桥：订阅外部 broker（每订阅 handler 注册 + 规范通配分发），重连退避 + 订阅重建，串行泵入 handler |
| `crates/rushwind-transport-sse` | SSE 通知传输：`/events` 线契约（CORS 预检、三源 token、401 纯文本错误行、反跨用户流检查、按用户过滤广播流）+ 进程内 Hub + bootstrap 工厂，授权经 `Gate` trait 插拔 |

### HTTP 面

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-http` | HTTP 边缘：gRPC 对齐的错误信封 `HttpError`——code/reason/message/details，`AuthnError`/`StorageError` 内置转换；请求中间件栈 recovery / request-id / logging / CORS / timeout 与 `HttpEdge` 装配器；`with_authn` / `with_authorization` 把认证鉴权契约接上 axum 路由，`Authenticated` 提取器；feature 门控的 `/healthz`+`/readyz` 与 `/metrics` 挂载，设计见 [docs/http-edge.md](./docs/http-edge.md) |
| `crates/rushwind-http-binding` | proto-HTTP 线格式契约（由调用方提供描述符池）：表单绑定器（点路径、两种字段名拼写、map/list/oneof 结构、周知叶子白名单）、Content-Type codec 解析、认证前 bind 层、protojson 响应 codec、每路由生命周期尾部、四字段状态错误信封 |
| `crates/rushwind-gen-http` | 描述符驱动的 proto-HTTP 路由面代码生成器：每 binding 路由表 + 表单绑定计划、(package, reason)→HTTP 状态码错误表、每 service 一个 trait + 空占位实现、public/gated 挂载发射器 |
| `crates/rushwind-proto-build` | 契约构建引擎（build-dep）：buf 注释闭包（选项字节保真）+ protox 类型面过滤 + prost/pbjson（可选 tonic）+ gen-http face 发射（字节级子闭包切片），部署 build.rs 缩为配置 |
| `crates/rushwind-redact` | 静态响应脱敏：`(redact.v1)` 选项 fail-closed 编译成计划，序列化前原地改写动态消息（mask/email/固定值/element 嵌套/method_skip），语义锚定 protoc-gen-go-redact 的生成物 |

### 存储域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-storage` | 存储契约：`Repository` trait、三种分页（Page/Offset/Token）、过滤器树、Viewer 五级租户、FieldMask、审计钩子 |
| `crates/rushwind-storage-memory` | 内存参考引擎：过滤器/排序/游标的语义基准，零依赖 |
| `crates/rushwind-storage-seaorm` | SeaORM 引擎：SQLite/PostgreSQL/MySQL 三后端同启，三方言 SQL 快照钉死渲染，SQLite 过一致性套件，live 套件跑 CI 容器 |
| `crates/rushwind-storage-seaorm-support` | 类型化实体服务桥：`PagingRequest` select 装配（query 语法过滤/orderBy/三态切片、按列型绑定）、SeaORM 失败→信封、实体时间↔proto Timestamp |
| `crates/rushwind-storage-mongodb` | MongoDB 引擎：FilterExpr→BSON 翻译离线单测，LIKE 族编译为转义正则，live 套件跑 CI 容器 |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch 引擎：REST + refresh-on-write，`.keyword` 精确匹配，bulk 原子批写 |
| `crates/rushwind-storage-opensearch` | OpenSearch 引擎：ES 线格式薄复用（wire 兼容） |
| `crates/rushwind-storage-cassandra` | Cassandra 引擎：bucket 固定分区 + 契约求值器过滤，LWT 原子批写 |
| `crates/rushwind-storage-influxdb` | InfluxDB 引擎：measurement 即表，id 为 series tag，InfluxQL 删除 |
| `crates/rushwind-storage-clickhouse` | ClickHouse 引擎：SQL over HTTP，mutations_sync 读己之写，探针式冲突检测 |
| `crates/rushwind-storage-cache` | Cache-Aside 装饰器：SingleFlight 合并击穿、缓存键含 viewer 作用域、generation 防陈旧回填 |
| `crates/rushwind-storage-soft-delete` | 软删除装饰器：墓碑写入、全读路径过滤、restore/purge，引擎无关 |
| `crates/rushwind-storage-observe` | 观测装饰器：每调用一个 `tracing` span（table/op/outcome），OTel 导出交由 subscriber 选型 |
| `crates/rushwind-storage-tree` | 树形查询：children/roots/ancestors/subtree，契约级遍历 + 环检测，任意引擎可用 |
| `crates/rushwind-storage-proto` | proto 契约线格式：`proto/rushwind/storage/v1/query.proto` 生成（prost + pbjson），29 操作符映射 + AIP 文本解析 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive 宏：DTO↔Record 映射编译期生成；标量族加宽、`#[record(as_text)]` 枚举、`#[record(with = "…")]` 自定义转换、`#[record(rename)]` 列名 |
| `crates/rushwind-storage-axum` | HTTP 端点层：任意 Repository 挂成 CRUD 路由，列表查询双入口（protojson `q` / AIP `filter`），viewer 钩子收口租户 |

### 安全域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-authn` | 认证契约：`Authenticator` trait（提取/验证两半）、`AuthClaims` 声明包、错误分类学、`AuthenticationGate` 门链接入层，见 [docs/security-authn-authz.md](./docs/security-authn-authz.md) |
| `crates/rushwind-authn-apikey` | API-key 引擎：静态 key 集 / 每键 claims / 验证回调 |
| `crates/rushwind-authn-basicauth` | Basic-Auth 引擎：RFC 7617 凭证对静态用户表或验证回调 |
| `crates/rushwind-authn-hmac` | HMAC 引擎：keyID.timestamp.signature 签名校验，时钟偏移窗口 |
| `crates/rushwind-authn-jwt` | JWT 引擎：HS/RS/PS/ES/EdDSA 族的铸造与验证 |
| `crates/rushwind-authn-noop` | Noop 引擎：全放行、铸造空凭证 |
| `crates/rushwind-authn-presharedkey` | 预共享 key 引擎：集合成员校验、铸造为随机抽取 |
| `crates/rushwind-authn-session` | 会话引擎：不透明会话 ID + 可插拔 SessionStore |
| `crates/rushwind-authn-gate` | 门会话阶段：`AccessTokenChecker` 契约（Redis 白名单/黑名单）与 authenticate-then-check 阶段头，trace-id 解析、未验签 bearer 嗅探；信封渲染留部署侧错误表 |
| `crates/rushwind-authz` | 鉴权契约：`Engine` trait（单裁决 + 三批量过滤）、Subject/Action/Resource/Project 模型、策略 JSON 互通，见 [docs/security-authn-authz.md](./docs/security-authn-authz.md) |
| `crates/rushwind-authz-acl` | ACL 引擎：有序 allow/deny 规则 + 通配匹配，默认拒绝、拒绝优先 |
| `crates/rushwind-authz-rbac` | RBAC 引擎：角色→权限、用户→角色双表，传递继承带环检测 |
| `crates/rushwind-authz-noop` | Noop 引擎：单裁决全通过、批量过滤全空 |

### 配置域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-config` | 配置源契约：`Source` trait（load + 可选 watch/watch_value 能力默认方法）、`SignalStream`/`ValueStream` 流契约、`FallbackSource` 优先级合成（首答胜出 + 有效值变更流合并，无任务边界） |
| `crates/rushwind-config-env` | 环境变量引擎：默认键 + 前缀解析，未设变量为"缺席"而非错误 |
| `crates/rushwind-config-file` | 文件引擎：整文件读取 + 父目录监视（编辑器原子重命名安全），事件突发合并、陈旧值内容抑制，流 Drop 即停 |
| `crates/rushwind-config-http` | HTTP 配置源：URL 即键的 GET + 轮询 ValueWatcher |
| `crates/rushwind-config-etcd` | etcd 配置源：按键 GET + 原生 Watch，signal/push 双模式 |
| `crates/rushwind-config-consul` | Consul KV 配置源：按键 GET + blocking query 的 push 式 watch |

### 注册中心域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-registry` | 注册中心契约：`Registrar` + `Discovery` 双 trait，键布局/线格式以 golden 测试字节级钉死 |
| `crates/rushwind-registry-etcd` | etcd 适配器：注册 + 发现，租约 TTL + 自愈 keepalive，句柄 Drop 回退过期；live 套件（CI etcd 容器）钉死互操作 |
| `crates/rushwind-registry-consul` | Consul 适配器：agent HTTP API 上的注册与发现 |
| `crates/rushwind-registry-eureka` | Eureka 适配器：eureka v2 REST API 上的注册与发现 |
| `crates/rushwind-registry-kubernetes` | Kubernetes 适配器：kube 客户端的 in-cluster pod 标签注册 + pod watch 发现 |
| `crates/rushwind-registry-nacos` | Nacos 适配器：nacos-sdk naming 客户端上的注册与发现 |
| `crates/rushwind-registry-polaris` | Polaris 适配器：polaris v1 HTTP 客户端 API 上的注册与发现 |
| `crates/rushwind-registry-servicecomb` | ServiceComb 服务中心适配器：v4 注册 API 上的注册、心跳与 WebSocket watch |
| `crates/rushwind-registry-zookeeper` | ZooKeeper 适配器：ZooKeeper 协议上的注册与发现 |

### 可观测域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-metrics` | 指标契约：`Metrics` trait（counter 累加 / histogram 记录 / gauge 设置），标签规范化排序，记录永不失败调用方 |
| `crates/rushwind-metrics-prometheus` | Prometheus 引擎：按名懒注册 + 每类缓存表，`encode()` 渲染文本格式挂 /metrics 路由 |
| `crates/rushwind-metrics-otel` | OTel 引擎：OTLP 导出（gRPC/HTTP 二进制 protobuf），仪表懒创建缓存，gauge 以 up-down counter 代位 |
| `crates/rushwind-metrics-datadog` | Datadog 引擎：手写 DogStatsD 线协议 over UDP，标签排序、采样率后缀、可选批量缓冲 |
| `crates/rushwind-tracer` | 追踪契约：OTLP tracer-provider 装配 + W3C trace-context 载体助手 |
| `crates/rushwind-health` | 健康检查契约：Status/Result/Checker + 每检查独立超时的聚合器 + TCP/HTTP 检查器 + axum 处理器 |

### 弹性域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-retry` | 可组合重试：指数退避 + 抖动、重试谓词、总超时 |
| `crates/rushwind-ratelimit` | 限流契约：算法无关的 `Limiter` trait |
| `crates/rushwind-ratelimit-tokenbucket` | 令牌桶引擎：按速率回填 + 突发容量，Allow/Wait/Close |
| `crates/rushwind-ratelimit-bbr` | BBR 式自适应引擎：滑窗吞吐估计 + inflight 上限 |
| `crates/rushwind-circuitbreaker` | 熔断契约：`State` + `CircuitBreaker` trait（Allow/MarkSuccess/MarkFailure/Execute/State/Close） |
| `crates/rushwind-circuitbreaker-vegas` | Vegas 式引擎：延迟膨胀探测 |
| `crates/rushwind-circuitbreaker-sres` | Google SRE 概率式引擎：以接受率优雅衰减取代硬开/硬闭 |
| `crates/rushwind-circuitbreaker-hystrix` | Hystrix 式引擎：错误率阈值 + 睡眠窗 + 半开试探 |

### 缓存域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-cache` | KV 缓存契约：get/set/SetNX/批量 + TTL |
| `crates/rushwind-cache-local` | 进程内 KV 引擎：TTL 惰性过期 + 容量驱逐 |
| `crates/rushwind-cache-redis` | Redis 引擎：GET/SET/SETNX/DEL/EXISTS + MGET 与管道批量 |

### 消息、事务与任务

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-broker` | 消息契约：`Broker` / `Subscriber` trait、Message/Event 形状、JSON handler 助手 |
| `crates/rushwind-broker-kafka` | Kafka 引擎：samsa 纯 Rust 协议客户端上的每 topic 生产者 + 消费组订阅（免 librdkafka C 工具链） |
| `crates/rushwind-broker-pulsar` | Pulsar 引擎：pulsar-rs 多 topic 生产者 + Shared 消费者，固定订阅名 |
| `crates/rushwind-broker-rabbitmq` | RabbitMQ 引擎：lapin 上的 AMQP 0-9-1 发布/订阅，走 amq.topic 交换机 |
| `crates/rushwind-broker-nats` | NATS 引擎：async-nats 上的 core-NATS 发布/订阅 |
| `crates/rushwind-broker-redis` | Redis 引擎：每 topic 专属订阅连接上的 pub/sub |
| `crates/rushwind-broker-mqtt` | MQTT 引擎：rumqttc 上的发布/订阅，重连自动重订阅 |
| `crates/rushwind-broker-stomp` | STOMP 引擎：裸 TCP 上的极简 STOMP 1.2 客户端，对接 RabbitMQ stomp 插件 |
| `crates/rushwind-transaction` | 分布式事务契约：引擎之上的最小客户端面 |
| `crates/rushwind-transaction-dtm` | DTM 引擎：DTM HTTP 协议（reqwest）上的 saga / TCC / 二阶段消息 / XA |
| `crates/rushwind-apalis-postgres` | 任务队列的 Postgres 存储后端（apalis）：SKIP LOCKED 认领、可见性超时、孤儿恢复 |

### 编码域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-encoding` | 编码契约：命名注册的 codec 之后的 serde 编解码 |
| `crates/rushwind-encoding-json` | JSON 引擎：serde_json 入册命名 codec 注册表 |
| `crates/rushwind-encoding-msgpack` | MessagePack 引擎：rmp-serde |
| `crates/rushwind-encoding-yaml` | YAML 引擎：serde_yaml |
| `crates/rushwind-encoding-toml` | TOML 引擎：toml |
| `crates/rushwind-encoding-cbor` | CBOR 引擎：ciborium |
| `crates/rushwind-encoding-bson` | BSON 引擎：bson |
| `crates/rushwind-encoding-xml` | XML 引擎：quick-xml |
| `crates/rushwind-encoding-proto` | Protobuf 引擎：prost 二进制 proto，作为类型化 sidecar |

### 脚本域

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-script` | 脚本引擎契约：能力拆分 trait 族（loader / executor / global / function / module / watch 六能力聚合，sandbox / runtime-hook / sync / quota 四能力独立），probe 方法即能力探测面，`ScriptValue` 数据桥，名称键工厂注册表，`EnginePool` / `AutoGrowEnginePool`，`Manager`；源契约 `ScriptSource` / `SignalStream` 与本地载体、组合（memory、file mtime 轮询、静态树 + 前缀、双策略多源聚合、TTL 失效监视缓存、变换链） |
| `crates/rushwind-script-wasm` | Wasm 引擎：wasmi 纯解释器上的模块实例化与 `_start` 导出调用，空导入面，其余能力恒拒 |
| `crates/rushwind-script-cel` | CEL 引擎：cel-rust 表达式编译与求值，`ScriptValue` 变量桥，map 扁平化为前缀全局变量 |
| `crates/rushwind-script-lua` | Lua 引擎：mlua vendored Lua 5.4，标准库白名单沙箱、宿主函数注册、指令配额钩子真中断与事后超时检查、watch 重载 |
| `crates/rushwind-script-javascript` | JavaScript 引擎：boa 运行于专属 actor 线程（命令通道 + 一次性应答，串行执行），全局/模块/脚本函数桥接、结果数组语义、配额事后检查 |
| `crates/rushwind-script-starlark` | Starlark 引擎：starlark-rust 标准方言模块求值、宿主环境注入、脚本函数调用、JSON 序列化值读回、watch 重排队 |
| `crates/rushwind-script-config` | 配置源桥：任意配置域 `Source` 适配为脚本 `ScriptSource`——缺席映射为未找到、错误分类桥（NotWatchable→能力不支持）、信号流直通 |

### AI 与对象存储

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-ai` | AI 模型契约：OpenAI 兼容端点上的 chat 补全 |
| `crates/rushwind-ai-openai` | OpenAI 兼容引擎：reqwest 上的 chat / 流式 / embeddings，通吃 OpenAI、Qwen、Ollama |
| `crates/rushwind-oss` | 对象存储契约：S3 兼容存储上的 put/get/delete |
| `crates/rushwind-oss-s3` | S3 引擎：reqwest 上的 SigV4 签名 REST，覆盖 AWS S3 与 MinIO |
| `crates/rushwind-oss-local` | 本地盘引擎：每 bucket 一实例、目录随 put 物化、缺失读 NotFound、删除幂等、键穿越拒绝 |

### 装配与测试

| 位置 | 职责 |
|:---|:---|
| `crates/rushwind-bootstrap` | 配置驱动装配：YAML → 各域引擎（存储/注册中心/认证/鉴权/broker/缓存/熔断/限流/AI/对象存储/指标/配置源/脚本）+ HTTP 服务器（路由包、边缘中间件栈与 CORS、按子树的认证鉴权包装、特性门控的健康与指标挂载）+ cron 传输，全部汇入单一生命周期 |
| `crates/rushwind-testkit` | 跨适配器一致性测试套件——任何传输/引擎必须整套通过 |

### 示例

| 位置 | 职责 |
|:---|:---|
| `examples/multi-server` | 双服务器生命周期演示（级联停机、阶段顺序） |
| `examples/axum-admin` | axum 适配器演示：健康路由 + 信号驱动的优雅停机 |
| `examples/ws-gateway` | WS 网关演示：门拒绝 + 会话上限 + 生命周期级联的会话关闭 |
| `examples/quic-gateway` | QUIC 网关演示：环回门 + 会话上限 + 握手截止 + 端点级联关闭 |
| `examples/mqtt-ingest` | MQTT 消费演示：对接外部 broker，信号驱动的干净退出 |
| `examples/bootstrap-demo` | 装配演示：一份 YAML + memory 引擎工厂 + 路由包，起完整服务 |
| `examples/storage-basics` | 同一段 Repository 代码跑内存与 SQLite 双引擎，输出逐行一致 |
| `examples/apalis-postgres-demo` | 任务队列演示：Postgres 存储上的投递/调度/消费全程（认领、重试退避、死信、孤儿恢复） |

## 生命周期

```text
┌─ 启动：全部 Server 并发运行 ────────────────────────────┐
│  触发集合：OS 信号 / 内部 stop() / 外部信号 / 任一 Server 退出 │
└────────────────────────┬──────────────────────────┘
                         ▼
        阶段 2：before 钩子（顺序执行，各自独立预算）
                         ▼
        阶段 3：全部 Server.stop 并发执行（各自独立预算，panic 隔离）
                         ▼
        阶段 4：after 钩子（顺序执行，各自独立预算）
                         ▼
        终局：outcome() / subscribe_done() 对外可观察
```

每一阶段的预算都在**该阶段开始的时刻**现取现造，绝不继承自更早的上下文。忽略停机信号的 Server 在排水截止时被**丢弃**（Rust 的 drop 即取消），挂死的 `stop()` 被截止切断并记录 `Timeout`。规范细节见 [docs/architecture.md](./docs/architecture.md)。

## 给适配器作者

新传输 = 实现 `Server` + 过一致性套件，两件事：

```rust
// crates/rushwind-transport-<你的栈>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

新存储引擎同理 = 实现 `Repository` + 过存储套件：

```rust
// crates/rushwind-storage-<你的引擎>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

`cargo test` 全绿即合规，CI 对每个适配器 crate 强制执行。契约语义与套件用例清单见 [docs/architecture.md](./docs/architecture.md)。

## 开发门禁

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI 在 Linux/Windows/macOS 三平台矩阵上执行同一套门禁。全 workspace `#![forbid(unsafe_code)]`。

## 安全

- 恶意的 `stop()` 拖不死进程：每个阶段有硬预算
- 服务器 panic 被隔离为记录，绝不跳过兄弟服务器的清理
- 漏洞报告流程见 [SECURITY.md](./SECURITY.md)，威胁模型见 [docs/threat-model.md](./docs/threat-model.md)，认证/鉴权层契约与威胁面速记见 [docs/security-authn-authz.md](./docs/security-authn-authz.md)

## 许可

[MIT License](./LICENSE)
