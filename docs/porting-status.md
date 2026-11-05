# rushwind 域实现状态

全部 20 个域的 Rust 实现收口记录。日期：2026-09-14。

## 上游跟进（2026-11-05）

三个上游 Go 仓库（go-crud、kratos-transport、kratos-bootstrap）在移植基线（2026-09-14）之后的提交已逐条盘点并跟进：

**storage 线（go-crud a66fd69..638881f）**

- 多租户隔离修复：审计发现全部引擎的 `upsert`（部分引擎的 `create`）绕过 viewer 作用域——跨租户覆盖写（memory/mongodb/seaorm/es/os/cassandra/clickhouse/influx 全中招）；修复为统一探针语义：先经作用域读探针，可见才更新，不可见即插入面，主键被占一律 `Conflict`，绝不静默覆盖不可见行。influx 另修行协议换行注入（租户可控字符串可伪造第二个点）。
- 租户轴落地（go-crud `EnforceTenant` 语义移植）：`Viewer` 增 `Tenancy`（Unidentified 缺省 fail-closed → `StorageError::MissingViewer`；Platform 系统后台放行；Tenant(id) 全路径谓词注入 + 写入强制盖章）；`Schema` 增 `tenant_column` opt-in；一致性套件增租户模块（跨租户 get/update/delete NotFound、upsert Conflict 且原行完好、盖章不可迁移、token 分页租户隔离、无身份 fail-closed），宏签名要求 tenant factory——此前 CI 全绿正因套件从未钉住作用域下的 upsert。
- HTTP 边与装配：`CrudApi` 无 viewer 策略时由 all-access 改为缺省拒绝（fail-closed，`viewer: all` 显式放行）；租户列伪造守卫（作用域 viewer 不得写非本人 owner/unit/tenant 值）；`MissingViewer` → 400。
- 缓存装饰器 `ScopeKey` 增租户轴（同 actor 异租户不再共条目）。
- 其余 go-crud 修复经核对为已覆盖或结构不适用：软删无 `deleted_by`（与 531624f 修后语义一致）、分页 limit 已钳制（opensearch 同源缺陷的 Rust 面无恙）、gorm 主键回读/mongodb FindOne 选项/Rust 面结构不同、cassandra 三缺陷（gocql 一致性/大小写折叠/集合解码）为驱动专属、doris 不存在于 Rust 线。

**transport/broker 线（kratos-transport 035b95cc..d5f78b01）**

- broker-redis：健康检查独立连接（同连接 Ping 内部 Receive 会吞 PUBLISH 帧）、心跳 PONG 静默（issue #133）、断开后 publish/subscribe 清晰报错而非 panic、重连地址重推、密码连接、PEL 僵尸消息 XAUTOCLAIM 回收。
- broker-rabbitmq：订阅模型收敛为「同步 declare+bind + 单一再订阅循环」（中间轮次的回归以最终形态为准）、waitConnection 重建、毒消息 Nack、同主题先退旧再订新。
- transport-ws：会话注册 + Stop 关闭全部会话、控制帧与数据写串行、pong 续期防心跳误杀、慢消费者发送上限。
- transport-sse：分派克隆防重复 base64、发布时打时间戳（TTL 不再吞事件）、回放日志封顶、Last-Event-ID 严格大于、退订有界重试、Range 快照、Add 原子化、元数据事件不断连、显式 404。
- broker-{mqtt,kafka,nats,stomp,pulsar} 与 transport-mqtt：同主题退订次序、毒消息处理、错误隔离、幂等退订、 producers 重连重建、批量间隔零值守卫等。
- webtransport/h3/quic/axum/cron 经逐条核对为已覆盖或设计不适用：Rust 面以 endpoint 级真释放 + 逐连接 stop 竞速实现关停（强于上游 http3.Server 不可重启补丁的意图），axum 中间件短路是 tower 模型结构保证，cron 无 gRPC keepalive 对应面。

**bootstrap 线（kratos-bootstrap bc2912a..5968de4）**

- 仅 Go tag 注册表更新，无可移植实质。

**明确推迟**（feature 级，非缺陷）：

- 向量检索契约模块（vector/）与 qdrant/milvus/weaviate/neo4j 四引擎：整个向量域未移植，按需立项。
- JSONB 点路径过滤（638881f 修的 JsonValue 绑定缺陷）：Rust 线尚无 JSON 路径过滤能力（storage-proto 拒绝 json_path、seaorm 无 JSON 翻译），补能力时按其教训以类型化值绑定并测试 CONTAINS 命中与 EQ 精确两案例。
- 13→8 引擎口径差：上游新增 qdrant/milvus/weaviate/neo4j（向量），cassandra 仓库层补完对应 Rust 面为契约优先设计（scylla 驱动typed 行反序列化天然规避三缺陷）。

## 已完成域

| 域 | Rust crates | 状态 |
|:---|:---|:---|
| transport | rushwind-transport + axum/mqtt/quic/ws/webtransport/h3 | 完成（原有；webtransport/h3 后补） |
| storage | rushwind-storage + 12 引擎 | 完成（原有） |
| registry | rushwind-registry + 8 引擎 | 完成 |
| security (authn/authz) | rushwind-authn(-*) / rushwind-authz(-*) | 完成 |
| transaction | rushwind-transaction + dtm | 完成 |
| ai | rushwind-ai + openai | 完成 |
| config | rushwind-config + env/file/http/etcd/consul | 完成（契约+env/file 先行；http/etcd/consul 后补） |
| metrics | rushwind-metrics + prometheus/otel/datadog | 完成 |
| cache | rushwind-cache + local/redis | 完成 |
| health | rushwind-health | 完成 |
| ratelimit | rushwind-ratelimit + tokenbucket/bbr | 完成 |
| circuitbreaker | rushwind-circuitbreaker + vegas/sres/hystrix | 完成 |
| oss | rushwind-oss + s3 | 完成 |
| broker | rushwind-broker + mqtt/redis/nats/rabbitmq/stomp/kafka/pulsar | 完成 |
| retry | rushwind-retry | 完成 |
| tracer | rushwind-tracer | 完成 |
| testing | rushwind-testkit | 覆盖主用途（tracer 测试用的 graphql/thrift/protobuf 生成夹具未纳入） |

## 明确推迟域

| 域 | 推迟原因 |
|:---|:---|
| workflow | 契约仅 Client.Close + Worker.Stop/IsRunning；四个引擎（argo/conductor/goworkflows/temporal）都是重型外部系统的 SDK 适配器，各自独立成项 |
| pprof | pprof 是特定运行时的剖析机制，不跨运行时通用；Rust 生态对应物是 pprof-rs（Linux perf，本仓 CI 含 Windows 无法验证）与 tokio-console |
| log | 独立日志门面包装；工作区标准是 `tracing`，重复封装无收益 |
| ratelimit/sentinel | 无维护良好的 Rust SDK；直接阈值+拒绝规则可映射到 token-bucket 引擎 |
| circuitbreaker/sentinel | 同上，该适配面无维护良好的 Rust SDK |
| config/apollo | 携程 Apollo 配置中心 SDK，中国区专用，按需再实现（契约已可插拔） |

## 备注

- bootstrap 装配层 2026-09-15 起覆盖全部可装配域：命名实例族（authn/authz/broker/cache/circuitbreaker/ratelimit，各带应用侧注册的工厂表）与单实例族（ai/oss/metrics）经工厂表装配并在 `Bootstrapped`/`RouteInput` 上暴露；config 域按优先级列表组合进 FallbackSource；script 域经其自带工厂表装配进 Manager 并由关停清扫关闭；内置 http 服务器经 `HttpEdge`（中间件开关、CORS 两层、超时）与按子树的 `with_authn`/`with_authorization*` 包装（含 project 轴的固定/声明两种形态，互斥校验）；`/healthz`+`/readyz` 与 `/metrics` 挂载及 Prometheus 内置引擎走 bootstrap 的 `health`/`metrics` 特性，OTLP tracer 装配走 `trace` 特性，特性关闭时装配报错而非静默；cron 传输按注册任务名挂载。prometheus 与 tracer 引擎为此补了 `from_settings` 接线形状。明确不接：encoding（无设置面的全局自注册，应用一行 `register()` 即全部接线）、apalis 任务存储（作业载荷类型是应用知识）、retry（静态策略工具，无可装配对象）。
- config 域的 etcd/consul/http 源已实现；编译期文件嵌入源（Rust 侧对应 include_bytes!/include_str! 一类机制）暂未做成独立 Source，按需再补。
- broker 域的 kafka 已实现：samsa 纯 Rust 协议客户端，免 librdkafka C 构建工具链（消费侧 headers 取不到、无 leave-group 与分区扩容监视等差异见 crate 文档清单）。pulsar 已实现：pulsar-rs（6.9，仅启用 tokio-runtime + compression 特性以规避其默认特性里的 async-std 与 native-tls），载荷经 crate 的字节透传、headers 映射 user properties、key 映射 partition key（无 unsubscribe 请求、无生产者轮换重试等差异见 crate 文档清单）。仍推迟：nsq（唯一 Rust 客户端 0.0.3 止于 2017，无维护）、azuresb（azure_messaging_servicebus 0.21 止于 2024-10，源自已归档的 azure-sdk-for-rust）、rocketmq（crates.io 上唯一客户端 5.0.0 止于 2023-08；aliyun ONS 变体无 Rust SDK）、sqs/gcpubsub（官方 Rust SDK 均存在且活跃——aws-sdk-sqs、google-cloud-pubsub 1.4——但 localstack / gcloud emulator 容器各 2–3GB 超出 CI 拉取预算；契约已可插拔，需要时按需再实现）。
- ratelimit 的 clock 注入未实现：测试用真实短时长。
