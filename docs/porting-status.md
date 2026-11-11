# rushwind 域实现状态

全部 20 个域的 Rust 实现收口记录。日期：2026-09-14。

## 契约扩面与引擎增量（2026-11-12）

九项一次落地（各带单元/一致性测试，fmt/clippy -D warnings 全仓干净）：

- **http-binding 二进制 proto 请求体解码**：`ResolvedCodec::Proto` 从透传改为 `DynamicMessage::decode` 真绑定，失败与其他 codec 同类回 400 `body unmarshal %s`（解码器自己的 prose）；空体仍跳过绑定（参考解码器的精确次序）。此前"需要二进制线格式的接口先别开"的告警解除。
- **oss 契约扩面（list + presign）**：契约加 `list(prefix, after, limit)`（升序键 + 严格 after 游标 + limit 页；零 limit = `InvalidLimit`）与 `presign(key, method, expires)`（缺省实现 = `Unsupported`——只有签名权威引擎可覆写）。s3 引擎两件都实现（ListObjectsV2 + `encoding-type=url` + `start-after` + max-keys 钳 1000；presign 即引擎本就说着的 query-string 签名），oss-local 实现 list（目录遍历 + 平台分隔符归一）、presign 诚实拒绝（file:// 无签名权威）；live 套件补 list/presign 用例（含"无凭据客户端可执行 presigned GET"钉子）。
- **ratelimit 引擎族 +2**：`rushwind-ratelimit-slidingwindow`（滑动窗口日志：任意滑动窗内至多 limit 次准入、边界精确无固定窗双预算缝、日志内存以 limit 为界）与 `rushwind-ratelimit-leakybucket`（漏桶整流：背漏恒速排出、空闲不攒额度——与令牌桶的突发语义相对），两者都带 `with_clock` 注入钟与 close-唤醒 waiter。
- **ai 契约三面**：`StreamingChatModel`（`ChatStreamEvent::{Content, ToolCall, Finish}`；工具调用分片在引擎侧拼装成完整调用才进事件面）与 `EmbeddingModel` 提为契约 trait（原为 openai 引擎私有扩展），`rushwind-ai-openai` 补齐 SSE 工具调用 delta 拼装（按 index 合并、EOF 无 finish_reason 也冲刷已到达的分片），对象安全钉死（`Arc<dyn …>` 是部署形状）。
- **script-wasm 能力面**：`GlobalAccessor` 真实现（wasm 标量 Int/UInt/Float/Bool、`env` 导入模块链接进每次实例化、注册表为唯一事实来源）+ `FunctionRegistrar::call_function`（按名调用导出函数，参数按签名严格编组——Bool→整型参数、Int/UInt 范围检查绝不截断、Float→f64/f32；算术不符与未知导出按 Failed 拒绝）。`register_function` 维持拒绝并写明原因：契约 HostFunction 是异步的而 wasmi 同步执行，诚实的桥需要一个 reactor，行内 poll 会死锁任何等待运行时原语的宿主函数。
- **config/nacos 源**（`rushwind-config-nacos`）：v1 cs/configs REST 读（原文直传、404=缺席、可选 tenant/accessToken），轮询 watch 沿 config-http 形状；mock server 套件钉读/缺席/构造守卫/watch 推送。
- **broker 毒消息契约前置面**：`NackAction::{Requeue, Drop}` + `Event::nack`（`Nacker`；隐式 ack 引擎为文档化 no-op——投递已落定，无物可退回）与 `with_error_handler`（ErrorHandler 观察点：包装 Handler，错误到达观察者但仍以 Err 面目冒出，投递结算权留在引擎投递循环）。`BrokerError` 补 Clone。**各引擎翻转 manual-ack 仍按需立项**——七个投递循环的逐引擎翻转换来的是另一场战役（kafka seek、rabbitmq manual-ack、redelivery 上限与 live 套件重验）。
- **storage IEXACT 操作符全矩阵**：契约 `Op::Iexact`（Unicode 折叠精确等值、text-only、一元）接进 wire（此前点名拒绝）、memory 评估器（折叠比较）、seaorm（`LOWER(col) = LOWER(?)`）、mongo（锚定折叠正则）、ES/OpenSearch（`term` 的 `case_insensitive`）、ClickHouse（`lowerUTF8`）；一致性套件与三方言快照各补钉子。REGEXP/IREGEXP 维持 wire 拒绝（前置：契约零依赖doctrine下的正则求值故事）；JSON_CONTAINS/ARRAY_CONTAINS/json_path 维持拒绝（前置：Value 协议先有 JSON/Array 种类——两个前置项都已写进架构文档的操作符表）。
- **文档改齐**：security-authn-authz.md 的"bootstrap 尚不装配认证"过时口径更正（bootstrap 经命名实例工厂表装配 authn/authz 并按声明挂 with_authn/with_authorization*）。

testkit 套件增量：storage 一致性套件增 iexact 钉子（折叠大小写不折叠形状）。


## 独立路线增量（2026-11-11）

不依赖上游三仓的独立功能面，六个模块一次落地（各带一致性/单元套件，fmt/clippy -D warnings 干净）：

- **密码哈希域**（`rushwind-password` + argon2/bcrypt/scrypt/pbkdf2 四引擎）：PHC 自描述串，验证恒由存量哈希内嵌参数驱动（bcrypt 的 MCF 同理），`needs_rehash` 承担升级闭环；Ok(false)（错密码）与 Err(InvalidHash)（坏行）严格分面；argon2 0.5 实际未实现 `PasswordVerifier`（文档示例陈旧），四引擎统一走"解析→内嵌参数重导出→常数时间比较"骨架。bcrypt 的 72 字节截断是算法本身行为，文档与钉子测试都如实钉住。
- **向量检索域**（`rushwind-vector` + memory 参考引擎）：移植 go-crud `vector` 模块语义——score 恒为越大越相似（余弦 `1−d`/L2 `1/(1+d)`/点积原值）、TopK 非分页、维度纪律（首个 upsert 定维、不一致拒绝）、pgvector 文本编解码、零向量 fail-neutral（余弦距离 1.0 而非 NaN）。契约零依赖（类型+纯函数）；外部引擎族（qdrant/milvus/weaviate/neo4j）仍按需立项，契约已可插拔。
- **分布式锁域**（`rushwind-lock` + memory/redis 引擎）：try-once 获取（等待是调用方重试策略，契约不隐藏活跃性问题）、所有权 token、TTL 强制（零 TTL 拒绝 InvalidTtl——liveness 全靠它）、属主校验的 release/extend（Ok(false) = 互斥已失效，绝不误删后继者的锁）。redis 引擎 = `SET NX PX` + token 比对 Lua；live 套件接进 CI broker 任务的 redis 容器，顺手把从未接线的 cache-redis live 套件也补上。
- **config 编译期嵌入源**（`rushwind-config-embedded`）：`include_bytes!`/`include_str!` 喂静态键→字节树；不可 watch 是链接期事实的诚实答案。
- **config/apollo 源**（`rushwind-config-apollo`）：手写 reqwest（house 的 S3/STOMP 手写先例）——configs REST 读（configurations 规范化为键序 JSON，404=缺席）+ notifications/v2 长轮询（304 窗口语义、长轮询超时=窗口+读超时、防即时应答服务器的热自旋 gap）；mock server 测试，无 SDK 依赖。
- **ratelimit clock 注入**：契约加 `Clock` 类型（`Arc<dyn Fn() -> Instant>`）与 `system_clock()`，tokenbucket/bbr 各加 `with_clock`——令牌记账与窗口旋转走注入钟，测试用假钟推进替代真实短 sleep（wait 路径的 sleep 仍真实时钟，文档写明）。tokenbucket 的 "clock 注入未实现" 备注销案。

testkit 新增三个特性门控套件：`password`（8 测试：三分面验证、盐唯一、needs_rehash 面、篡改摘要、空/Unicode 密码）、`vector`（8 测试：三 metric 排序、top-k 截断、upsert 替换、维度纪律、空集非错误、坏查询拒绝）、`lock`（9 测试：try-once、token 校验、伪造 token 不偷锁、TTL 失效、extend 延长/失效、键独立、零 TTL 拒绝）。

## 上游跟进（2026-11-05）

三个上游 Go 仓库（go-crud、kratos-transport、kratos-bootstrap）在移植基线（2026-09-14）之后的提交已逐条盘点并跟进：

**storage 线（go-crud a66fd69..638881f）**

- 多租户隔离修复：审计发现全部引擎的 `upsert`（部分引擎的 `create`）绕过 viewer 作用域——跨租户覆盖写（memory/mongodb/seaorm/es/os/cassandra/clickhouse/influx 全中招）；修复为统一探针语义：先经作用域读探针，可见才更新，不可见即插入面，主键被占一律 `Conflict`，绝不静默覆盖不可见行。influx 另修行协议换行注入（租户可控字符串可伪造第二个点）。
- 租户轴落地（go-crud `EnforceTenant` 语义移植）：`Viewer` 增 `Tenancy`（Unidentified 缺省 fail-closed → `StorageError::MissingViewer`；Platform 系统后台放行；Tenant(id) 全路径谓词注入 + 写入强制盖章）；`Schema` 增 `tenant_column` opt-in；一致性套件增租户模块（跨租户 get/update/delete NotFound、upsert Conflict 且原行完好、盖章不可迁移、token 分页租户隔离、无身份 fail-closed），宏签名要求 tenant factory——此前 CI 全绿正因套件从未钉住作用域下的 upsert。
- HTTP 边与装配：`CrudApi` 无 viewer 策略时由 all-access 改为缺省拒绝（fail-closed，`viewer: all` 显式放行）；租户列伪造守卫（作用域 viewer 不得写非本人 owner/unit/tenant 值）；`MissingViewer` → 400。
- 缓存装饰器 `ScopeKey` 增租户轴（同 actor 异租户不再共条目）。
- 其余 go-crud 修复经核对为已覆盖或结构不适用：软删无 `deleted_by`（与 531624f 修后语义一致）、分页 limit 已钳制（opensearch 同源缺陷的 Rust 面无恙）、gorm 主键回读/mongodb FindOne 选项/Rust 面结构不同、cassandra 三缺陷（gocql 一致性/大小写折叠/集合解码）为驱动专属、doris 不存在于 Rust 线。

**transport/broker 线（kratos-transport 035b95cc..d5f78b01，逐条 PORTED / 核对不适用）**

- broker-redis：**移植**——断开后 publish/subscribe 以 NotConnected 拒绝（原先惰性重拨反而成功）、断开幂等 + 订阅清理、密码连接（PR #132，serde 向后兼容）、同主题订阅登记表（旧流先退，ptr_eq 身份校验防陈旧句柄误删）。**核对不适用**——健康检查独立连接（本引擎无健康检查路径）、PONG 静默（无任何日志面）、重连地址重推（无 pool/addr 分裂）、stream AutoAck/XAUTOCLAIM（无消费组流，明示非目标）。
- broker-rabbitmq：**移植**——消费模型收敛为上游最终形态（同步 declare+bind + 单一再订阅循环 + 指数退避重拨，永不双消费者）、断开幂等 + 重连闭环、同主题登记表、失败登记不残留。**核对不适用**——毒消息 Nack（载荷透传无引擎侧解码、auto-ack 为文档化设计）。
- transport-ws / transport-sse：十六项上游缺陷**全部核对不适用**——Rust 形态不同（sse 为有界 broadcast ring、退订即 receiver drop、无共享可变事件、无回放日志；ws 为极简路由构建器，无会话管理器/心跳/客户端），但补了五个防回归钉子测试（多流分发载荷逐字节一致、慢消费者不阻塞发布、Stop 关闭全部会话、空载荷不断流、显式 404/405）。
- broker-mqtt：**移植**——断开真正停泵（原先 1 秒内自动复活并重挂订阅）、退订等 UNSUBACK（泵配对 FIFO，10 秒上限）、失败登记清理、陈旧句柄守卫。**核对不适用**——同主题先退旧再订新（处理器原子替换，按 MQTT 规范同过滤器服务端自替换）。
- broker-nats：**移植**——真断开 + 地址存储 + Connect 重建（原先 disconnect 为空操作）、flush 失败登记清理。
- broker-stomp：**移植**——断开次序 UNSUBSCRIBE→DISCONNECT（帧序依赖活连接）、重连重放订阅。
- broker-pulsar：**移植**——Connect 重建 client（原先断开后永久不可用）、同主题流登记表（原先双消费者分流丢消息）。
- broker-kafka：**移植**——key→id 映射。**核对不适用**——writer nil map（disconnect 清空而非置 nil）、批量间隔零值（无批量模式）、同主题（ additive 设计无覆盖面）。
- transport-mqtt：**移植**——重启真重订阅端到端钉子（嵌入式 broker 验证）；注册表锁/僵尸窗口等骨架缺陷在 Rust 面无对应结构。
- webtransport/h3/quic/axum/cron 经逐条核对为已覆盖或设计不适用：Rust 面以 endpoint 级真释放 + 逐连接 stop 竞速实现关停（强于上游 http3.Server 不可重启补丁的意图），axum 中间件短路是 tower 模型结构保证，cron 无 gRPC keepalive 对应面。
- **遗留契约缺口（2026-11-12 部分收口）**：契约已备好上游毒消息路由的两块前置面（`NackAction`/`Event::nack` 拒绝脸 + `with_error_handler` 错误观察点）；剩余工作是各引擎的 manual-ack 翻转（当前七引擎全部隐式 ack，见上方 2026-11-12 节），按需立项。

**bootstrap 线（kratos-bootstrap bc2912a..5968de4）**

- 仅 Go tag 注册表更新，无可移植实质。

**明确推迟**（feature 级，非缺陷）：

- 向量检索契约模块（vector/）与 qdrant/milvus/weaviate/neo4j 四引擎：整个向量域未移植，按需立项。
- JSONB 点路径过滤（638881f 修的 JsonValue 绑定缺陷）：Rust 线尚无 JSON 路径过滤能力（storage-proto 拒绝 json_path、seaorm 无 JSON 翻译），补能力时按其教训以类型化值绑定并测试 CONTAINS 命中与 EQ 精确两案例——前置是 `Value` 协议先有 JSON/Array 种类（2026-11-12 已写入架构文档操作符表）；IEXACT 这一族已在 2026-11-12 全矩阵落地。
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
| vector 外部引擎 | 契约 + memory 参考引擎已落地（2026-11-11）；qdrant/milvus/weaviate/neo4j 外部引擎仍按需立项，各自独立成项 |
| pprof | pprof 是特定运行时的剖析机制，不跨运行时通用；Rust 生态对应物是 pprof-rs（Linux perf，本仓 CI 含 Windows 无法验证）与 tokio-console |
| log | 独立日志门面包装；工作区标准是 `tracing`，重复封装无收益 |
| ratelimit/sentinel | 无维护良好的 Rust SDK；直接阈值+拒绝规则可映射到 token-bucket 引擎 |
| circuitbreaker/sentinel | 同上，该适配面无维护良好的 Rust SDK |

## 备注

- bootstrap 装配层 2026-09-15 起覆盖全部可装配域：命名实例族（authn/authz/broker/cache/circuitbreaker/ratelimit/lock/vector，各带应用侧注册的工厂表）与单实例族（ai/oss/metrics）经工厂表装配并在 `Bootstrapped`/`RouteInput` 上暴露；config 域按优先级列表组合进 FallbackSource；script 域经其自带工厂表装配进 Manager 并由关停清扫关闭；内置 http 服务器经 `HttpEdge`（中间件开关、CORS 两层、超时）与按子树的 `with_authn`/`with_authorization*` 包装（含 project 轴的固定/声明两种形态，互斥校验）；`/healthz`+`/readyz` 与 `/metrics` 挂载及 Prometheus 内置引擎走 bootstrap 的 `health`/`metrics` 特性，OTLP tracer 装配走 `trace` 特性，特性关闭时装配报错而非静默；cron 传输按注册任务名挂载。prometheus 与 tracer 引擎为此补了 `from_settings` 接线形状。明确不接：encoding（无设置面的全局自注册，应用一行 `register()` 即全部接线）、apalis 任务存储（作业载荷类型是应用知识）、retry（静态策略工具，无可装配对象）。
- config 域的 etcd/consul/http 源已实现；编译期文件嵌入源（rushwind-config-embedded，include_bytes!/include_str! 静态树）与 Apollo 源（rushwind-config-apollo，手写 REST + 长轮询）已于 2026-11-11 补齐。
- broker 域的 kafka 已实现：samsa 纯 Rust 协议客户端，免 librdkafka C 构建工具链（消费侧 headers 取不到、无 leave-group 与分区扩容监视等差异见 crate 文档清单）。pulsar 已实现：pulsar-rs（6.9，仅启用 tokio-runtime + compression 特性以规避其默认特性里的 async-std 与 native-tls），载荷经 crate 的字节透传、headers 映射 user properties、key 映射 partition key（无 unsubscribe 请求、无生产者轮换重试等差异见 crate 文档清单）。仍推迟：nsq（唯一 Rust 客户端 0.0.3 止于 2017，无维护）、azuresb（azure_messaging_servicebus 0.21 止于 2024-10，源自已归档的 azure-sdk-for-rust）、rocketmq（crates.io 上唯一客户端 5.0.0 止于 2023-08；aliyun ONS 变体无 Rust SDK）、sqs/gcpubsub（官方 Rust SDK 均存在且活跃——aws-sdk-sqs、google-cloud-pubsub 1.4——但 localstack / gcloud emulator 容器各 2–3GB 超出 CI 拉取预算；契约已可插拔，需要时按需再实现）。
