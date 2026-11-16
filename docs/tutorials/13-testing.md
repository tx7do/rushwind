# 13 · 测试之道

> 本章目标：掌握本框架的测试方法论——契约一致性套件、live 特性门、容器化验证环境；学会为自己的引擎实现接入同一套一致性测试。

前置：任意章节；本章是全系列的收束。

## 测试哲学：契约，而非实现

框架里每个域的核心 crate 都定义契约，而 `rushwind-testkit` 把"契约必须被满足"变成可执行的套件。引擎实现者们不各自发明测试，而是把构造函数接进同一个宏：

```rust
// crates/rushwind-transport-axum/tests/conformance.rs —— 全文骨架
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use rushwind_transport::Server;
use rushwind_transport_axum::AxumServer;

fn axum_server() -> Arc<dyn Server> {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = AxumServer::new(bind, Router::new()).expect("ephemeral bind must succeed");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::axum_server);
```

这一行宏展开后是完整的生命周期一致性用例：端点可读、`start` 恰好一次、停机信号被体面处理、`Cancelled` 与真故障的区分……你新写一个 `Server` 实现，接上宏就继承了全部既有结论。存储引擎同理——`examples/storage-basics` 里"两个引擎跑同一 scenario"演示的正是这套思路的应用层形态：**同一场景跨引擎输出一致**。存储域的套件是一个家族：主套件（`rushwind_storage_conformance_suite!`，全部引擎必接）之外，JSON/Array 算子与点路径（`rushwind_storage_json_suite!`）、regex 族（`rushwind_storage_regex_suite!`）是**采用制**——引擎带着方言面就采纳钉住，没有面（如 SQLite 无 regexp 函数）就如实拒绝、不采纳套件而用 crate 内快照钉自己的 SQL 与拒绝面；装饰器（cache/observe/soft-delete/tree）也采纳，钉的是**透明性**——装饰不能改变语义。

## 两层测试：纯单元与 live

仓库的集成测试分两层，靠特性门切开：

- **纯单元层**：默认运行，零外部依赖。熔断、限流、重试、内存引擎、配置回退链、注册中心线格式……凡是算法与数据结构，都在这一层。
- **live 层**：`#![cfg(feature = "live")]`，只在显式 `cargo test -p <crate> --features live` 且对应容器在位时运行。每个 live crate 的地址都有环境变量可覆盖，默认对准本机标准端口。

```bash
# 只跑纯单元层（默认）
cargo test -p rushwind-circuitbreaker-hystrix

# 起了 NATS 容器后跑 live 层
docker run -d --name nats -p 4222:4222 nats:2
cargo test -p rushwind-broker-nats --features live

# 地址不在默认端口时用环境变量指路
BROKER_NATS_ADDRESS=nats://10.0.0.8:4222 cargo test -p rushwind-broker-nats --features live
```

## live 环境变量速查表

| 变量 | 默认值 | 域 |
|:---|:---|:---|
| `BROKER_MQTT_ADDRESS` | `127.0.0.1:1883` | 消息 |
| `BROKER_NATS_ADDRESS` | `nats://127.0.0.1:4222` | 消息 |
| `BROKER_KAFKA_ADDRS` | `127.0.0.1:9092` | 消息 |
| `BROKER_PULSAR_ADDRESS` | `pulsar://127.0.0.1:6650` | 消息 |
| `BROKER_RABBITMQ_URL` | `amqp://guest:guest@127.0.0.1:5672` | 消息 |
| `BROKER_STOMP_ADDRESS` | `127.0.0.1:61613` | 消息（RabbitMQ stomp 插件） |
| `BROKER_REDIS_URL` / `CACHE_REDIS_URL` | `redis://127.0.0.1:6379` | 消息 / 缓存 |
| `CONSUL_HTTP_ADDR` | `http://127.0.0.1:8500` | 注册 / 配置 |
| `REGISTRY_ETCD_ENDPOINT` / `CONFIG_ETCD_ENDPOINT` | `http://localhost:2379` | 注册 / 配置 |
| `REGISTRY_POLARIS_ADDRESS` | `http://127.0.0.1:8090` | 注册 |
| `REGISTRY_SERVICECOMB_ADDRESS` | `http://127.0.0.1:30100` | 注册 |
| `REGISTRY_ZOOKEEPER_ADDRESS` | `127.0.0.1:2181` | 注册 |
| `APALIS_PG_DATABASE_URL` | `postgres://rushwind:rushwind@127.0.0.1:5432/rushwind` | 任务队列 |

容器资源紧张的机器建议用非常规宿主端口（如 `-p 35432:5432`）避开本机已有服务，再用环境变量指过去。

## 端到端：从装配文档到 curl

单测之上的最后一层是 bootstrap 端到端——`examples/bootstrap-demo` 本身就是模板：一份 YAML 装出存储 + CRUD 面 + 路由包，跑起来后用 curl 逐条打：

```bash
cargo run -p bootstrap-demo &
# 从打印里拿端点
curl http://127.0.0.1:<port>/health     # 路由包：健康
curl http://127.0.0.1:<port>/wired      # 路由包：装配注入验证
curl -X POST http://127.0.0.1:<port>/widgets -d '{"name":"demo"}'   # CRUD 面
```

把它复制成你的服务冒烟测试：CI 里跑纯单元层 + 端到端冒烟，夜里或发布前跑全量 live 层。

## 大仓库的验证策略

百余个 crate 的 workspace，全量并发测试在资源受限的机器上会碰到链接器并发压力（症状是偶发的链接失败，与代码无关）。两个实用策略：

1. **按 crate 验证**：改动哪个域就 `-p` 那个 crate 及其直接依赖，快速回路。
2. **分层跑**：日常只跑纯单元层（`cargo test --workspace` 不开任何 `live` 门），live 层按域排期。一致性套件保证"没动的引擎不会因为契约变化而悄悄坏掉"——这正是把测试写进契约的回报。

## 踩坑清单

- **live 测试默认对准本机端口**：容器映射到非标准端口时**必须**设环境变量，否则测试会打到本机真服务上（或干脆超时）。
- **并行 live 测试共享外部资源**：MQTT 的 client_id 冲突会互踢连接、Kafka 的消费组互抢分区——并行前确认测试间资源隔离（各 crate 的 live 套件已按此设计，自己新加的用例要遵守）。
- **apalis worker 构造后不 start 就不就绪**：直接用 `Worker::new` 会停在不可 Claim 的状态——生产经 ReadinessLayer 翻就绪位，测试里构造后立刻 `worker.start()`。
- **别在纯单元层测试里打外部地址**：哪怕"只是 localhost 上的 Redis"——它会把你CI 里没容器的失败变成玄学。外部依赖属于 live 层。
- **一致性宏测的是契约不是性能**：吞吐/延迟回归用 criterion 类基准另立门户，别往 conformance 套件里塞计时断言。

## 动手练习

1. 写一个"每秒 ping 一次、pong 一次"的玩具 `Server` 实现，接上 `rushwind_conformance_suite!`，观察哪条一致性用例抓到了你第一次实现里的停机 bug。
2. 给第 08 章的缓存装饰器写一个双引擎一致性场景：`MemoryRepo` 与 `SeaRepo::sqlite_memory` 跑同一 scenario，断言输出逐字节一致。
3. 搭一套属于你的 live 环境：一个 compose 文件拉起 NATS + Redis + Consul，配好环境变量，全量 live 层绿灯。

---

## 系列终了

十三章走完，你已经拥有：一条生命周期主线（`App`/`Server`/`StopSignal`）、一条装配路径（`Bootstrap`）、十几个域的契约与引擎矩阵，以及把它们粘起来的测试方法。接下来的最佳路径是读 `examples/`——`axum-admin` 是全栈单进程的完整形态。祝构建顺利。

[系列目录](README.md) | 上一章：[12 · 多协议传输](12-transports.md)
