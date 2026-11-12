# 12 · 多协议传输

> 本章目标：掌握传输矩阵——WebSocket、QUIC、HTTP/3、WebTransport、MQTT 消费桥、cron 定时任务；理解统一的会话准入中间件（门/策略/计数）。

前置：[01 · 生命周期与第一个服务](01-hello-rushwind.md)。

## 会话中间件：传输间的公共语言

传输矩阵里所有面向连接的协议共享一套准入词汇（`rushwind-transport`）：

```rust
pub trait HandshakeGate: Send + Sync {
    fn name(&self) -> String;
    fn inspect(&self, handshake: &Handshake) -> GateVerdict;   // Continue | Reject(Rejection { status, reason })
}
```

三条契约纪律：**门是同步、纯 CPU、不做 IO** 的（要查库/调远程，把结果预先算好带进来）；HTTP 系的握手快照只有 `headers`，裸 socket 系只有 `remote` 地址——**缺席的证据必须保持缺席**；准入在正式接手前还会被原子计数器二次复核，超帽的连接立刻关闭，槽位永不泄漏。

## WebSocket：`WsRoute`

```rust
use rushwind_transport_ws::WsRoute;

async fn echo(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        if socket.send(msg).await.is_err() { break; }
    }
}

let (route, bus) = WsRoute::new()
    .gate(Arc::new(TokenGate))            // 握手门：看得到 header 对
    .max_concurrent_sessions(16)          // 准入上限
    .session_handler(echo)
    .build()?;                            // 没注册 session_handler 会 build 失败

let server = AxumServer::new(addr, Router::new().route("/ws", route))
    .with_aux_shutdown(bus);              // bus 搭档：停机时把所有会话一起拉倒
```

坑：**bus 必须交给 `with_aux_shutdown`**——不注册的话，会话只能等进程退出才结束。门的快照只有 HTTP 头（`remote` 恒为 `None`），按来源地址限流在这条路上做不到。

## QUIC：`QuicServer`

*取自 `examples/quic-gateway`，可 `cargo run -p quic-gateway`：*

```rust
let server = QuicServer::builder(quinn_config()?)     // 你提供带证书的 quinn::ServerConfig
    .gate(Arc::new(LoopbackOnlyGate))                 // 看 peer 地址
    .max_concurrent_sessions(16)
    .handshake_timeout(Duration::from_secs(5))
    .session_handler(echo_session)
    .build(SocketAddr::from(([127, 0, 0, 1], 0)))?;

fn quinn_config() -> quinn::Result<quinn::ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    quinn::ServerConfig::with_single_cert(
        vec![cert.cert.into()],
        rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key.serialize_der().into()),
    )
}
```

端点形如 `quic://{addr}`；门拒绝 → `refuse()`（静默拒绝），超帽 → 显式 `connection.close`（quinn 的连接 Drop 不等于关闭）。QUIC 恒 TLS，证书是你的事，示例用 rcgen 自签。

## HTTP/3 与 WebTransport

- **`transport-h3`**：门跑在**每个请求**上（快照是请求头）；连接级准入帽，超帽关整条连接（H3 多路复用所致）。没有路由框架——`request_handler` 完全拥有响应路径。端点 `https://{addr}`。
- **`transport-webtransport`**：wtransport 底座，门快照含 `:authority`/`:path` 伪头 + 头表；拒绝即丢弃会话请求（该阶段没有拒绝响应，客户端表现为连接被关）。构造要 `wtransport::Identity`（证书链），恒 TLS。端点 `webtransport://{addr}`。

三者（QUIC/H3/WebTransport）的 builder 面一致：`gate` + `max_concurrent_sessions` + `handshake_timeout` + handler + `build(addr)`——会一套等于会三套。

## MQTT 消费桥：`MqttBridge`

`rushwind-transport-mqtt` 不是 broker——它是把**外部** MQTT broker 的消息泵进应用生命周期的消费桥。*取自 `examples/mqtt-ingest`，可 `cargo run -p mqtt-ingest -- 127.0.0.1:1883`：*

```rust
let bridge = MqttBridge::builder(broker_addr, "rushwind-mqtt-ingest-demo")
    .subscribe("demo/#")                                  // QoS 1
    .session_handler(|message| async move {
        println!("[ingest] {} = {}", message.topic, String::from_utf8_lossy(&message.payload));
    })
    .build()?;

let app = App::builder().name("mqtt-ingest-demo").version("0.0.1")
    .erased_server(Arc::new(bridge)).build();
app.run(StopSignal::new()).await?;
```

特性：**处理器串行执行是刻意的背压**（QoS 1 会在 broker 侧排队）；无限重连（退避 100ms→5s 封顶），每代连接重放全部订阅；通配符过滤永不匹配 `$` 开头的系统主题；每订阅可单独 `subscribe_with_qos_handler` 覆盖默认处理器。

## gRPC：tonic 的 Routes 接进生命周期

`rushwind-transport-grpc` 把 tonic 0.14 的 `Routes` 表接进 `Server` 契约——与 axum 适配器同一条映射规则（构造期急切绑定、排水在 start 里、`stop()` 为空）：

```rust
use rushwind_transport_grpc::GrpcServer;
use tonic::service::Routes;

// 服务面来自 proto-build 的 tonic generator：编译域 proto、覆写 stub、装配进 Routes。
let server = GrpcServer::new("0.0.0.0:50051".parse()?, Routes::new(my_service))?;
```

crate 自身不带 proto（corpus-free）：生成器已在 `rushwind-proto-build` 里（`tonic` 特性），部署一行接线。端点回 `grpc://host:port`。

## cron：分钟对齐的定时服务器

`rushwind-transport-cron` 把定时任务做成 `Server`，与 HTTP/QUIC 服务同生命周期：

```rust
use rushwind_transport_cron::{CronJob, CronServer, CronSpec};

let server = CronServer::new("cron://ops")
    .with_job(CronJob::new("nightly-sweep", CronSpec::parse("30 3 * * *")?, || Box::pin(async {
        // 每日 03:30 的清扫
    })))
    .with_job(CronJob::new("health-report", CronSpec::parse("*/5 * * * *")?, || Box::pin(async {
        // 每 5 分钟
    })));
```

语义四条：**分钟对齐**（30 秒一格的 tick，只在整分触发）；错过即跳过（慢循环不补跑）；**处理器是分离任务**（慢任务不拖延下一 tick）；`stop` 会等在飞处理器跑完。字段支持 `*`、值、区间、列表、`*/n`；星期域是 cron 编号（周日 = 0，输入 7 也认）；**本地时区**。

## 踩坑清单

- **所有 builder 的 `build` 都会在"缺 session_handler"时失败**——fail-closed，没有默认会话行为。
- **QUIC 系三兄弟恒 TLS**：本地联调要用自签证书 + 客户端跳过校验（webtransport 的 dev 依赖开了 `dangerous-configuration` 仅为测试）。
- **cron 的任务没跑完进程就停会怎样**：`stop` 等在飞任务，但 `stop_timeout` 兜底强杀——长任务自己写检查点。
- **MQTT 桥的 `$SYS` 主题永远收不到**：通配符设计如此；监控 broker 自身状态请直连 broker 的管理面。
- **门的 IO 违约会毒化整条路径**：契约要求门同步纯 CPU，把异步查询塞进 `inspect` 是设计错误，改为预计算 + 缓存。

## 动手练习

1. 给 ws-gateway 示例加一个"缺少 `X-Demo-Token` 拒绝 401"的 `HandshakeGate`，用 wscat 验证 401/升级成功两态。
2. 用 `transport-cron` + 第 10 章的健康检查写一个"每分钟体检、连续三次 down 就打点告警"的巡检服务。
3. 把 quic-gateway 的 `LoopbackOnlyGate` 改成"仅允许 CIDR 段"，体会门快照里 `remote` 的用法（对照：HTTP 系没有它）。

---
[系列目录](README.md) | 上一章：[11 · 脚本引擎](11-script-engines.md) | 下一章：[13 · 测试之道](13-testing.md)
