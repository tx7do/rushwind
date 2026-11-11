# 07 · 消息通信

> 本章目标：掌握 `Broker` 契约（发布/订阅/请求响应）、`Message`/`Event` 线形状、JSON 助手与中间件；学会按语义（而非习惯）选消息引擎。**live**：本章集成验证需要消息容器。

前置：无（建议先读[第 01 章](01-hello-rushwind.md)了解生命周期）。

## Broker 契约

```rust
pub trait Broker: Send + Sync {
    fn name(&self) -> &'static str;
    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;
    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;
    fn publish<'a>(&'a self, topic: &'a str, message: Message) -> BoxFuture<'a, Result<(), BrokerError>>;
    fn subscribe<'a>(&'a self, topic: &'a str, handler: Handler)
        -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>>;
    fn request<'a>(&'a self, topic: &'a str, message: Message)
        -> BoxFuture<'a, Result<Message, BrokerError>>;   // 默认未实现
}
```

`Message` 是引擎无关的线形状：`payload: Vec<u8>`、`key`（Kafka 分区键 / RabbitMQ 路由键）、`headers`/`metadata`、`partition`/`offset`。订阅侧的 `Event` 自带确认门：

```rust
let handler: Handler = Arc::new(|event| Box::pin(async move {
    // ...处理...
    event.ack().await?;          // 没有确认语义的引擎上是无操作
    Ok(())
}));
```

## JSON 助手

业务对象进出的标准姿势：

```rust
use rushwind_broker::{json_message, json_handler, Event};

#[derive(serde::Serialize, serde::Deserialize)]
struct OrderCreated { order_id: u64 }

let message = json_message(&OrderCreated { order_id: 7 })?;
let handler = json_handler(|event: Event, payload: OrderCreated| async move {
    tracing::info!(order = payload.order_id, topic = event.topic(), "consumed");
    Ok(())
});
broker.subscribe("orders", handler).await?;   // json_handler 返回的就是 Handler
```

## 七种引擎的取舍

| 引擎 | 构造 | 通配订阅 | header 载体 | request/response | 语义要点 |
|:---|:---|:---|:---|:---|:---|
| `broker-kafka` | `KafkaBroker::new(addrs)` + `connect` | ✗ | 发送侧有、投递侧无 | ✗ | 每订阅全新消费组，从最早偏移开始；偏移自动提交 |
| `broker-nats` | `NatsBroker::connect(addr).await` | `*`/`>` | ✗ | ✓（唯一原生支持） | 至多一次，ack 无操作 |
| `broker-pulsar` | `PulsarBroker::connect(addr).await` | ✗ | 用户属性 + 分区键 | ✗ | 固定共享订阅；非阻塞发送等回执 |
| `broker-rabbitmq` | `RabbitmqBroker::connect(url).await` | `*`/`#`（topic 交换机） | AMQP headers | ✗ | 发布走 `amq.topic`；每订阅匿名独占队列，自动 ack |
| `broker-mqtt` | `MqttBroker::new(options)` + `connect` | `+`/`#` | ✗ | ✗ | QoS 1；重连后全量重订阅 |
| `broker-redis` | `RedisBroker::connect(url)`（同步、惰性） | ✗ | ✗ | ✗ | pub/sub，至多一次，无持久化 |
| `broker-stomp` | `StompBroker::new(addr)` + `connect` | ✗ | ✗ | ✗ | 手写 STOMP 1.2 客户端，指数退避重连并重放订阅 |

全部引擎都有 `from_settings(serde_json::Value)`（bootstrap 工厂线格式），live 集成测试统一挂在 `live` 特性门后：

```rust
// 构造差异是刻意的：nats/pulsar/rabbitmq 异步急切拨号；
// kafka/mqtt/redis/stomp 同步构造，connect 才上线
let nats = NatsBroker::connect("nats://127.0.0.1:4222").await?;
let mqtt = MqttBroker::new(MqttOptions::new("127.0.0.1:1883"));
mqtt.connect().await?;
```

## 中间件

`MiddlewareBroker` 给发布与订阅两条路径各挂一串中间件（重试、打点、死信……）：

```rust
let broker = MiddlewareBroker::new(Arc::new(inner), publish_mw, subscriber_mw);
```

**先注册的中间件在最外层、最先执行**——与直觉相反，写重试（应最内）与打点（应最外）时注意方向。

## 踩坑清单

- **MQTT 的 client_id 是唯一身份**：两个连接同 ID，broker 会踢掉旧的。并行测试/多实例各用随机 ID（默认 `rushwind-mqtt-<rand>`），自定 ID 时保证全局唯一。
- **MQTT/STOMP 的 `connect` 有 10 秒握手窗口**，超时报 `... connect timed out`；未连接就 publish 是 `NotConnected`，不会排队。
- **Kafka 的分区列表按 topic 冻结**：运行中扩分区不会被感知（要感知就重建订阅）；header 发得出去收不回来（投递路径无 header）。
- **Pulsar 的订阅名是固定的共享订阅**；退订只关消费者不退服务端订阅，backlog 靠保留策略回收。
- **RabbitMQ 每次发布开新信道并等 confirm**：高频发布场景在契约外自行池化或批量。
- **退订要显式调用 `unsubscribe()`**：多个引擎的 Drop 退订只是尽力而为（锁被占就跳过）。
- **`request` 只有 NATS 实现**，其余引擎返回 `Failed("request not implemented by this engine")`——选型时这是硬约束。
- **毒消息面是契约级的两件套**：`Event::nack(NackAction::Requeue/Drop)`（隐式 ack 引擎是文档化 no-op）与 `with_error_handler(handler, observer)`——handler 错误到达观察者但仍以 `Err` 冒出，订阅不被杀死。当前七引擎全部隐式 ack；manual-ack 翻转按需立项。

## 动手练习

1. 用内存里能跑的方式先写消费逻辑（`json_handler` + 假 broker），再切 NATS 真容器（`live` 门）——验证"契约不动，引擎即构造细节"。
2. 写一个发布中间件：失败重试两次（配合[第 09 章](09-governance.md)的 `Retrier`），再写一个订阅中间件打耗时点；用 `MiddlewareBroker` 组装并确认执行方向。
3. 对比 Redis 与 NATS 各发一千条消息的吞吐，解释"至多一次"在两者上的不同表现。

---
[系列目录](README.md) | 上一章：[06 · 认证与鉴权](06-auth.md) | 下一章：[08 · 缓存与存储装饰器](08-cache.md)
