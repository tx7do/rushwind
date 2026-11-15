# 09 · 注册发现与韧性

> 本章目标：掌握注册发现契约（`Registrar`/`Discovery`）与五种后端；熔断三算法、限流四算法与可组合重试。**live**：注册中心与配置中心的一致性验证需要对应容器。

前置：[01 · 生命周期与第一个服务](01-hello-rushwind.md)。

## 注册发现：两份能力，五个后端

`rushwind-registry` 把注册与发现拆成两个独立契约——**只会注册的服务不必实现发现，只会消费的服务不必实现注册**：

```rust
pub trait Registrar: Send + Sync {
    fn register(&self, registration: Registration) -> BoxFuture<'_, Result<RegistrationHandle, RegistryError>>;
    fn deregister(&self, registration: Registration) -> BoxFuture<'_, Result<(), RegistryError>>;
}
pub trait Discovery: Send + Sync {
    fn get_service(&self, service_name: &str) -> BoxFuture<'_, Result<Vec<Instance>, RegistryError>>;
    fn watch(&self, service_name: &str) -> BoxFuture<'_, Result<Box<dyn Watcher>, RegistryError>>;
}
pub trait Watcher { fn next(&mut self) -> BoxFuture<'_, Result<Vec<Instance>, RegistryError>>; fn stop(&mut self); }
```

要点：

- `Instance`（来自 `rushwind-transport`）= `{id, name, version, endpoints}`；注册键形如 `/microservices/{name}/{id}`。
- `register` 返回 `RegistrationHandle`：**Drop 即尽力注销**，持有它存活；`watch` 的 `next` 返回**全量快照**而非增量。
- 线格式（键与 JSON 值）是字节级稳定的，跨语言服务可以互认。

| 后端 | 存活机制 | watch 机制 | 标准端口 |
|:---|:---|:---|:---|
| `registry-consul` | TTL 心跳任务（可配 TCP 健康检查） | 阻塞查询 | 8500 |
| `registry-etcd` | 租约 + 自愈保活（TTL/3 刷新） | 前缀 watch | 2379 |
| `registry-polaris` | 无（不注销不过期，`deregister` 是唯一移除路径） | 轮询 discover | 8090 |
| `registry-servicecomb` | 30s 心跳 PUT | WebSocket 推送 + 指数退避重拨 | 30100 |
| `registry-zookeeper` | 临时节点 + 会话抖动守护任务 | 子节点/存在性 watch | 2181 |

```rust
let registry = ConsulRegistry::connect("http://127.0.0.1:8500")?;
let handle = registry.register(Registration::new(instance)).await?;   // 持有 handle = 保持注册
let instances = registry.get_service("order-service").await?;
```

## 熔断：一个协议，三种算法

契约就是 allow/mark 协议，外加一个 `execute` 便捷包装：

```rust
match circuitbreaker::execute(&breaker, || async { call_backend().await }).await {
    Ok(value) => /* ... */,
    Err(ExecuteError::CircuitOpen) => /* 快速失败：熔断开了 */,
    Err(ExecuteError::Inner(e)) => /* 业务错误 */,
}
```

直接用协议时守一条纪律：**`allow()` 放行后必须恰好调用 `mark_success`/`mark_failure` 之一**；`Err(Open)` 时两个都不能调。

| 引擎 | 算法 | 关键参数（默认） | 适用 |
|:---|:---|:---|:---|
| `circuitbreaker-hystrix` | 滑动桶窗口错误率；睡眠窗后半开单次试通行 | 错误率 0.50、量阈值 20、窗 10s | 经典按失败率熔断 |
| `circuitbreaker-vegas` | TCP Vegas 式 RTT 膨胀度 `(cur-base)/base` 超 α 开、低于 β 合 | α 0.5、β 0.3、预热 10 样本 | 延迟劣化早于错误出现的服务 |
| `circuitbreaker-sres` | Google SRE 概率接受 `requests − k·accepts` | k 2.0、窗 10s、40 桶 | 渐进式恢复、避免惊群 |

三者皆纯内存、无容器依赖。注意 **vegas 不看真实流量**——你得把每次 RTT 喂给 `record_latency(rtt)`。

## 限流：allow / wait / close

```rust
pub trait Limiter: Send + Sync {
    fn allow(&self) -> bool;                                              // 拒绝是 false，不是错误
    fn wait(&self) -> BoxFuture<'_, Result<(), RateLimitError>>;          // 等到有额度
    fn close(&self);                                                      // 唤醒所有等待者并报 Limited
}
```

- **`ratelimit-tokenbucket`**：经典令牌桶，启动即满桶（突发先给足）。`rate`/`burst` ≤ 0 是构造错误。`wait` 精确等待令牌缺口（每令牌 1/rate 秒）。
- **`ratelimit-bbr`**：BBR 自适应——按最小 RTT/QPS 窗口推算最大并发在途数。**请求结束必须调 `done(rtt)`**，否则在途槽泄漏、限流器逐渐失真。
- **`ratelimit-slidingwindow`**：滑动窗口日志——任意滑动窗内至多 `limit` 次，边界精确（无固定窗双预算缝），日志内存以 `limit` 为界。
- **`ratelimit-leakybucket`**：漏桶整流——背漏恒速排出、准入按 `drain_rate` 节流、空闲不攒额度（与令牌桶的突发语义相对）。两者都带注入钟（测试零真实 sleep）。

```rust
let limiter = TokenBucket::new(TokenBucketOptions { rate: 50.0, burst: 20.0 })?;
if limiter.allow() { handle(); } else { respond_429(); }
```

## 可组合重试：`Retrier`

```rust
let retrier = Retrier::default()
    .max_attempts(3)                                            // 含首次
    .backoff(Backoff::Exponential { initial: Duration::from_millis(200), factor: 2.0, max: Duration::from_secs(10) })
    .jitter(Jitter::Equal)                                       // None/Full/Equal
    .max_total_wait(Duration::from_secs(30));

match retrier.execute(|e| is_retryable(e), || call_flaky()).await {
    Ok(v) => v,
    Err(RetryError::MaxAttempts(last)) => /* 重试耗尽 */,
    Err(RetryError::Timeout) => /* 总预算耗尽 */,
}
```

判定谓词返回 `false` 的错误**立刻**以 `MaxAttempts` 上抛（一次重试都不烧）；总超时预算在每次睡眠前与失败后检查。

## 韧性三件套的组合姿势

```rust
// 典型外呼链：限流在最前（保护自己）→ 熔断（保护依赖）→ 重试（对抗抖动）
if !limiter.allow() { return Err(too_many()); }
circuitbreaker::execute(&breaker, || retrier.execute(|e| retryable(e), || call()).await
    .map_err(|e| match e { RetryError::MaxAttempts(e) => e, RetryError::Timeout => timeout() })).await
```

## 踩坑清单

- **`service_prefix` 前缀过匹配**：`order` 会匹配到 `order-service` 的实例——发现结果要按 `Instance::name` 再过滤一次。
- **consul 的 TCP 健康检查从容器内打宿主端口会全挂**：本地联调建议 `enable_health_check: false`（官方 live 测试正是这么做的）。
- **etcd 的注册句柄 Drop 只停保活任务，不主动 revoke**：租约按 TTL 自然过期（默认 15s），要立即消失就调 `deregister`。
- **polaris 注册的服务名是名字+方案直接拼接**（无分隔符的兼容行为），用裸名去发现会扑空——跨语言混布时尤其小心。
- **servicecomb 的发现视图有约 30 秒缓存**：注册后别指望立刻被发现，集成测试要给余量。
- **熔断 `execute` 之外手工用协议时**，放行后忘 mark 会让窗口统计悬空——优先用 `execute` 包装器。

## 动手练习

1. 用三个纯内存算法（hystrix/vegas/sres + tokenbucket/bbr + Retrier）写一个"自动降级到副本"的外呼封装，全部单元可测、零容器。
2. 起 Consul 容器，注册两个实例，用 `watch` 感知其中一个 `deregister`；再验证 handle Drop 后 TTL 到期自动消失。
3. 给第 07 章的发布中间件接上 `Retrier`，用谓词区分"可重试的超时"与"不可重试的业务拒绝"。

---
[系列目录](README.md) | 上一章：[08 · 缓存与存储装饰器](08-cache.md) | 下一章：[10 · 可观测性](10-observability.md)
