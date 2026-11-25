# panic 上报：可插拔的进程级异常捕获日志机制

本文是 panic 上报域（`rushwind-panic`）的权威设计文档：回答一个审计结论——"全仓没有异常捕获输出的日志机制"哪些部分成立、哪些不成立，以及把不成立的部分用一块可插拔的 brick 补齐、再深化成上下文传播平台。

## 现状与定位

审计把全仓的 panic 输出路径分了四层，本机制逐一闭合：

| 层 | 审计时的现状 | 现状 |
|:---|:---|:---|
| HTTP handler panic | recovery 层 `catch_unwind` 后自打一行 `rushwind.http` 日志 | recovery 层挂**请求上下文**（request_id/method/path）后 catch；报告由进程 hook 单行输出（target `rushwind.panic`），无双报 |
| App 生命周期 panic | 有捕获、无输出（`ServerError::Panicked` 只进返回值） | raise 时由 hook 报告；**终态聚合**由 bootstrap 的 `run_logged()` 补一条 `rushwind.bootstrap` 出口（core 声明 No logging 不破） |
| broker handler panic | 裸 `tokio::spawn(handler(event))`，无任何捕获/检查 | 全部 13 引擎的 handler spawn 走 [`spawn_dispatch`](../crates/rushwind-broker/src/lib.rs)：投递身份（topic + message id）挂进 panic 上下文 |
| 其余任意 panic | 只有默认 hook 的 stderr 一行 | 进程级 hook 全覆盖，默认 `TracingReporter` 进 tracing 管道 |

后三层被一个事实统一：**`panic::set_hook` 安装的 hook 在 `catch_unwind` 捕获之前触发**。panic 升起的那一刻 hook 就运行，无论后续有没有人捕获。所以不需要逐点包 `catch_unwind`——一个进程级分发 hook 覆盖一切。

为什么是独立 crate 而不进 `rushwind-core`：core 的 crate 文档明文声明 No logging，异常上报的默认后端（tracing）与该声明冲突。惯例同 `rushwind-metrics`：契约 + 后端各归其位。

## 形状

```rust
/// 插拔点：一个 reporter 就是一个订阅者。
pub trait PanicReporter: Send + Sync {
    fn report(&self, report: &PanicReport);
}

/// 一次 panic 的提升时快照。
pub struct PanicReport {
    pub message: String,              // &str/String downcast，未知载荷给兜底串
    pub location: Option<String>,     // file:line:col
    pub thread: Option<String>,       // raise 线程名（tokio worker 有名）
    pub sequence: u64,                // 入队序（队列锁下分配，FIFO 投递严格递增）
    pub context: Vec<(String, String)>, // scoped 上下文（broker: topic/message_id；HTTP: 请求身份）
}

/// 注册 reporter，返回守卫；守卫 drop 即摘除该 reporter。
pub fn plug(reporter: Arc<dyn PanicReporter>) -> PlugGuard;
/// 同类型去重版：同一 `TypeId` 的 reporter 被**替换**而非叠加。
pub fn plug_once<T: PanicReporter + 'static>(reporter: Arc<T>) -> PlugGuard;

/// 把上下文挂到 fut 的每次 poll 上——raise 时 hook 读得到。
pub fn scoped<K, V, F>(context: impl IntoIterator<Item = (K, V)>, fut: F) -> Scoped<F>;

/// broker 合同的接线点：每个 handler spawn 带投递身份。
rushwind_broker::spawn_dispatch(event, &handler);

/// payload 展开的共享形状（第三处归一点）。
pub fn payload_message(info: &PanicHookInfo<'_>) -> String;

/// 默认后端（feature `tracing`）：tracing::error!(target: "rushwind.panic", ...)。
pub struct TracingReporter;
/// metrics 桥（feature `metrics`）：panics_total 计数器，桥 rushwind-metrics。
pub struct MetricsReporter;
```

`PlugGuard` 不可 Clone——一个守卫对应一个注册位。守卫被长期持有（bootstrap 挂在装配体上、嵌方挂在自己选的位置），drop 语义是"摘除我这个 reporter"，**不是**"卸载 hook"。

## 分发管线：hook 只做入队，reporter 在 worker 上跑

**std 的硬法律（本机 stable 1.100 实验钉死）**：panic hook 执行期间 raise 的任何 panic 无条件 abort 进程——hook 里的 `catch_unwind` 救不了，双重 panic 的裁决发生在 raise 时、任何 catch 之前。这条法律决定了架构：**不能在 hook 里直接调用户 reporter**。

所以 dispatch 管线是两级：

1. **hook（panic 机器内）**：调捕获的原 hook（stderr 语义保持）→ 把消息、位置、线程名、scoped 上下文提取成 `PanicReport`（值快照）→ 队列锁下分配 sequence 并入队 → **同步等待 flush 完成才返回**。
2. **worker 线程（panic 机器外）**：常驻单线程，FIFO 出队，持注册表锁遍历 reporter、逐个 `catch_unwind` 调用，完成后放行等待中的 dispatch。

三个关键性质从这个位置关系里长出来：

- **崩溃必达（crash-flush）**：致命的未捕获 panic，其 unwind 要等报告送达才继续——崩溃路径上的日志不会丢。
- **reporter panic 不杀进程**：reporter 的调用点在 panic 机器之外，worker 线程的计数从零起，`catch_unwind` 是真的 catch。一个总在 panic 的 reporter 只损失它自己的那份报告，链上它后面的 reporter 照常收到。worker 自己的 raise 会再入全局 hook——线程本地旗标让 worker 线程上的 dispatch 只调原 hook，报告回环就此断掉。
- **背压天然有界**：dispatch 同步等 flush，一条线程同时最多压一个 job——队列深度 ≤ 正在 panic 的线程数 ≤ 进程线程数。panic 风暴是背压减速，不是内存堆积；队列上限这个想法正式不需要。

原 hook（std 默认或第三方）与 hook 内我们的自身机器（快照提取的分配）若 raise，仍是 std 法律的 abort——那是 std 的地板，不是本设计的选择，文档如实声明。

## 上下文传播（scoped）

`scoped(context_pairs, fut)` 把键值对挂到包装 future 的**每次 poll** 上：std 线程本地 + poll 进入时 set / 退出（含 unwind）时恢复。panic 在 poll 内 raise 时 hook 恰好读得到——report 的 `context` 就来自这里。

- **执行器无关**：不用 `tokio::task_local`，不引运行时依赖，任何 poll 该 future 的运行时都成立。
- **嵌套组合**：内层作用域遮蔽外层，poll 结束（含 panic unwind）恢复外层——守卫栈纪律。
- **broker 接线**：合同 crate 的 [`spawn_dispatch`](../crates/rushwind-broker/src/lib.rs) 是全部 13 引擎唯一的 handler 投递口——topic 恒挂，message id 有则挂。handler panic 的报告带投递身份，不只是代码位置。
- **HTTP 接线**：recovery 层把请求身份（request_id/method/path）挂进上下文，然后 catch unwind 回信封。为此 request-id 中间件**提到最外层**（`HttpEdge` 装配次序：request-id, recovery, logging, CORS, timeout）——recovery 读得到 `RequestId` 扩展。HTTP panic 从两行日志（hook 一行 + recovery 一行）变成**一行带请求身份的报告**。
- 成本：每次包装一个内层 `Box::pin` + 一份键值对（每投递两个小分配）——投递热路径上的诚实代价，换来的是报告可定位到投递而非仅到代码。

## 无竞态模型

约束起点：`set_hook` 是 OS 级进程单槽，本 crate 把全局性收在"分发器 + 常驻 worker"这一个物理事实里，插拔面只暴露 reporter 列表。全部竞态类逐项关闭：

| 竞态 | 关闭方式 |
|:---|:---|
| 两线程同时 install，take/set 交错吞链 | install **只发生一次**（首次 plug 时，`OnceLock::get_or_init` 的闭包里完成 capture + spawn + set_hook，整体在互斥内）；此后任何插拔只动注册表，永不碰 `set_hook` |
| 守卫 drop "恢复原 hook" 的 last-writer-wins | **恢复操作不存在**。hook 装上就不再卸；空注册表的投递是空握手，行为等价，没有可观察差异——卸载这个概念从 API 里消失 |
| 摘除时 reporter 正在处理 in-flight panic | worker **持注册表锁调用** reporter：unplug 的守卫必须等本轮结束，严格撤销，零 in-flight 残留 |
| reporter 自身 panic | worker 隔离模型（见上节）——panic 发生在 panic 机器之外，`catch_unwind` 真实生效；worker 再入走原 hook-only 路径 |
| worker 中途死亡 → dispatch 永挂 | 双保险：belt `catch_unwind` 包住整个投递轮次，机器自身 raise 则清空队列（所有挂起 Sender 随 job 丢弃关闭，等待方走"worker 消失"路径）+ `WORKER_DOWN` 原子旗标让后续 dispatch 不再等待——丢一份报告好过挂死进程 |
| dispatch 持锁 unwind → Mutex 中毒锁死机制 | 两把锁（注册表、队列）访问全部容毒（`into_inner`）；注册表只是 `Vec`，队列只是 `VecDeque`，无不变式可破坏 |
| bootstrap/测试反复装配 → 重复上报或重复 install | `plug_once` 按 `TypeId` 替换（互斥内原子）；install-once 保证 worker 恰有一个 |

诚实的边界（是全局串行化的自然结果，不是竞态）：

- 投递顺序 = 报告 raise 的入队顺序（单 worker FIFO，`sequence` 严格递增可观测）；reporter 顺序 = 注册顺序；并发 plug 的相对次序 = 锁获取次序，不预设、不可依赖。
- 一个慢 reporter 会阻塞它之后的所有报告投递与并发 unplug——panic 是罕见路径，该代价只在异常发生时存在。
- `report` 内调用 `plug`/`plug_once`/drop 守卫会死锁（worker 持注册表锁跨调用）——trait 文档明文禁止。
- 第三方后装的 hook 若不回调 prev 会遮蔽本机制——文档钦定用法：**进程启动时尽早 plug**。与 tracer 的"provider 显式值"哲学同构：全局的只有分发器槽位本身，插拔面是显式值。

## bootstrap 装配与 run_logged

配置文档顶层小节（非引擎、无工厂，不需要 `EngineConfig`）：

```yaml
panic_reporting:
  enabled: false   # 缺省 true
```

装配（`Bootstrap::build`）在 `enabled` 为真时 `plug_once(TracingReporter)`，守卫挂在 `Bootstrapped` 上——**拔除随装配体生存**：装配体 drop，默认 reporter 摘除。`plug_once` 的 TypeId 幂等让同一进程多次装配只保留最后一份。嵌方要换后端：关掉开关，拿自己的 reporter `plug`（要 metrics 一起上就再 `plug(MetricsReporter::new(engine))`——两个后端共存，一个答"多常"、一个答"何处"）。

`Bootstrapped::run_logged(external)` 是带出口的 run：非协作终态（`Panicked`/`Timeout`/`Failed`）打一条 `tracing::error!`（target `rushwind.bootstrap`）。`rushwind-core` 自己零日志是 crate 声明，这条日志线归装配层。

## 测试

panic crate（`--features tracing,metrics` 14 钉全绿）：

- 生命周期钉：plug → panic → reporter 收到；guard drop → 不再收到（严格撤销）。
- **hook 先于 catch_unwind**：`catch_unwind` 包住 panic，reporter 照收——机制覆盖 App 层的证明钉。
- `plug_once` 幂等：同类型两次注册替换生效、陈旧守卫 drop 不伤活注册、不同类型共存。
- **隔离钉**：总在 panic 的 reporter → 进程存活、链上后继照常收到、其 panic 不回环进链。
- **次序钉**：两个 reporter 按注册顺序收到同一 panic；`sequence` 投递严格递增（单线程 FIFO）。
- **并发 FIFO 全序钉**：4 线程 × 100 panic 共享一只 recorder，投递序列严格递增且恰好 400 条——并发下的投递顺序。
- **字段钉**：thread 名 = raise 线程名。
- **上下文钉**：scoped future 内 panic 带键值对；作用域外 panic context 为空。
- **嵌套钉**：内层作用域遮蔽外层、poll 结束（含 unwind）恢复外层。测试自身踩过 `futures::executor::block_on` 不可嵌套（线程本地 Enter 守卫）的坑——单执行器 + `FutureExt::catch_unwind` 是正确形状。
- **输出钉**：`TracingReporter` 事件形状对着捕获订阅器逐字段断言（Some 臂与 None 臂各一；订阅器关 ANSI，否则 `k=v` 字面量被色码拆断）。
- **metrics 钉**：`MetricsReporter` 每报告计一次 `panics_total`。
- payload 展开：`&'static str` / `String` / 未知载荷三形状。
- 并发压测：4 线程 × 200 轮 plug/panic/unplug，不丢、不挂、不死锁。

接线钉：

- broker 合同（10 钉全绿）：`spawn_dispatch` 直接钉——panicking handler 的报告带 topic + message_id（引擎的 52 套件只证明投递语义不变，这颗钉证明上下文真的挂上了）。
- HTTP（18 钉全绿）：handler panic → 500 信封 + 报告上下文含 method/path/**request_id**（同时钉住 request-id 最外层的新次序）；**兜底钉**——standalone recovery（无 request-id 层）上下文只有 method/path，request_id 键必须缺席而非空串。
- bootstrap（21+15+2 钉全绿）：配置节缺省开/显式关；守卫挂装配体；`run_logged` 协作终态透传 Ok、非协作终态透传 Err。
- 引擎（13 crate、52 套件全绿）：`spawn_dispatch` 对既有离线一致性套件透明——投递语义零变化，只是报告多了身份。

## Benchmark

仓库家传形状（`benches/` + criterion + 叙事注释，storage 线两处先例）。panic 线两处，`cargo bench -p` 可复跑：

| 基准 | 本机数值（2026-11-25，Windows debug bench） | 回答的问题 |
|:---|:---|:---|
| `scoped/trivial-poll` vs `plain/trivial-poll` | 150 ns vs 1.8 ns——**wrapper 每次 poll 约 150 ns** | 每投递/每请求都付的钱（Box::pin + 上下文对表 + TLS 换位） |
| `dispatch/spawn-dispatch` vs `dispatch/bare-spawn` | 1.97 µs vs 1.53 µs——**每投递附加约 440 ns** | broker 合同接线的每投递代价 |
| `panic/report-roundtrip` | **6.8 µs** 端到端 | 罕见路径的数量级：capture → 入队 → worker 握手 → 送达 |

结论形状：**无事发生时几乎免费（百纳秒级），有事发生时有界（微秒级）**——机制的成本故事与它的定位一致。

工具链事实（2026-11 验证）：本机 stable 1.100 上 `panic::take_hook` 返回 `Box` 而非 `Arc`；hook 内 raise 的 panic 无条件 abort；`MutexGuard` 不实现 `io::Write`（tracing 捕获订阅器要自写 Writer）；`payload_as_str` 1.91 才稳定；`futures::executor::block_on` 线程本地 Enter 守卫、不可嵌套。均以实验钉死并按现状实现。

## 设计决策清单

| 主题 | 决策 |
|:---|:---|
| 机制形状 | 进程级单分发 hook（install-once、永不卸）+ 常驻 worker 投递 + 可插拔 reporter 注册表 |
| hook 内不做的事 | 不调 reporter（std 法律：hook 内 raise 无条件 abort，`catch_unwind` 救不了）——hook 只提取快照、入队、等 flush |
| 崩溃路径 | dispatch 同步等 flush：未捕获 panic 的 unwind 在报告送达后才继续，崩溃日志必达；队列深度天然 ≤ 并发 panic 线程数，不需要上限 |
| reporter panic | worker 线程隔离 + 逐调用 `catch_unwind`：损失一份报告，不损失进程 |
| 上下文传播 | `scoped` 用 std TLS 包每次 poll——执行器无关、零运行时依赖；broker 走 `spawn_dispatch` 单口，HTTP 走 recovery 挂请求身份 |
| HTTP 双报 | recovery 不再自打日志：报告只有 hook 一行，带请求上下文；request-id 提到最外层是前提 |
| App 终态 | `run_logged()` 在 bootstrap——core 的 No-logging 声明不破 |
| 插拔点 | `PanicReporter` trait，显式 `Arc` 值；全局性只存在于物理 hook 槽位与单个 worker |
| 撤销语义 | 严格撤销：worker 持锁调用，无 in-flight 残留；代价是慢 reporter 阻塞后续投递 |
| 同类型重复注册 | `plug_once` 按 `TypeId` 替换；`plug` 保持叠加语义 |
| worker 之死 | belt catch + 清队关闭挂起 Sender + `WORKER_DOWN`：丢报告好过挂死 |
| 默认后端 | `TracingReporter`（`rushwind.panic`）；`MetricsReporter`（`panics_total`）为可选第二后端，两者共存 |
| broker 投递身份 | topic 恒挂、message id 有则挂——原生无 id 的引擎（memory/rabbitmq/mqtt）只有 topic |
| rust-version | 本 crate 独立抬到 1.82（`PanicHookInfo` 类型名 1.82 稳定；payload 展开手写 downcast 保持地板） |
