# 架构

本文是 RushWind 的权威设计文档：写给两类读者——实现适配器的人（需要精确理解契约语义），以及审阅核心安全性的人。

## 工作区布局与职责边界

| crate | 依赖 | 职责 |
|:---|:---|:---|
| `rushwind-transport` | `tokio-util`（仅 `CancellationToken` 包装） | 契约：`Server` trait、`StopSignal`、`Instance`、`ServerError`。**唯一允许"只有接口"的 crate** |
| `rushwind-core` | `tokio`（signal/time/sync）、`futures`（`FuturesUnordered`、`catch_unwind`） | 生命周期编排。不持有任何业务概念 |
| `rushwind-storage` | 无（刻意零依赖） | 存储契约：`Repository` trait、`Schema`/`Record`/`Value` 动态协议、三种分页、过滤器树、Viewer 租户、审计钩子 |
| `rushwind-storage-cache` | `tokio`（仅 oneshot） | Cache-Aside 装饰器：SingleFlight 合并发、作用域键、generation 防陈旧回填。任意 `Repository` 之上可叠 |
| `rushwind-testkit` | 上两者 + `tokio::time`（探针延迟） | 一致性套件（传输 + 存储两套）。见文末清单 |
| `examples/*` | 按需 | `publish = false` 的演示程序 |

适配器 crate（P1 起）依赖 `rushwind-transport`（契约）与它们各自的协议栈（axum、quinn、rumqttc……），**永不依赖 `rushwind-core`**——编排是被调用的，不是被引用的。存储引擎 crate 同理：依赖 `rushwind-storage` 与各自的驱动（SeaORM/sqlx、官方 mongodb crate……），契约 crate 本身零依赖，任何引擎都能采用而不锁定用户。

## 分发模型：对象安全 trait + 借用生命周期装箱

`Server` 的三个方法里，`start`/`stop` 返回 `ServerFuture<'a>`——绑定到借用 `&self` 的装箱 future。这个形状是三个约束的交点：

1. **异构集合**。一个 App 持有不同具体类型的服务器，需要 `Vec<Arc<dyn Server>>`，因此 trait 必须对象安全，方法必须返回装箱的 `dyn Future`。
2. **无任务边界**。装箱 future 携带借用生命周期（`'a`），编排器用 `FuturesUnordered` 在**同一个任务内**并发推进所有服务器——不需要 `'static`，不需要 `tokio::spawn`，因此也不存在 abort 语义、JoinError 语义和任务泄漏问题。借用检查器静态保证没有一个服务器 future 逃出 `run` 的作用域。
3. **适配器作者零样板成本**。每个生命周期方法体是一行 `Box::pin(async move { ... })` 加普通 async 代码；类型其余部分完全是惯用的。

代价：每次 `start`/`stop` 调用一次堆分配（每服务器每生命周期各一次，非每请求）。被否决的备选：

| 备选 | 否决理由 |
|:---|:---|
| `#[async_trait]` | 引入 proc-macro 依赖，生成物与本手写形状完全相同——多一层魔法，无收益 |
| RPITIT（`-> impl Future`） | trait 不可对象安全，异构集合需要 enum 分发，适配器集合退化为闭集 |
| 泛型 App（单型服务器） | 无法表达"管理面 + 会话面混跑"这一立项场景 |
| JoinSet + spawn | 强制 `'static` 与所有权转移，触发 abort/JoinError 语义分支，且服务器句柄被任务边界割裂 |

## 生命周期规范

`App::run` 的行为是固定且可测试的：

| 阶段 | 行为 | 预算 |
|:---|:---|:---|
| 启动 | 全部 `Server::start` 并发推进（`FuturesUnordered`） | 无 |
| 触发等待 | 等待四个触发源之一：OS 终止信号、`App::stop()`、调用方外部信号、任一服务器退出（错误/自退/panic 均算） | 无 |
| 排水 | 触发后，信号已发出；仍在运行的服务器获得**全额预算**协作退出；截止时仍悬挂的 future 被 `clear()` **丢弃**（drop 即取消） | `stop_timeout`，从触发时刻起算 |
| 阶段 2 | before 钩子顺序执行，结果刻意丢弃 | 每钩子独立 `stop_timeout`，钩子开始时现取现造 |
| 阶段 3 | 全部 `Server::stop` 并发执行，每个独立包 `tokio::time::timeout`；panic 被 `catch_unwind` 捕获转为记录 | 每服务器独立，同上 |
| 阶段 4 | after 钩子，同阶段 2 | 同上 |

**预算规则**：任何阶段的截止时刻都在该阶段开始时才计算，绝不复用更早的时刻。复用更早时刻的预算有两类经典失效：停机预算从运行起点派生会让超时形同虚设；钩子超时在运行开始时创建会在正常运行期间就被耗尽。

**错误聚合**：`Cancelled`（协作退出）在聚合时被过滤；其余的启动侧首个错误优先，否则停止侧首个错误。panic 与超时分别记录为 `Panicked` / `Timeout`。

**丢失语义说明**：排水循环的 `select!` 使用 `biased;` 且流分支列首。原因：非 biased 的 select 在多分支同时就绪时随机选择，落选的 `starts.next()` 分支携带的已完成项会被整体丢弃——错误记录随之丢失。biased + 列首消除了这一竞态。这是 `tokio::select!` 文档中"分支顺序在 biased 模式下决定优先级"语义的正确用法，无饥饿风险（信号分支就绪时流分支必然 Pending，反之亦然）。

## 适配器停机映射（以 axum 为例）

契约只规定可观察语义（endpoint 先于启动解析、启动在信号时返回、stop 释放资源），不规定适配器如何把协议栈自身的能力映射到语义上。axum 适配器（`crates/rushwind-transport-axum`）选择的映射：

| 协议栈能力 | 契约语义 |
|:---|:---|
| 构造期 eager bind + `local_addr` | `endpoint()` 返回真实 `scheme://host:port`（含 `:0` 的 OS 端口分配）；绑定失败在注册时暴露而非启动时 |
| `axum::serve(...).with_graceful_shutdown(stop.wait())` | 信号触发停止接受并排空在途请求——排空发生在 `start` 内部；完成后 `start` 返回 `Cancelled` |
| 排空完成后栈自身关闭监听 | `stop()` 为空操作：编排层停止阶段到达时监听已关闭，无物可释放 |
| （兜底）| 未完成的连接随 start future 一起被排水截止丢弃——适配器无需自实现硬超时 |

留给后续适配器的两条规则：

1. **资源释放必须穷尽栈的原生机制**。axum 的优雅停机就是 hyper 的原生能力；适配器自排空、自管理连接池都是重复造轮子，且排空语义不可能比栈更正确。
2. **`stop()` 为空不代表停机可选**。axum 之所以空，是因为它的栈把全部停机语义内置到了 serve 的返回路径里。没有原生排空机制的栈（会话型传输普遍如此）必须在 `stop()` 里实现真实释放，编排层的截止是兜底而非替代。

两条规则各有一个现成案例：规则 1 是 axum 适配器（hyper 的 `with_graceful_shutdown` 全程承载，`stop()` 为空）；规则 2 是 quic 适配器——quinn 的端点是长驻对象、不接受循环的存续而存续，`stop()` 因此做**真实释放**（`Endpoint::close`，连带关闭其拥有的全部连接），这是本仓库里 `stop()` 非空的唯一现行示例。

会话型传输（WS，及 P2 的 QUIC/MQTT 桥）不经过本节的生命周期映射表——它们的门链、准入策略与会话停机总线是独立的一层契约，见 [session-middleware.md](./session-middleware.md)。

## 取消模型

三层，从软到硬：

1. **协作信号**：`StopSignal`（`CancellationToken` 薄包装）。契约要求服务器实现竞速 `stop.wait()` 并以 `Cancelled` 退出。这是唯一"优雅"的路径，服务器有机会自行清理。
2. **drop 弃置**：排水截止后，未退出的启动 future 被 `FuturesUnordered::clear()` 丢弃。Rust 中丢弃 future 即取消——不执行任何代码，不运行析构外的清理，直接消失。这是对不合作服务器的硬约束。
3. **超时截断**：阶段 3 的每个停止 future 被 `timeout` 包裹，截止即被丢弃并记录 `Timeout`。

注意层 2 和层 3 的清理是**进程级兜底**——被 drop 的 future 里的 socket 等资源由各自的 `Drop` 实现释放（tokio 原语均正确实现），但服务器自己的"优雅清理逻辑"（排空队列、通知对端）不会运行。因此适配器作者应当把"信号 → 退出"路径实现得尽可能短。

## Panic 语义

`futures::FutureExt::catch_unwind` 包裹每个生命周期 future。panic 在服务器边界被捕获、字符串化为 `Panicked` 记录、兄弟服务器的清理照常执行——这是套件中两个用例（panic 隔离、级联不因 panic 中断）验证的语义。

边界：`panic = abort` 的发布构建中 `catch_unwind` 无效，进程直接终止。RushWind 不对此做假设；文档化的立场是：**运行 RushWind 的进程应使用 unwind panic 策略，或接受 panic 即进程死亡的语义**。

## 存储契约：动态协议替代运行时反射

`rushwind-storage` 的核心命题是「一套泛型 Repository 驾驭多种引擎」。动态性通常靠运行时反射获得；Rust 没有运行时反射，动态性被显式化为一个小协议面：

| 决策 | 理由 |
|:---|:---|
| `Schema`（表/列/类型）+ `Record`（BTreeMap 行）承载动态字段 | 引擎从 Schema 派生自身机制；字段遍历确定性（测试与审计可复现） |
| `QueryCtx` 按值传递（`Viewer` + `Arc<dyn Auditor>`） | clone 便宜（Arc）；单引用 + owned ctx 让装箱 future 的生命周期与传输契约同样简单 |
| `FilterExpr` 递归树（`All`/`Any` 任意嵌套） | 内存引擎逐条求值即规范语义，SQL 引擎翻译为 `sea_query::Condition`，套件钉死两者一致 |
| 三种分页表达为 `Paging::{Page, Offset, Token}` | Token 游标 = 主键升序流（URL-safe base64 编码的末位 id）；与自定义排序组合是 `InvalidQuery` 而非静默忽略 |
| 软删除与生命周期钩子刻意不入 P0 契约 | 软删除是引擎侧约定（`deleted_at` + 默认过滤），硬进契约会绑架无此概念的引擎 |

三条硬性义务（套件强制）：

1. **Viewer 双轴作用域是权限边界**：所有路径（get/list/count/update/upsert/delete）必须施加两根正交的作用域轴——**数据范围轴**（`DataRange`：ALL/UNIT/USER/OWN/NONE）与**租户轴**（`Tenancy`：`Unidentified` 对租户表一律 `MissingViewer` 报错 fail-closed、`Platform` 放行、`Tenant(id)` 全路径谓词注入且写入强制盖章）；越界的行与不存在的行**不可区分**（`get` 返回 `Ok(None)`，写返回 `NotFound`）。写路径无豁免：`upsert` 先经作用域读探针，可见才更新、不可见即插入面，主键被占一律 `Conflict`——绝不静默覆盖不可见行（跨租户覆盖正是这套探针要堵死的洞）。
2. **create 回填主键**，所有写操作返回完整存储行。
3. **过滤器翻译必须忠实**（含嵌套组），或以 `InvalidQuery` 拒绝——校验先于引擎触达。

### 语法层：proto 契约是唯一事实来源

两个设计支柱在此落地。其一，**查询语法的操作符分类学**（`EQ`/`LIKE`/`CONTAINS`/`GTE`/`IS_NULL`……29 个，血统直追 Django ORM 的 field lookups）；其二，**契约由 proto 定义**——`proto/rushwind/storage/v1/query.proto` 是线格式的唯一事实来源，`FilterExpr`/`FilterCondition`/`PaginationRequest`/`Sorting`/`FieldMask` 均定义于此，任何语言的服务交换字节一致的消息。

`rushwind-storage-proto` 从这份 proto **生成**两种产物（prost 出类型、pbjson 出 protojson serde），生成物永不手写，线格式因此不可能漂移。`wire` 模块把生成类型翻译成契约类型，29 操作符映射规则：

| wire 操作符 | 契约归属 |
|:---|:---|
| `EQ` `EXACT` | `Eq` |
| `NEQ` `GT` `GTE` `LT` `LTE` | 同名直映 |
| `LIKE` `NOT_LIKE` `ILIKE` | `Like` `NotLike` `Ilike` |
| `IN` `NIN` | `In` `NotIn` |
| `IS_NULL` `IS_NOT_NULL` | 同名直映 |
| `BETWEEN` | `Between` |
| `CONTAINS` `STARTS_WITH` `ENDS_WITH` | 同名直映 |
| `ICONTAINS` `ISTARTS_WITH` `IENDS_WITH` | 折叠为 `Ilike` 模式（`%v%` / `v%` / `%v`）——每个 SQL 引擎已说着的方言 |
| `REGEXP` `IREGEXP` `JSON_CONTAINS` `ARRAY_CONTAINS` `EXISTS` `SEARCH` `IEXACT`；`date_part`/`json_path` 扩展 | **`Unsupported`**，在 wire 边界点名拒绝，绝不静默丢弃 |

三种分页经 `PaginationRequest` oneof 进入：`NoPaging` 映射为 `Offset{0, MAX_LIMIT}`（契约允许的最大窗口）。protojson 之外，契约 crate 还内建 **AIP 文本子集**解析器（`FilterExpr::from_aip`，零依赖手写）：`name = "bolt" AND (age >= 10 OR role IN ("a","b"))`，AND 优先于 OR、并列即隐式 AND、`NOT` 翻转有精确补运算的操作符——与 protojson 两条路汇入同一棵 [`FilterExpr`] 树，端到端测试钉死两条路在引擎上的结果一致。

枚举值的 protojson 形式是 proto 成员名（`"IS_NULL"`，不是驼峰）；FieldMask 用 `{"paths":[...]}` 消息形状，而非 well-known 类型的逗号串。

引擎矩阵按「SQL 全家 + 文档库」铺开：

| 引擎 crate | 后端 | 离线验证 | live 套件 |
|:---|:---|:---|:---|
| `rushwind-storage-memory` | 进程内 | 27 例套件 + 语义基准 | — |
| `rushwind-storage-seaorm` | **SQLite / PostgreSQL / MySQL**（三后端同启） | 27 例套件（SQLite 内存库）+ 8 例三方言 SQL 快照 | CI service 容器（`--features live`，`STORAGE_DATABASE_URL`） |
| `rushwind-storage-mongodb` | **MongoDB**（官方驱动） | 翻译层 7 例（FilterExpr→BSON，离线）+ LIKE→正则转义钉死 | CI service 容器（`--features live`，`MONGODB_URI`） |

SQL 三方言的语句渲染由快照测试逐字钉死（占位符风格 `$n` vs `?`、LIMIT/OFFSET 绑定、ILIKE 折叠），live 容器套件验证的是连接、事务与真实服务器的方言行为——两层互相兜底。MongoDB 的 `LIKE` 族编译为转义加锚定的正则（SQL 通配符语义），元字符绝不逃逸成通配；生成主键用 `max(pk)+1`（文档库没有 rowid 别名），`batch_create` 的严格原子性需要副本集事务，单机部署下是单命令尽力语义——两处都在引擎文档里言明。内存引擎的存在不是多余的样例——它让「同一过滤器树、多引擎、逐行一致」成为套件可执行的断言，而非文档承诺。

横切层以装饰器表达：`rushwind-storage-cache` 把 Cache-Aside + SingleFlight 模式包成任意 `Repository` 之上的透明层。两条租户攸关的设计决策——**缓存键含 viewer 全部作用域轴**（数据范围 + 租户轴：`own(1)` 与 `own(2)` 永不共享条目，同 actor 异租户同样不共享，缓存无法跨租户泄漏），以及**失效即递增 per-key generation**（写事务落地前已出发的加载不得用旧行回填缓存）——各有一条行为测试钉死；装饰器本身还须整套通过 40 例一致性套件（30 例契约 + 10 例租户），证明其透明性。

同一手法延伸到软删除：`rushwind-storage-soft-delete` 把经典 ORM 的软删语义做成引擎无关的装饰器——表声明一个 `deleted_at` 整数列，`delete` 变为墓碑写入，所有读路径（get/list/count/update 目标）过滤墓碑，`restore`/`purge`/`list_deleted` 显式 opting out；`upsert` 写入可见世界（墓碑 id 以新数据复活）。审计话语权归装饰器：delete 审计为 `Delete`（尽管底层是 `Update`），落库写走无审计 sink 的静默上下文。装饰器本身过全套 40 例透明性套件（30 契约 + 10 租户）+ 9 例行为测试。

DTO↔Entity 映射落在 `rushwind-storage-macros`：契约 crate 定义 `ToRecord`/`FromRecord` 一对 trait，derive 宏为受支持的标量模型（`String`/`i64`/`f64`/`bool` 及其 `Option`）生成实现——`None` ↔ `NULL`，缺失或错型字段即 `InvalidQuery`。映射在编译期完成，全程无需运行时反射。

最后两块积木各归其位。树形查询不进契约、也不进引擎——`rushwind-storage-tree` 把整棵树的词汇表（`children`/`roots`/`ancestors`/`subtree`/`is_ancestor`）表达为契约级查询的组合：约定一个 `parent_id` 整数列，children/roots 是过滤列表，subtree 是逐层广度扫描，环损坏报 `InvalidQuery` 而非死循环，悬空父 id 如根截止。代价是深子树每层一次 list——SQL 引擎日后可用递归 CTE 出专用快路径，而任何引擎（含装饰器栈）第一天就能用。可观测性同理不绑栈：`rushwind-storage-observe` 只发 `tracing` span（`rushwind.storage`，带 `table`/`op`/`outcome`），导出到 OpenTelemetry 是 subscriber 侧（tracing-opentelemetry）的选型——观测栈是用户的底板，RushWind 只递积木。

最后一块积木把整条线接到线上：`rushwind-storage-axum` 把任意 `Repository` 挂成 CRUD 路由（GET/POST/PATCH/PUT/DELETE），列表查询的两种线上语法在 HTTP 边界双入口——`?q={protojson}` 原样收下 protojson 请求文档，散参数则面向临时调用方（`filter` 走 AIP 文本、`sort=field:dir`、`fields` 掩码、三种分页参数）。写路径的 JSON body 经 schema 校验（未知列、类型错位即 400），错误taxonomy映射为状态码（NotFound→404、InvalidQuery→400、Conflict→409、Unsupported→501、MissingViewer→400）。租户在边界收口，且**缺省拒绝**：未安装 viewer 策略的 `CrudApi` 以 `Viewer::default()`（NONE 范围 + 无租户身份）执行一切请求——忘了装策略的后果是请求被拒而非数据裸奔，全访问是 `with_viewer(|_| Viewer::all())` 的显式选择；已装策略时，载荷里伪造的租户列/`owner_id`/`unit_id`（与 viewer 身份不符的值）在触达引擎前即 400 拒绝（引擎底层的强制盖章是其后的第二道闸）。作用域强制仍然由引擎在每次调用时执行——HTTP 层只负责把身份变成作用域，从不越权放行。

## 生命周期设计决策

| 决策 | 理由 |
|:---|:---|
| `StopSignal` 单独传参，超时由编排器施加、服务器不可见预算 | 服务器只需响应信号，预算是编排器的职责 |
| 并发推进用 `FuturesUnordered`，借用 future，无 spawn | 见分发模型一节 |
| `catch_unwind` 在服务器边界兜底 panic，记为 `Panicked` | panic 语义显式化、可测试 |
| 排水有截止，悬挂 future 被 drop | 对不合作服务器有硬边界 |
| 钩子结果一律丢弃 | 无日志门面；失败钩子绝不能阻塞后续清理——"记录但继续"在此加强为"丢弃且继续" |
| 信号集为 SIGTERM + SIGINT（Unix），Ctrl+C（Windows） | SIGQUIT 在现代部署中语义是 core dump，不适合优雅停机 |
| 终局经 `watch` 通道关闭 + `OnceLock` 发布，发布在关闭前完成 | 发布/观察之间有明确的先行发生关系，无数据竞争窗口 |

## 一致性套件清单

`rushwind_conformance_suite!` 生成的全部用例（CI 对每个适配器强制）：

| 用例 | 验证 |
|:---|:---|
| `shutdown_cascades_on_server_failure` | 一台服务器失败 → 兄弟服务器仍被清理，终局为错误 |
| `shutdown_phases_run_in_order` | 事件序严格为 before 钩子 → teardown → after 钩子 |
| `stop_phase_deadline_is_enforced` | 挂死的 `stop()` 被截止切断，终局为 `Timeout` |
| `ill_behaved_server_is_abandoned_at_deadline` | 无视信号的服务器在排水截止被弃置，生命周期正常完结 |
| `panicking_server_is_isolated_and_siblings_still_tear_down` | panic 被隔离为记录，兄弟清理不受影响 |
| `endpoint_is_uri_shaped` | 适配器：`endpoint()` 返回 `scheme://authority` |
| `cooperative_shutdown_completes` | 适配器：真实服务器在信号下干净退出 |

套件自检：`crates/rushwind-testkit/tests/conformance_self.rs` 以内置探针跑通全部用例，验证套件本身有效。

`rushwind_storage_conformance_suite!` 生成的全部用例（CI 对每个存储引擎强制，`--features storage`）：

| 用例组 | 验证 |
|:---|:---|
| CRUD 七件（create 回填主键/全列往返/get 缺失 None/update 只补丁给定字段且越权即 NotFound/delete 双向语义/批写原子性冲突回滚/upsert 插改一体） | 契约的写路径义务与 NULL 往返 |
| 比较符/模式符/NULL/集合区间/实数数值比较 | 18 个操作符语义；模式符为 SQL 通配（`%`/`_`），`ilike` 折叠大小写 |
| `and_or_groups_nest` | `All`/`Any` 任意嵌套翻译与求值一致 |
| `invalid_filters_are_rejected_before_the_engine` | 未知列、操作符元数以 `InvalidQuery` 拒绝 |
| `sorting_asc_desc_and_secondary` | 多级排序，主键兜底决胜 |
| `paging_page_mode` / `paging_offset_mode` / `paging_token_stream_covers_everything_once` | 三种分页；游标流全覆盖、不重不漏、pk 升序 |
| `paging_and_sorting_violations_are_rejected` | page 0、Token+自定义排序、垃圾游标 → `InvalidQuery` |
| `field_mask_projects_returned_rows` | 掩码行只含掩码列，无掩码行全列 |
| `viewer_*` 四件 | ALL/NONE 边界、OWN 隔离（读不到、写不动、原行无损）、UNIT/USER 范围 |
| `count_and_exists_honor_the_scope` | 计数与 exists 同受作用域约束（越界行不满足任何谓词） |
| `create_on_an_occupied_pk_conflicts` | 显式主键撞已占行即 `Conflict`，原行无损 |
| `upsert_never_touches_an_out_of_scope_row` | 探针语义：不可见行走插入面，被占即冲突，绝不覆盖不可见行 |
| `audit_entries_flow_to_the_sink` | create/update/delete 产生 `AuditEntry`（读审计是引擎策略，不钉死） |

**租户模块**（每个引擎的 tenant factory 对 `tenant_suite_schema` 再跑一遍）：

| 场景 | 钉住的语义 |
|:---|:---|
| `create_force_stamps_the_tenant` / `batch_create_stamps_every_row` | 租户角色伪造租户列无效，盖章覆盖每一行 |
| `platform_create_respects_explicit_tenant` | 平台视图可显式指定租户（代租户建行） |
| `tenant_cannot_migrate_a_row_by_patching_the_tenant_column` | patch 租户列无法把行迁入他租户 |
| `list_and_count_are_tenant_scoped` / `token_paging_is_tenant_scoped` | 列表/计数/token 流只见本租户 |
| `cross_tenant_get_is_not_found` / `cross_tenant_update_and_delete_are_not_found` | 跨租户读/改/删 ≡ 不存在（防存在性泄露），原行无损 |
| `cross_tenant_upsert_conflicts_and_leaves_the_foreign_row_intact` | 跨租户 upsert 冲突且原行完好——CI 此前全绿正因缺这道钉子 |
| `unidentified_viewer_fails_closed_on_every_path` | 无身份 viewer 在租户表上全路径 `MissingViewer` 报错，绝不静默放行 |
