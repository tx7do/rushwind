# 会话中间件：契约与映射

本文是会话中间件链的权威设计文档：契约（`rushwind-transport/src/session.rs`）、各传输的执行点、会话停机总线、以及明确的非目标。与 [architecture.md](./architecture.md) 的关系：那份管 `Server` 生命周期契约；本文管生命周期契约**之外**的会话层。

## 模型

会话型传输（WS、QUIC、webtransport、h3、MQTT 消费桥；后续 tcp）的握手发生在任何认证上下文存在之前。HTTP 族中间件（tower）永远看不到会话握手——所以会话层有自己的两件契约设施：

**门链（`GateChain` / `HandshakeGate`）**。每个门对握手快照（`Handshake`：HTTP 族填 headers，裸套接字填 remote，从不猜测缺失的证据）给出同步裁决，链按注册顺序评估、首拒截断。门按契约**同步且纯 CPU**：只验证本地证据（签名、允许列表、头部形状），永不执行 IO。需要远程证据的裁决（令牌内省、撤销检查）属于门背后的进程内缓存，或未来的异步门层。`rushwind-authn` 的 `AuthenticationGate` 是门链的第一个具体消费者——把任意认证引擎接为门，见 [security-authn-authz.md](./security-authn-authz.md)。

**准入策略（`SessionPolicy`）**。每路由/每监听器级配额。会话上限在 axum-ws 上为升级前预检加升级回调入口的原子准入双检；在 quic 上为连接建立时刻的原子准入；两者共用契约层的 [`SessionCounter`]——单一实现、单点测试，配额槽位因 guard 的 drop 语义（含任务中止路径）不可能泄漏。握手超时在 quic 上为 `Connecting` future 与截止的竞速。未配额的路径零计数、零开销。

两个设施都在契约 crate 里定义，评估语义对所有传输**逐字节相同**——这正是"跨传输统一中间件"的落点：统一的是链和策略，不是会话类型本身。

## 执行矩阵

| 机制 | axum-ws（HTTP 族） | h3（HTTP 族，h3/h3-quinn） | webtransport（wtransport） | quic（裸 UDP，quinn） | mqtt 桥（消费桥） | 其余裸传输（tcp） |
|:---|:---|:---|:---|:---|:---|:---|
| 门链评估 | HTTP 请求时刻，升级之前；拒绝 = 403 响应，无升级发生 | 请求时刻；拒绝 = 携带拒绝状态码的 HTTP 响应（拒绝 reason 无载体，被丢弃） | 会话请求时刻（`:authority`/`:path` 伪头 + 头表进入快照）；拒绝 = 丢弃会话请求（该阶段无协议拒绝响应原语，客户端看到连接关闭），reason 记录无载体被丢弃 | 握手尝试时刻；拒绝 = `Incoming::refuse()`，协议原生拒绝原语 | **N/A**——单一预信任 broker 连接，无握手面；broker 凭证属连接配置 | 各自握手回调内；拒绝 = 协议拒绝原语 |
| 会话上限预检 | 升级前检查活跃计数 → 429 | 无预检（同 quic：无两段窗口） | 无预检（同 quic） | 无预检（无先升级后准入的两段窗口） | **N/A**——单连接，无会话multiplex | 待定 |
| 会话上限准入 | 升级回调入口的原子准入检查；竞态通过者直接关闭 | 连接建立时刻的原子准入检查；超限连接显式 `Connection::close` | 连接建立时刻的原子准入检查；超限连接显式 `Connection::close`（wtransport `Connection::close`） | 连接建立时刻的原子准入检查；超限连接显式 `Connection::close` | **N/A** | 待定 |
| 握手超时 | N/A（hyper 的升级即时完成，无可观察的握手窗口） | `Connecting` future 与截止竞速，超时即丢弃、握手中断 | 会话请求 future（QUIC 握手 + 请求到达）与截止竞速，超时即丢弃 | `Connecting` future 与截止竞速，超时即丢弃、握手中断 | **N/A**（broker 侧由连接超时与 keepalive 覆盖） | 待交付 |
| 未认证字节预算 | N/A（升级完成前无帧） | 待交付（QUIC 层认证前的飞行字节计量） | 待交付（QUIC 层认证前的飞行字节计量） | 待交付（QUIC 层认证前的飞行字节计量） | **N/A**（订阅即认证后） | 待交付 |
| 每远端配额 | 不可用（HTTP 族监听器暂不暴露对端地址；ConnectInfo 管道是前置条件） | 不可用（同 axum-ws） | 不可用（同 axum-ws） | **已交付（2026-11-13）**：`SessionPolicy::max_sessions_per_remote` + `SessionCounter::admit_with_remote` 按 peer IP 原子计数，空闲条目从映射表摘除（表不随对端数增长） | **N/A**（对端是 broker 本身） | 待交付 |
| 对端地址可见性（`Handshake.remote`） | `None` | `None` | `None` | `Some(peer)`——门可据实裁决；`gates_observe_peer_address` 用例固定 | **N/A**（无门；对端是 broker 本身） | 各自协议提供则填 |

mqtt 消费桥是矩阵里的第三种形态：它不是"面向不可信客户端的服务器"，而是**通往预信任基础设施的单条消费连接**——会话中间件的全部机制对它都是 N/A，其安全边界是 broker 连接配置（凭证/TLS）与应用层对不可信 payload 的处理。契约只收编当下可执行的字段：`SessionPolicy` 目前含会话上限与握手超时（quic 路径上两者均有真实执行点），字节预算与每远端配额等其执行点落地时随语义一起定义。

h3 与 webtransport 是矩阵的第二列、第三列 HTTP 族成员：门链在**请求/会话请求时刻**拿到真实的头部证据（h3 的 `Request` 头表、webtransport 的 `:authority`/`:path` 与头表），其余机制照 quic 的执行点（连接级原子准入、握手截止竞速）在各自栈上落地。两者的集成套件（`requests_round_trip_through_the_handler`、`gates_observe_request_headers`、`gate_rejection_answers_with_status`；`gates_observe_authority_and_path`、`denied_sessions_are_dropped` 等）把这些映射逐条钉死。

## 会话停机总线

会话是路由持有的长驻状态，而停机信号属于 `Server`——路由在装配期拿到闭包时生命周期尚不存在，两者天然隔离。解法：

1. `WsRoute::build()` 返回路由外加一条**会话停机总线**（`StopSignal`，路由内部持克隆）。
2. 路由挂到 `Router`，服务器用 `AxumServer::with_aux_shutdown(bus)` 登记总线。
3. 生命周期停机信号触发时，`AxumServer::start` 的优雅停机 future 先把信号**转发到每条登记的总线**，再让 hyper 开始排空。
4. 每个会话包装器 `select!` 总线与用户会话 future——信号一到，会话 future 被丢弃（drop 即取消），套接字关闭，客户端立刻看到连接终止。

效果：会话与监听器**同步**死于生命周期停机，而不是等着被排水截止兜底、或苟到进程退出。集成测试 `session_echoes_and_dies_with_lifecycle` 固定这条链路（真实客户端看到关闭 + `run` 返回干净结果）。

故意的不对称：总线是**每路由**的，服务器只转发给自己登记的总线。没有全局会话注册表——那需要路由可枚举，axum 的类型擦除不给这个能力，而引入它换不来语义收益。

第二个不对称，同样刻意：**quic 不需要总线**。总线解决的是"路由在装配期拿到闭包、而停机信号属于生命周期"的隔离问题——这只发生在挂载进 `Router` 的路由身上。quic 的会话由**服务器自有的接受循环**直接建立，而接受循环就运行在 `Server::start` 里，拿到的就是停机信号本体：每个会话任务直接与该信号竞速，无需任何转发层。总线是路由挂载形态的补丁，不是会话层的普遍需求——自持接受循环的传输直接接信号、不建总线。webtransport 与 h3 即按此规则落地：两者的接受循环都自持于 `Server::start`，会话任务直接与停机信号竞速（各自的 `lifecycle_stop_tears_down_*` 用例固定）。mqtt 消费桥同属此类（单连接泵直接竞速停机信号），且更进一步：它连会话面都没有。

## 非目标（当前明确不做）

| 不做的事 | 理由 |
|:---|:---|
| 认证后的帧级策略（消息速率、配额） | 需要帧流抽象；等有第一个真实消费者再设计，避免拍脑袋 |
| 跨传输会话抽象（统一 send/recv） | 各栈帧语义差异大；抽象层只会成为最低公共分母。链统一、会话类型原生 |
| 异步门层 | 同步门 + 进程内缓存覆盖当前全部用例；异步层引入装箱 future 的每请求开销 |
| 默认拒绝的空门链 | 空链接受一切——配额与门都是显式装配的，默认开放对齐"积木盒"哲学；文档在每处标注 |

## 威胁模型映射

[threat-model.md](./threat-model.md) 的"未认证通道预算"条目 → 本交付的对应关系：握手并发耗尽 → 会话上限（双检）；无凭证握手 → 门链。握手超时、字节预算、每远端配额三者在裸传输落地时补齐——见上文执行矩阵。
