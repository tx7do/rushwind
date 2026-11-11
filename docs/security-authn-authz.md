# 认证与鉴权：契约与引擎矩阵

本文是认证/鉴权层的权威设计文档：两个契约 crate 的模型、引擎矩阵、设计决策清单、以及执行点。与 [session-middleware.md](./session-middleware.md) 的关系：那份管会话层的门链与准入策略；本文管门链**背后**的身份判定与策略求值。

## 模型

积木盒哲学在这里的形态：契约 crate 只定义接口，每一种凭证方案/策略模型是独立的引擎 crate，由使用者按需拼装。

**`rushwind-authn`（认证契约）**。`Authenticator` trait 把认证切成两半：提取（`extract_token`——从请求头里取出原始凭证，默认实现是 `Authorization: <scheme> <token>` 形状）与验证（`authenticate_token`——凭证换 claims；`create_identity`——claims 换凭证）。`authenticate` 组合两半，并把一切提取失败折叠为 `MissingBearerToken`——所有引擎共享这一统一形态。`AuthClaims` 是声明包的 JSON 对象载体，类型化 getter 的语义固定：缺键零值、数值跨类型截断转换、`NumericDate` 拍平为 Unix 秒。错误分类学 `AuthnError` 有 21 个条目（状态码 + 稳定 reason 字符串），供执行点映射拒绝响应。

**`rushwind-authz`（鉴权契约）**。`Engine` trait 覆盖单裁决（`is_authorized`）与三个批量过滤（projects / pairs / projects-at-all），模型类型只有 Subject / Action / Resource / Project 四根轴。策略安装走 `PolicyMap`/`RoleMap`：JSON 载荷承载引擎本地结构，载荷字段名即线格式；载荷不能反序列化进引擎的规则类型时**静默跳过**——一条坏策略不拖垮整个引擎。

## 引擎矩阵

| crate | 方案/模型 |
|:---|:---|
| `rushwind-authn-apikey` | 不透明 bearer key：静态集合 / 每键 claims / 验证回调 |
| `rushwind-authn-basicauth` | RFC 7617：静态用户表 / 验证回调 |
| `rushwind-authn-hmac` | `keyID.timestamp.signature` HMAC-SHA256 请求签名，时钟偏移窗口（默认 5 分钟，双向） |
| `rushwind-authn-jwt` | 签名 JWT（HS/RS/PS/ES/EdDSA 族），验证侧采用严格默认（exp/nbf 出现即校验、零宽限、exp 非必需、算法钉死） |
| `rushwind-authn-noop` | 全放行、铸造空凭证 |
| `rushwind-authn-presharedkey` | 静态 key 集合成员校验，铸造为均匀随机抽取 |
| `rushwind-authn-session` | 不透明会话 ID + 可插拔 `SessionStore`（进程内默认实现） |
| `rushwind-authz-acl` | 有序 allow/deny 规则 + 通配模式匹配，默认拒绝、拒绝优先 |
| `rushwind-authz-rbac` | 角色→权限、用户→角色双表 + 传递角色继承（带环检测） |
| `rushwind-authz-noop` | 单裁决全通过、批量过滤全空 |

每个 crate 的模块文档记录自己的语义与差异；本表只是索引。

## 执行点

**会话传输（ws/quic）**：`rushwind-authn` 提供 `AuthenticationGate`——把任意 `Authenticator` 接入 [`GateChain`](../crates/rushwind-transport/src/session.rs) 的适配层。门链是 [session-middleware.md](./session-middleware.md) 认定的会话传输唯一 authn/authz 挂载点；认证失败映射为 Rejection{status: 错误分类学状态码, reason: 错误 code}。门按契约**同步纯 CPU**：只有本地证据引擎（静态集合、签名验证、进程内存储）可以进门；带 IO 的验证回调必须前置进程内缓存，或不进门链。

**HTTP 族（axum）**：tower 中间件看得到每个请求，路由级 enforcement 是应用自己的装配；契约 crate 只供给原语。`rushwind-bootstrap` 目前不装配认证，接入属后续工作。

## 设计决策清单

| 主题 | 决策 |
|:---|:---|
| 请求上下文 | `authenticate(&[(String,String)])` 读头对切片；头名与 scheme 大小写不敏感比较——gRPC metadata 归一化为小写、HTTP 头原样呈现，两者都收 |
| 出站注入 | `format_authorization` 只格式化值，放置是调用方的事 |
| 凭证铸造 | `create_identity` 只返回凭证字符串，不携带写回上下文的副作用 |
| 引擎注册 | 直接构造器，registry/storage 模式 |
| 资源释放 | `Drop`，不设显式 `close()` 方法 |
| 声明包数值转换 | 整数跨宽度用 `as` 截断，float→int 饱和（Rust 1.45+ 语义） |
| basicauth 铸造错误 | 全部并入分类学：缺 subject → `InvalidSubject`，无静态口令 → `MissingKeyFunc` |
| hmac 铸造错误 | 同样并入分类学，无裸字符串错误 |
| session ID 熵 | OS CSPRNG 16 字节，hex 编码为 32 字符 ID |
| session 引擎缺省存储 | 每引擎实例私有新实例，无全局共享单例 |
| session 凭证载体 | 头是唯一载体（可改名），提取走 `extract_token` 覆写 |
| 算法表 | 不含 ES512——`jsonwebtoken` 无此支持，构造期拒绝为 `UnsupportedSigningMethod` |
| 未知算法名 / PEM 解析失败 | 构造期拒绝：`UnsupportedSigningMethod` / `GetKeyFailed`，绝不静默留下未配置的引擎 |
| 策略载荷 | JSON 载荷 + serde 反序列化，字段名即线格式；坏载荷静默跳过 |
| jwt 库错误分类 | `jsonwebtoken` 错误映射进分类学：`ExpiredSignature`→`TokenExpired`、`InvalidSignature`→`SignTokenFailed`、`InvalidAlgorithm`→`UnsupportedSigningMethod`、其余→`InvalidToken`。nbf 违例的变体身份依库而定，拒绝本身一致 |

## 未覆盖（与原因）

| 能力 | 未覆盖原因 |
|:---|:---|
| mTLS 认证 | 需要传输层暴露对端证书；rushwind 传输契约尚无此面 |
| OAuth2 / OIDC 引擎 | 完整令牌交换流程 + IdP 发现 + 异步 IO；需要先定 async 契约面 |
| Casbin / Cedar 引擎 | 本地重库（casbin-rs / cedar-policy 存在）；接入需先定模型文件与契约的映射 |
| Cerbos / OPA / Zanzibar 系（keto、openfga）客户端 | 远程策略服务客户端；需要 reqwest + 异步契约面 |
| AWS IAM（SigV4）校验引擎 | AWS SigV4 校验；需先定凭证-请求绑定的契约形状 |
| ~~密码哈希工具族~~ | **已实现**（2026-11-11）：`rushwind-password` 契约 + argon2/bcrypt/scrypt/pbkdf2 四引擎，与 authn/authz 契约正交、共享 testkit 一致性套件 |

## 威胁面速记

- `AuthenticationGate` 的 reason 字段只放错误 code，消息留在服务端——稳定标识符，无信息泄漏面。
- apikey/hmac 引擎的验证回调、session 引擎的自定义存储都是进程外信任源；把它们接进门链前先想清楚门契约（同步纯 CPU）。
- noop 引擎（两侧）是占位符不是策略：authn-noop 的空 claims 在下游鉴权眼里是匿名身份，authz-noop 的批量过滤返回空列表——列表端点会渲染为空，而不是放行一切。
- jwt 引擎的验证配置刻意采用严格默认（含"exp 非必需"）；接受无 exp 令牌的路由方需要自己补时效约束。

## 测试

各引擎 crate 内联覆盖了各自协议的语义测试用例（含黄金向量式的错误身份断言）；没有跨引擎一致性套件——引擎语义本来就不同（noop 的全放行就是它的全部意义），套件只会弱化为"调用了不 panic"。这与 storage 的套件决策（[architecture.md](./architecture.md) 的一致性套件清单）相反，原因即在于此。
