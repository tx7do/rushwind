# 06 · 认证与鉴权

> 本章目标：掌握 `Authenticator`/`Engine` 两份安全契约、八种认证引擎与三种鉴权引擎的取舍，以及 HTTP 面上的装配规则与两大坑。

前置：[05 · HTTP 服务栈](05-http-edge.md)。

## 认证契约：`Authenticator`

```rust
pub trait Authenticator: Send + Sync {
    fn scheme(&self) -> &'static str;
    fn extract_token(&self, headers: &[(String, String)]) -> Result<String, AuthnError> { /* 默认：Authorization 头按 scheme 取 */ }
    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError>;
    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError>;
    fn authenticate(&self, headers: &[(String, String)]) -> Result<AuthClaims, AuthnError> { /* 取 token 再验 */ }
}
```

`AuthClaims` 是 JSON 包（带 `get_subject`/`get_scopes` 等类型化读取器）。`create_identity` 是"铸造"：JWT 引擎签 token、session 引擎建会话返回会话 ID——引擎既能验也能发。

## 八种认证引擎

| 引擎 | 凭据形态 | 身份来源 | 典型场景 |
|:---|:---|:---|:---|
| `authn-jwt` | `Bearer <jwt>` | token claims | 标准无状态认证，HS/RS/PS/ES/EdDSA 全家 |
| `authn-apikey` | `Bearer <key>` | 静态键表 / 每键 claims / 校验回调 | 服务间调用 |
| `authn-presharedkey` | `Bearer <key>` | 无（空 claims） | 纯成员资格，不区分身份 |
| `authn-basicauth` | `Basic base64(u:p)` | 用户名 | 运维端点、内网工具 |
| `authn-hmac` | `Bearer keyID.timestamp.sig` | keyID | 请求防篡改（配合时间窗） |
| `authn-session` | `X-Session-Id`（可配） | `SessionStore` 里的 claims | 浏览器会话 |
| `authn-noop` | 任意 Bearer | 无 | 开发与测试 |

```rust
// JWT：构造即校验算法与密钥匹配
let options = JwtOptions::new().with_algorithm("HS512")?.with_key(b"hs512-secret");
let auth = JwtAuthenticator::new(options);
let token = auth.create_identity(&claims)?;        // 铸造
let claims = auth.authenticate_token(&token)?;     // 验证

// API Key：校验回调优先级最高
let auth = ApiKeyAuthenticator::new(ApiKeyOptions::new()
    .with_validator(Arc::new(|key| db_lookup(key))));

// HMAC：keyID.timestamp.signature，默认双向 5 分钟时钟窗
let auth = HmacAuthenticator::new(HmacOptions::new()
    .with_secret("key-1", "super-secret").with_max_skew(Duration::from_secs(60)));

// Session：存储可插拔，默认私有内存表
let auth = SessionAuthenticator::new(SessionOptions::new()
    .with_store(Arc::new(MemoryStore::new())));
```

`AuthnError` 分两档：认证失败 → 401（稳定错误码如 `AUTHN_MISSING_BEARER_TOKEN`），配置错误 → 500（`InvalidType`、`MissingKeyFunc`、`UnsupportedSigningMethod`……）。PEM 解析失败在构造时即报 `GetKeyFailed`——坏配置不会悄悄变成一个谁都登不进来的引擎。

## 鉴权契约：`Engine`

```rust
pub trait Engine: Send + Sync {
    fn name(&self) -> String;
    fn is_authorized(&self, subject: Subject, action: Action, resource: Resource, project: Project) -> Result<bool, AuthzError>;
    fn projects_authorized(&self, subjects: Subjects, action: Action, resource: Resource, projects: Projects) -> Result<Projects, AuthzError>;
    fn filter_authorized_pairs(&self, subjects: Subjects, pairs: Pairs) -> Result<Pairs, AuthzError>;
    fn filter_authorized_projects(&self, subjects: Subjects) -> Result<Projects, AuthzError>;
    fn set_policies(&self, policies: PolicyMap, roles: RoleMap) -> Result<(), AuthzError>;   // 引擎内可变，热更策略
}
```

两个真实引擎都是**默认拒绝**：

- **`authz-rbac`**：角色→权限 + 用户→角色（角色→角色边即继承，环安全），通配符只支持精确、`*`、前缀 `doc:*`、后缀 `*:read` 四种。每个检查重算继承闭包，适合小中型角色层级。
- **`authz-acl`**：有序 allow/deny 规则列表，默认 deny-overrides，`with_default_allow()` 翻转无匹配默认值。每检查走全表，适合几十条规则的量级。

`authz-noop` 是陷阱说明书：布尔检查永远 `true`，但批量过滤永远返回**空**——列表端点用它不是"放行"而是"清空"。它是占位符，不是策略。

## HTTP 面：桥接与两大坑

认证与鉴权通过 `rushwind-http` 的桥进入请求路径：

```rust
use rushwind_http::{with_authn, with_authorization, Authenticated, OptionalAuthenticated};

let protected = Router::new()
    .route("/widgets", post(create))
    .route("/widgets/:id", get(show));

// 顺序即铁律：先叠鉴权，再叠认证（后叠的在外圈）
let protected = with_authorization(protected, engine.clone(), "read", "widgets");
let router = public.merge(with_authn(protected, authenticator.clone()));
```

- **坑一：认证层必须是最后包上的层**。层是洋葱，后包的在外圈；鉴权桥要读认证桥放进请求扩展的 claims，所以**先叠鉴权、后叠认证**，bootstrap 装配器内部（`apply_guards`）执行的正是这条顺序。
- **坑二：`authn-noop` 也要 Bearer 头**。noop 引擎"什么凭证都验通过"，但提取仍是契约默认行为——缺 `Authorization: Bearer <任意>` 照样 401。它验证的是"提取-验证"通路，不是"免认证"。

提取器与鉴权桥：

```rust
async fn me(Authenticated(claims): Authenticated) -> Json<Value> { /* 401 拒绝缺席 */ }
async fn maybe(OptionalAuthenticated(claims): OptionalAuthenticated) -> ... { /* 永不拒绝 */ }

// 固定 action/resource 的鉴权层；带 project 轴用 with_authorization_for / with_authorization_claim
let guarded = with_authorization(router, engine.clone(), "read", "widgets");
```

白名单（公开 vs 保护路由）是装配动作——[第 05 章](05-http-edge.md)的子树拆分规则在这里同样适用。

## 踩坑清单

- **basicauth 校验回调模式不能铸造**：密码不在本进程里，`create_identity` 无从编码。需要铸造就用静态用户表。
- **hmac 签名只盖 `keyID.timestamp`**，不含请求体与路径——重放/篡改敏感的接口要叠加 TLS 与端点级正文签名。
- **presharedkey 验过的键是空 claims**：需要"谁在调用"的身份信息请用 apikey 的 per-key claims。
- **session 永不过期**：TTL 是 `SessionStore` 实现的事，`delete` 即登出。
- **rbac/acl 的通配符是有限四式**，中间带 `*`（如 `doc:*:read`）不在支持之列。
- **策略热更时坏载荷会被跳过**：`set_policies` 里反序列化失败的条目静默丢弃——上线新策略前先在影子引擎上过一遍。

## 动手练习

1. 用 `authn-jwt` + `authz-rbac` 搭一个"alice 能读不能写 widgets"的端到端测试，验证 401/403/200 三态。
2. 故意把鉴权层叠在认证层外面，观察鉴权桥因读不到 claims 返回 401（而非预期的鉴权判定），把坑变成肌肉记忆。
3. 用 `authn-session` 实现登录（`create_identity`）→ 携 `X-Session-Id` 访问 → 登出（store.delete）三部曲。

---
[系列目录](README.md) | 上一章：[05 · HTTP 服务栈](05-http-edge.md) | 下一章：[07 · 消息通信](07-broker.md)
