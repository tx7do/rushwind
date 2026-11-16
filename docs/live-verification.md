# Live 服务器验证方法论

> 来源：2026-11-16 的七容器验证环节（PG/MySQL/OpenSearch/MongoDB/ClickHouse/Cassandra/InfluxDB/LocalStack-SQS/真 nsqd/MinIO）。该环节在八/九容器上累计抓出 **14 类真协议/方言 bug**——全部是离线套件（mock + SQLite + 参考向量）结构性看不见的。

## 核心教义

**mock 与引擎同错是离线套件的结构性盲区。** mock 的形状必须来自协议文档或真服务器实验——不能来自引擎实现的想象。引擎照错误的想象实现、mock 照同一想象搭台，离线钉子永远绿；只有真服务器是独立的事实源。

**方言/协议面的每一次改动，必须跑一次对应 live 套件。** 以下 14 类 bug 全部由此教义抓出。

## 验证矩阵

| 容器 | 钉数 | 引擎面 | 抓到的 bug |
|:--|:--|:--|:--|
| PostgreSQL 16 | 49 | seaorm 三方言 | JSONB 拒绝 text 绑定；NULL 携带 bigint 类型；IDENTITY 序列不感知显式 id |
| MySQL 8 | 49 | seaorm 三方言 | 双引号 = 字符串字面量；JSON 列 = 字符串参数按序列化文本比较 |
| OpenSearch 2 | 41 | ES 共享面 | 无 flattened 类型 → JSON 列改判存储-only；主键未回填 `_source` |
| MongoDB 7 | 49 | mongo 原生面 | 顶层数组 parse 失败静默空文档；null 匹配的数组元素级怪癖 |
| ClickHouse 24 | 49 | JSON 函数面 | Nullable × JSON 函数的类型合成；URL 形状 = 裸端点 |
| Cassandra 5 | 41 | fetch+eval | harness schema 漂移（引擎零改动） |
| InfluxDB 1.8 | 41 | fetch+eval | DROP 最终一致需轮询；harness schema 漂移 |
| LocalStack 3.4 (SQS) | 2 | SigV4 Query API | 属性解析读错 XML 形状（请求/响应形状混淆） |
| 真 nsqd | 2 | v2 二进制协议 | 响应 body 是裸 `OK` 非发明 `+OK`；心跳帧穿插；负心跳非法 |
| MinIO | 6 | S3 multipart | （复验通过） |

## 抓到的 bug 类别（方法论证据）

### 类别一：类型系统的方言差异
- PostgreSQL 的 JSONB 列拒绝 TEXT 参数（`column is of type jsonb but expression is of type text`）——JSON 种类必须走驱动的类型化 JSON 绑定。
- PostgreSQL 的 NULL 携带类型：bigint NULL 进 jsonb 列是类型错误——NULL 绑定必须按列的声明 kind。
- ClickHouse 的类型合成拒绝 `Nullable` 包装数组——JSON 函数的参数必须 coalesce 出非 Nullable 列。
- PostgreSQL 的 sea-query cust 占位符按**方言 mark** 匹配：PG 的 mark 是 `$n`，`?` 被当字面量渲染、绑定值**静默丢弃**（最阴险——不报错、查询悄悄变错）。

### 类别二：字面拼写与语义的方言差异
- MySQL 默认模式读 `"col"` 为字符串字面量——标识符必须反引号。
- MySQL 的 JSON 列与字符串参数按**序列化文本**比较，而 MySQL 序列化带空格——等值面必须 `CAST(? AS JSON)` 强制 JSON 对 JSON。
- nsqd 的成功响应 body 是裸 `OK`——mock 发明的 `+OK` 让真服务器上一个 publish 都发不出去。
- SQS 的 ReceiveMessage 响应属性是重复无编号的嵌套元素——按请求参数的点号形状解析则属性静默丢失。
- ES 的 Lucene regexp 是**全词匹配**语义——非锚定分支必须包 `.*….*`；全锚定分支剥锚裸渲染（全词匹配天然双端锚）。

### 类别三：服务器状态机的方言差异
- PostgreSQL 的 IDENTITY 序列不感知显式 id 插入——显式 id 后必须 `setval` 推进序列（SQLite rowid 取 max+1、MySQL AUTO_INCREMENT 自动重整，只有 PG 不动）。
- MongoDB 的 null 匹配对数组字段是**元素级**——`path ≠ null` 命中数组行、`{path: null}` 跳过数组行，与「缺失读 NULL」的契约语义相反——面必须拼写精确的两分（`$exists` + `$ne` / `$or`）。
- MongoDB 顶层非对象 JSON 的 parse 失败会静默变空文档——而 `{field: {}}` 匹配**一切嵌入文档**（毒载荷）。
- nsqd 的 ioloop 心跳帧按自己的定时器穿插——会落在命令与 OK 之间，响应读取必须循环跳 `_heartbeat_`。
- InfluxQL 的 DROP MEASUREMENT 最终一致——紧跟着的种子复用上一轮主键（轮询 SHOW MEASUREMENTS 直到消失）。

### 类别四：harness 自身的漂移
- ES/OS 的索引重置 base 硬编码端口 9200——映射到 9201 时删除从未生效，陈旧索引服务重复 id。
- cassandra/influx 的 harness 手写旧 schema——套件加列后 harness 不同步则新列被拒为未知列。
- live harness 必须**幂等**（drop-then-create / 轮询删除），本地重跑才会与 CI 的全新容器等价。

## 插桩形状（照抄即可）

- **harness 幂等**：drop-then-create + 轮询删除（ES 的 poll-404、influx 的 poll-SHOW-MEASUREMENTS、seaorm/cassandra 的 drop_table）。
- **keep-alive**：reqwest 连接池要求 mock/live 服务器按 HTTP/1.1 循环处理单连接的多个请求。
- **每钉独立资源**：并行测试互相抢消息/队列/索引——每测试唯一 topic/queue/index 名。
- **采用制套件**：`rushwind_storage_json_suite!` / `rushwind_storage_regex_suite!` 由带面的引擎采纳；无面的引擎（如 SQLite 无 regexp 函数）如实拒绝并用 crate 内快照钉自己的 SQL 与拒绝。
- **装饰器透明性**：cache/observe/soft-delete/tree 装饰器采纳同一套套件——钉的是「装饰不改变语义」。
- **固定参考向量**：签名链（SigV4/SAS）与协议编码在**离线**冻结（live 前就能跑），live 只验真服务器的接受面。

## 已声明的剩余验证项

- rocketmq 真 broker 的 SEND 写路径：帧编码已按真 broker 修正并验证（PULL 语义化应答），SEND 的 `SERVICE_NOT_AVAILABLE` 需磁盘宽裕宿主（store 磁盘检查在 91% 宿主上无法绕开）——专项待宽裕宿主。
- azuresb：Service Bus 无开源模拟器，等真部署。
- conductor/argo/temporal：等真部署。
- goworkflows：结构性无面（Go SDK 库形态，无服务器 API）。
