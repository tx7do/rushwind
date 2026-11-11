<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

[English](./README_en.md) | [中文](./README.md) | **日本語**

</div>

---

## 設計哲学

> **オールインワンではなく、レゴブロックの箱。**

RushWind が行うのは一つだけ：**信頼性の高いマルチサーバー・ライフサイクル編成**。コアは契約のみを定義します——トランスポート trait、シャットダウンシグナル、インスタンスモデル——そして、個々のプロトコルスタックはすべて独立したアダプター crate として、利用者が必要に応じて組み合わせます。コアにはロギングも、レジストリも、設定センターも含まれません。それらはブロックであって、土板ではありません。

すべては Rust の所有権・キャンセル・エラーモデルにネイティブな設計であり、契約の意味論と予算・シャットダウン挙動の規範的詳細は [docs/architecture.md](./docs/architecture.md)（中国語）を参照してください。データアクセス層も同じ哲学の延長です：`rushwind-storage` 契約が一つの Repository 契約で多数のストレージエンジンを駆動し、エンジンごとのアダプター crate を必要に応じて導入します。

## 現状

能力面はすべて着地しました：**124 crate + 8 サンプル**。ライフサイクル編成をはじめ、トランスポート、HTTP、ストレージ、セキュリティ、設定、レジストリ、可観測性、レジリエンス、キャッシュ、メッセージ、トランザクション、タスク、エンコーディング、スクリプト、AI、オブジェクトストレージの全能力ドメインをカバーします。各ドメイン = 契約 crate 一つ + 必要に応じて導入するエンジンマトリクス。エンジンとアダプターはすべて適合性スイートに合格しなければなりません——`cargo test` が全緑であることが適合の定義です。各ドメインは独自に、Rust エコシステム自身のペースで進化していきます。

ロードマップに残っているのは：kcp（レガシー相互運用時）、`rushwind-protocols` 独立リポジトリ。

## レイアウト

ドメインごとに分けて掲載します。契約 crate はエンジンを前提としません。エンジンとアダプターは必要に応じて導入し、それぞれ適合性スイートに合格します。

### コアとトランスポート

| パス | 役割 |
|:---|:---|
| `crates/rushwind-core` | ライフサイクル編成：並行起動、カスケード停止、フェーズごとの期限、結果の観測 |
| `crates/rushwind-transport` | 契約層：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum アダプター：`Router` をライフサイクルの下で提供；シャットダウン対応はアーキテクチャ文書参照 |
| `crates/rushwind-transport-ws` | WS セッションルートビルダー：ゲートチェーン + アドミッション + セッションシャットダウンバス、契約は [session-middleware.md](./docs/session-middleware.md) 参照 |
| `crates/rushwind-transport-quic` | QUIC アダプター：quinn 受け入れループをライフサイクルに接続、フルセッションチェーン；`stop()` は実釈放（Endpoint::close） |
| `crates/rushwind-transport-webtransport` | WebTransport アダプター：wtransport エンドポイントをライフサイクルに接続、フルセッションチェーン（セッションリクエスト時刻の HTTP ファミリー・ゲートチェーン、原子アドミッション、ハンドシェイク期限）；`stop()` は実釈放 |
| `crates/rushwind-transport-h3` | HTTP/3 アダプター：h3/h3-quinn のリクエスト提供をライフサイクルに接続、リクエスト時刻のゲートチェーン（拒否はステータス応答へ映射）とコネクション単位の原子アドミッション；`stop()` は実釈放 |
| `crates/rushwind-transport-mqtt` | MQTT 消費ブリッジ：外部ブローカーを購読（サブスクリプションごとのハンドラー登録 + 仕様準拠のワイルドカード振り分け）、再接続バックオフ + サブスクリプション再構築、ハンドラーへの直列ポンプ |

### HTTP 面

| パス | 役割 |
|:---|:---|
| `crates/rushwind-http` | HTTP エッジ：gRPC 整列のエラー封筒 `HttpError`——code/reason/message/details、`AuthnError`/`StorageError` の組み込み変換；リクエストミドルウェアスタック recovery / request-id / logging / CORS / timeout と `HttpEdge` アセンブラ；`with_authn` / `with_authorization` で認証・認可契約を axum ルートに接続、`Authenticated` エクストラクター；feature ゲートの `/healthz`+`/readyz` と `/metrics` マウント、設計は [docs/http-edge.md](./docs/http-edge.md) |
| `crates/rushwind-http-binding` | proto-HTTP ワイヤ契約（呼び出し側が記述子プールを提供）：フォームバインダー（ドットパス、二つのフィールド名表記、map/list/oneof 構造、周知リーフの許可リスト）、Content-Type codec 解決、認証前 bind 層、protojson レスポンス codec、ルートごとのライフサイクル尾部、四フィールドのステータスエラー封筒 |
| `crates/rushwind-gen-http` | 記述子駆動の proto-HTTP ルート面コードジェネレーター：binding ごとのルート表 + フォームバインディング計画、(package, reason)→HTTP ステータスのエラー表、アノテーション付き proto サービスごとに一つの trait + 空プレースホルダー実装、public/gated マウントエミッター |

### ストレージドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-storage` | ストレージ契約：`Repository` trait、3 種のページング（Page/Offset/Token）、フィルターツリー、5 段階 Viewer テナンシー、FieldMask、監査フック |
| `crates/rushwind-storage-memory` | インメモリ参照エンジン：フィルター/ソート/カーソルの意味論的基準、依存ゼロ |
| `crates/rushwind-storage-seaorm` | SeaORM エンジン：SQLite/PostgreSQL/MySQL の 3 バックエンド同梱、方言ごとの SQL はスナップショットで固定、SQLite がスイート合格、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-mongodb` | MongoDB エンジン：FilterExpr→BSON 翻訳はオフライン単体テスト済み、LIKE 族はエスケープ正規表現にコンパイル、live スイートは CI コンテナで実行 |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch エンジン：REST + 書き込み時 refresh、`.keyword` 完全一致、bulk 原子性とロールバック |
| `crates/rushwind-storage-opensearch` | OpenSearch エンジン：ES ワイヤ形式の薄い再利用（ワイヤ互換） |
| `crates/rushwind-storage-cassandra` | Cassandra エンジン：bucket 固定パーティション + 契約評価器フィルタ、LWT 原子バッチ |
| `crates/rushwind-storage-influxdb` | InfluxDB エンジン：measurement をテーブルとして、id は series タグ、InfluxQL 削除 |
| `crates/rushwind-storage-clickhouse` | ClickHouse エンジン：HTTP 経由の SQL、mutations_sync で読み取り一貫性、プローブ型競合検出 |
| `crates/rushwind-storage-cache` | Cache-Aside デコレーター：singleflight ミス統合、スコープ込みキャッシュキー、generation 保護付き無効化 |
| `crates/rushwind-storage-soft-delete` | ソフト削除デコレーター：墓碑書き込み、全読み取り経路でフィルタ、restore/purge、エンジン非依存 |
| `crates/rushwind-storage-observe` | 可観測性デコレーター：呼び出しごとに `tracing` スパン（table/op/outcome）、OTel 出力は subscriber の選択 |
| `crates/rushwind-storage-tree` | 木構造クエリ：children/roots/ancestors/subtree を契約レベルの走査で＋循環検出、任意のエンジンで利用可 |
| `crates/rushwind-storage-proto` | proto 契約のワイヤ形式：`proto/rushwind/storage/v1/query.proto` から生成（prost + pbjson）、29 操作子マッピング + AIP テキスト構文 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive マクロ：DTO↔Record マッピングをコンパイル時に生成；スカラー拡幅、`#[record(as_text)]` enum、`#[record(with = "…")]` カスタム変換、`#[record(rename)]` カラム名 |
| `crates/rushwind-storage-axum` | HTTP エッジ層：任意の Repository を CRUD ルートとして公開、一覧クエリは protojson `q` / AIP `filter` の二入口、viewer フックでテナンシーを収口 |

### ベクトル検索ドメイン

| 場所 | 職責 |
|:---|:---|
| `crates/rushwind-vector` | ベクトル検索契約（RAG/意味検索）：統一 `VectorQuery`、スコアは常に大きいほど類似（余弦 `1−d`/L2 `1/(1+d)`/内積その値）、TopK でページングなし、次元規律、pgvector テキストコーデック——型と純関数のみ（go-crud vector 意味の移植） |
| `crates/rushwind-vector-memory` | メモリ参照エンジン：総当たり top-k + 次元固定/拒否。外部エンジンアダプターの意味論ベースライン；外部エンジン族（qdrant/milvus/weaviate 等）は必要時に立ち上げ |

### セキュリティドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-authn` | 認証契約：`Authenticator` trait（抽出/検証の両半分）、`AuthClaims` クレームバッグ、エラー分類学、`AuthenticationGate` ゲート接続層——[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authn-apikey` | API キー・エンジン：静的キー集合 / キーごとのクレーム / 検証コールバック |
| `crates/rushwind-authn-basicauth` | Basic-Auth エンジン：RFC 7617 資格情報を静的ユーザーテーブルまたは検証コールバックに対して |
| `crates/rushwind-authn-hmac` | HMAC エンジン：keyID.timestamp.signature 検証、時計ずれ窓 |
| `crates/rushwind-authn-jwt` | JWT エンジン：HS/RS/PS/ES/EdDSA 族の発行と検証 |
| `crates/rushwind-authn-noop` | noop エンジン：すべて受理、空の資格情報を鋳造 |
| `crates/rushwind-authn-presharedkey` | 事前共有鍵エンジン：集合所属検査、鋳造は無作為抽出 |
| `crates/rushwind-authn-session` | セッション・エンジン：不透明セッション ID と取り替え可能な SessionStore |
| `crates/rushwind-authz` | 認可契約：`Engine` trait（単一評決 + 3 つの一括フィルタ）、Subject/Action/Resource/Project モデル、JSON ポリシー相互運用——[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authz-acl` | ACL エンジン：順序付き allow/deny ルール + ワイルドカード照合、既定拒否・拒否優先 |
| `crates/rushwind-authz-rbac` | RBAC エンジン：役割→権限、ユーザー→役割の双表、循環検出付きの推移的継承 |
| `crates/rushwind-authz-noop` | noop エンジン：単一評決はすべて通過、一括フィルタはすべて空 |

### 設定ドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-config` | 設定ソース契約：`Source` trait（load + 既定の watch/watch_value 能力メソッド）、`SignalStream`/`ValueStream` ストリーム契約、`FallbackSource`——最初の回答が勝つ優先順位合成と、実効値への変更ストリーム統合、タスク境界なし |
| `crates/rushwind-config-env` | 環境変数エンジン：既定キー + 接頭辞解決、未設定変数は「不在」であってエラーではない |
| `crates/rushwind-config-file` | ファイルエンジン：ファイル全体の読み取り + 親ディレクトリ監視（エディタの原子リネームに強い）、イベントバースト統合、内容による陳腐値抑制、ストリームの drop で監視停止 |
| `crates/rushwind-config-http` | HTTP 設定ソース：URL をキーとした GET + ポーリング ValueWatcher |
| `crates/rushwind-config-etcd` | etcd 設定ソース：キーによる GET + ネイティブ Watch、signal/push の両モード |
| `crates/rushwind-config-consul` | Consul KV 設定ソース：キーによる GET + blocking query による push 式 watch |
| `crates/rushwind-config-embedded` | コンパイル時埋め込みソース：include_bytes!/include_str! で供給する静的キー→バイト木——ゼロ I/O、watch 不可（リンク時確定） |
| `crates/rushwind-config-apollo` | Apollo 設定ソース：configs REST 読み取り（canonical JSON、404=不在）+ notifications/v2 ロングポール watch、SDK なしの reqwest 直書き |

### レジストリドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-registry` | レジストリ契約：`Registrar` + `Discovery` の双 trait、キーレイアウト/ワイヤ形式は golden テストでバイト単位に固定 |
| `crates/rushwind-registry-etcd` | etcd アダプター：登録 + 発見、リース TTL + 自己修復 keepalive、ハンドルの drop で期限切れへ退避；live スイート（CI の etcd コンテナ）で相互運用を固定 |
| `crates/rushwind-registry-consul` | Consul アダプター：agent HTTP API による登録と発見 |
| `crates/rushwind-registry-eureka` | Eureka アダプター：eureka v2 REST API による登録と発見 |
| `crates/rushwind-registry-kubernetes` | Kubernetes アダプター：kube クライアントによる in-cluster pod ラベル登録 + pod watch 発見 |
| `crates/rushwind-registry-nacos` | Nacos アダプター：nacos-sdk naming クライアントによる登録と発見 |
| `crates/rushwind-registry-polaris` | Polaris アダプター：polaris v1 HTTP クライアント API による登録と発見 |
| `crates/rushwind-registry-servicecomb` | ServiceComb サービスセンター・アダプター：v4 レジストリ API による登録、ハートビート、WebSocket watch |
| `crates/rushwind-registry-zookeeper` | ZooKeeper アダプター：ZooKeeper プロトコルによる登録と発見 |

### 可観測性ドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-metrics` | メトリクス契約：`Metrics` trait（counter 加算 / histogram 記録 / gauge 設定）、ラベル正規化ソート、記録が呼び出し元を失敗させない |
| `crates/rushwind-metrics-prometheus` | Prometheus エンジン：名前ごとの遅延登録 + 種類ごとのキャッシュ表、`encode()` でテキスト形式を描画し /metrics ルートに |
| `crates/rushwind-metrics-otel` | OTel エンジン：OTLP エクスポート（gRPC / HTTP バイナリ protobuf）、計器の遅延生成キャッシュ、gauge は up-down counter で代用 |
| `crates/rushwind-metrics-datadog` | Datadog エンジン：手書き DogStatsD ラインプロトコル over UDP、タグソート、サンプルレート接尾辞、任意バッチバッファ |
| `crates/rushwind-tracer` | トレーシング契約：OTLP tracer-provider のセットアップ + W3C trace-context キャリアヘルパー |
| `crates/rushwind-health` | ヘルスチェック契約：Status/Result/Checker、チェックごとのタイムアウト付きアグリゲーター、TCP/HTTP チェッカー、axum ハンドラー |

### レジリエンスドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-retry` | 組み合わせ可能なリトライ：指数バックオフ + ジッター、リトライ述語、総タイムアウト |
| `crates/rushwind-ratelimit` | レート制限契約：アルゴリズム非依存の `Limiter` trait |
| `crates/rushwind-ratelimit-tokenbucket` | トークンバケット・エンジン：レートでの補充 + バースト容量、Allow/Wait/Close、計時計注入可能 |
| `crates/rushwind-ratelimit-bbr` | BBR 風の適応型エンジン：スライディングウィンドウのスループット推定 + inflight 上限 |
| `crates/rushwind-circuitbreaker` | サーキットブレーカー契約：`State` + `CircuitBreaker` trait（Allow/MarkSuccess/MarkFailure/Execute/State/Close） |
| `crates/rushwind-circuitbreaker-vegas` | Vegas 方式エンジン：レイテンシ膨張の探査 |
| `crates/rushwind-circuitbreaker-sres` | Google SRE 確率型エンジン：ハードな開/閉ではなく受入率の優雅な減衰 |
| `crates/rushwind-circuitbreaker-hystrix` | Hystrix 方式エンジン：エラー率しきい値 + スリープウィンドウ + ハーフオープン試行 |

### キャッシュドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-cache` | KV キャッシュ契約：get/set/SetNX/バッチ + TTL |
| `crates/rushwind-cache-local` | プロセス内 KV エンジン：TTL 付きエントリの遅延失効 + 容量退避 |
| `crates/rushwind-cache-redis` | Redis エンジン：GET/SET/SETNX/DEL/EXISTS + MGET とパイプライン一括設定 |

### ロックドメイン

| 場所 | 職責 |
|:---|:---|
| `crates/rushwind-lock` | 分散ロック契約：try-once 取得 + 所有権トークン + TTL 強制（forever-lock なし）+ 所有権検証付き release/extend |
| `crates/rushwind-lock-memory` | プロセス内エンジン：TTL 怠惰期限切れの map + トークン検証——単プロセス配置とテスト用、分散エンジンと同じ適合性スイート |
| `crates/rushwind-lock-redis` | Redis エンジン：`SET NX PX` 取得 + トークン照合の Lua 原子 release/extend、live スイートは CI の redis コンテナで走る |

### メッセージ・トランザクション・タスク

| パス | 役割 |
|:---|:---|
| `crates/rushwind-broker` | メッセージブローカー契約：`Broker` / `Subscriber` trait、Message/Event 形状、JSON ハンドラーヘルパー |
| `crates/rushwind-broker-kafka` | Kafka エンジン：samsa（純 Rust の Kafka プロトコルクライアント）によるトピックごとプロデューサー + コンシューマーグループ購読（librdkafka の C ツールチェーン不要） |
| `crates/rushwind-broker-pulsar` | Pulsar エンジン：pulsar-rs のマルチトピック・プロデューサー + Shared コンシューマー、固定サブスクリプション名 |
| `crates/rushwind-broker-rabbitmq` | RabbitMQ エンジン：lapin による AMQP 0-9-1 pub/sub、amq.topic エクスチェンジ経由 |
| `crates/rushwind-broker-nats` | NATS エンジン：async-nats による core-NATS pub/sub |
| `crates/rushwind-broker-redis` | Redis エンジン：トピックごとの専用購読コネクションによる pub/sub |
| `crates/rushwind-broker-mqtt` | MQTT エンジン：rumqttc による pub/sub、再接続時の自動再購読 |
| `crates/rushwind-broker-stomp` | STOMP エンジン：素の TCP 上の最小 STOMP 1.2 クライアント、RabbitMQ の stomp プラグインと対話 |
| `crates/rushwind-transaction` | 分散トランザクション契約：エンジンの上の最小クライアント面 |
| `crates/rushwind-transaction-dtm` | DTM エンジン：DTM の HTTP プロトコル（reqwest）による saga / TCC / 二相メッセージ / XA |
| `crates/rushwind-apalis-postgres` | apalis タスクキューの Postgres ストレージバックエンド：SKIP LOCKED クレーム、可視性タイムアウト、孤児復旧 |

### エンコーディングドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-encoding` | codec 契約：命名・登録された codec の背後にある serde マーシャリング |
| `crates/rushwind-encoding-json` | JSON エンジン：serde_json を命名 codec レジストリに登録 |
| `crates/rushwind-encoding-msgpack` | MessagePack エンジン：rmp-serde |
| `crates/rushwind-encoding-yaml` | YAML エンジン：serde_yaml |
| `crates/rushwind-encoding-toml` | TOML エンジン：toml |
| `crates/rushwind-encoding-cbor` | CBOR エンジン：ciborium |
| `crates/rushwind-encoding-bson` | BSON エンジン：bson |
| `crates/rushwind-encoding-xml` | XML エンジン：quick-xml |
| `crates/rushwind-encoding-proto` | Protobuf エンジン：prost によるバイナリ proto、型付き sidecar として |

### スクリプトドメイン

| パス | 役割 |
|:---|:---|
| `crates/rushwind-script` | スクリプトエンジン契約：能力分割 trait 族（loader / executor / global / function / module / watch の六能力を集約、sandbox / runtime-hook / sync / quota の四能力は独立）、probe メソッドが能力探出面、`ScriptValue` データブリッジ、名前キーのファクトリーレジストリ、`EnginePool` / `AutoGrowEnginePool`、`Manager`；ソース契約 `ScriptSource` / `SignalStream` とそのローカル担体・合成（メモリ、mtime ポーリングのファイル、静的ツリー + 接頭辞結合、二戦略マルチソース、TTL 失効監視キャッシュ、変換チェーン） |
| `crates/rushwind-script-wasm` | Wasm エンジン：wasmi 純インタープリター上のモジュール実体化と `_start` エクスポート呼び出し、空のインポート面、その他の能力は一律拒否 |
| `crates/rushwind-script-cel` | CEL エンジン：cel-rust による式のコンパイルと評価、`ScriptValue` 変数ブリッジ、マップの接頭辞付きグローバルへの平坦化 |
| `crates/rushwind-script-lua` | Lua エンジン：mlua の vendored Lua 5.4、標準ライブラリ許可リストのサンドボックス、ホスト関数登録、命令クォータフックによる真の中断と事後タイムアウト検査、watch の再読み込み |
| `crates/rushwind-script-javascript` | JavaScript エンジン：専用 actor スレッド上で稼働する boa（コマンドチャネル + 一回限りの応答、直列実行）、グローバル・モジュール・スクリプト関数のブリッジ、結果配列セマンティクス、事後クォータ検査 |
| `crates/rushwind-script-starlark` | Starlark エンジン：starlark-rust 標準方言のモジュール評価、ホスト環境注入、スクリプト関数呼び出し、JSON シリアライザー経由の値読み戻し、watch の再キューイング |
| `crates/rushwind-script-config` | 設定ソースブリッジ：任意の設定ドメイン `Source` をスクリプト `ScriptSource` へ適合——不在を未検出へマップ、エラー分類ブリッジ（NotWatchable → 能力未対応）、シグナルストリームの素通し |

### AI とオブジェクトストレージ

| パス | 役割 |
|:---|:---|
| `crates/rushwind-ai` | AI モデル契約：OpenAI 互換エンドポイント上の chat 補完 |
| `crates/rushwind-ai-openai` | OpenAI 互換エンジン：reqwest による chat / ストリーミング / embeddings——OpenAI、Qwen、Ollama に対応 |
| `crates/rushwind-oss` | オブジェクトストレージ契約：S3 互換ストレージ上の put/get/delete |
| `crates/rushwind-oss-s3` | S3 エンジン：reqwest による SigV4 署名 REST、AWS S3 と MinIO をカバー |

### 組み立てとテスト

| パス | 役割 |
|:---|:---|
| `crates/rushwind-bootstrap` | 設定駆動の組み立て：YAML → ストレージエンジン + HTTP サーバー + ルートパックを、単一ライフサイクルに統合 |
| `crates/rushwind-testkit` | クロスアダプター適合性スイート（ライフサイクル/ストレージ/パスワード/ベクトル/ロック、フィーチャー門控）——すべてのトランスポート/エンジンが全項目に合格する必要がある |

### サンプル

| パス | 役割 |
|:---|:---|
| `examples/multi-server` | 2 サーバーのライフサイクル・デモ（カスケード、フェーズ順序） |
| `examples/axum-admin` | axum アダプター・デモ：ヘルスルート + シグナル駆動のグレースフルシャットダウン |
| `examples/ws-gateway` | WS ゲートウェイ・デモ：ゲート拒否 + セッション上限 + ライフサイクル連動のセッション切断 |
| `examples/quic-gateway` | QUIC ゲートウェイ・デモ：ループバックゲート + セッション上限 + ハンドシェイク期限 + エンドポイント級の切断 |
| `examples/mqtt-ingest` | MQTT 消費デモ：外部ブローカーに接続し、シグナル駆動のクリーンな終了 |
| `examples/bootstrap-demo` | 組み立てデモ：一つの YAML + メモリエンジンファクトリー + ルートパックでフルサービスを起動 |
| `examples/storage-basics` | 同じ Repository コードをインメモリと SQLite の 2 エンジンで実行、出力は行単位で一致 |
| `examples/apalis-postgres-demo` | タスクキューデモ：Postgres ストレージ上の投函/スケジュール/消費の全域（クレーム、リトライバックオフ、デッドレター、孤児復旧） |

## ライフサイクル

```text
┌─ 起動：全サーバーが並行実行 ──────────────────────────┐
│  トリガー集合：OS シグナル / 内部 stop() /               │
│  外部シグナル / いずれかのサーバー終了                    │
└────────────────────────┬──────────────────────────┘
                         ▼
   フェーズ 2：before フック（順次実行、それぞれ独立した予算）
                         ▼
   フェーズ 3：全サーバーの stop を並行実行（独立予算、panic 隔離）
                         ▼
   フェーズ 4：after フック（順次実行、それぞれ独立した予算）
                         ▼
   終局：outcome() / subscribe_done() が外部から観測可能
```

各フェーズの予算は**そのフェーズが始まる瞬間**にその場で生成され、より早いコンテキストから継承されることは決してありません。シャットダウンシグナルを無視するサーバーは排水期限で**ドロップ**され（Rust の drop はキャンセル）、停止した `stop()` は期限で切断され `Timeout` として記録されます。規範的詳細は [docs/architecture.md](./docs/architecture.md)（中国語）を参照。

## アダプター作者へ

新しいトランスポート = `Server` を実装 + 適合性スイートに合格、の 2 ステップ：

```rust
// crates/rushwind-transport-<あなたのスタック>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

ストレージエンジンも同様 = `Repository` を実装 + ストレージスイートに合格：

```rust
// crates/rushwind-storage-<あなたのエンジン>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

`cargo test` が全緑であることが適合の定義です。CI がすべてのアダプター crate に対して強制します。契約の意味論とスイートのケース一覧は [docs/architecture.md](./docs/architecture.md) を参照。

## 開発ゲート

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI は Linux/Windows/macOS のマトリクス上で同一のゲートを実行します。ワークスペース全体が `#![forbid(unsafe_code)]` です。

## セキュリティ

- 悪意ある `stop()` はプロセスを停留させられない：各フェーズにハード予算がある
- パニックしたサーバーは隔離・記録され、兄弟サーバーの後片付けを決して省略しない
- 脆弱性報告は [SECURITY.md](./SECURITY.md)、脅威モデルは [docs/threat-model.md](./docs/threat-model.md)、認証・認可層の契約と脅威面速記は [docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）を参照

## ライセンス

[MIT License](./LICENSE)
