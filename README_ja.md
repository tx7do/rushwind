<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

[English](./README_en.md) | [中文](./README.md) | **日本語**

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.81+-DEA584?logo=rust)](https://www.rust-lang.org/)
[![CI](https://github.com/tx7do/rushwind/actions/workflows/ci.yml/badge.svg)](https://github.com/tx7do/rushwind/actions/workflows/ci.yml)

**Rust マイクロサービスフレームワーク —— コアとプラグイン群を、ひとつのリポジトリに。**

</div>

---

## RushWind とは

多くのエコシステムでは、フレームワークとプラグインはリポジトリが分かれています。フレームワークのリポジトリはライフサイクルと契約だけを担い、トランスポート・ストレージ・メッセージングといった「プラグイン」は別リポジトリに散らばり、バージョンはそれぞれに漂移し、ゲートもバラバラです。RushWind はこの二層をひとつにまとめました：**フレームワークのコアと全プラグインドメインが同一のワークスペースで一緒に進化します**——同じ適合性スイートの基準、そして同じ受け入れ基準「`cargo test` 全緑＝適合」のもとで。

- **フレームワーク半分**：ライフサイクル編成コア + トランスポート契約層 + 9 つのプロトコルスタック・アダプター（axum / WebSocket / QUIC / WebTransport / HTTP/3 / gRPC / MQTT / SSE / cron）。
- **プラグイン半分**：20 の能力ドメイン——ストレージ、ベクトル検索、セキュリティ（認証／認可／パスワードハッシュ）、設定、レジストリ、オブザーバビリティ（メトリクス／トレーシング／ヘルスチェック）、レジリエンス（リトライ／レートリミット／サーキットブレーカー）、キャッシュ、分散ロック、メッセージング、トランザクション、タスクキュー、エンコーディング、スクリプト、AI、オブジェクトストレージ。

ひとつのリポジトリはオールインワンを意味しません——単一リポジトリは開発のビューであって、リンクのビューではありません。使わないエンジンは依存ツリーに入らず、ドメイン同士は契約 crate だけで繋がります。コアにロギングも、レジストリも、設定センターも含まれない姿勢は変わりません：それらはブロックであって、土板ではありません。

## 設計哲学

> **オールインワンではなく、レゴブロックの箱。**

フレームワークのコアが行うのは一つだけ：**信頼性の高いマルチサーバー・ライフサイクル編成**——並行起動、カスケードシャットダウン、フェーズごとの時間予算、結果の観察。契約が先：すべての能力ドメインは契約 crate であり、個々のエンジン・個々のプロトコルスタックはすべて独立したアダプター crate として、利用者が必要に応じて組み合わせます。

すべては Rust の所有権・キャンセル・エラーモデルにネイティブな設計であり、契約の意味論と予算・シャットダウン挙動の規範的詳細は [docs/architecture.md](./docs/architecture.md)（中国語）を参照してください。データアクセス層も同じ哲学の延長です：`rushwind-storage` 契約が一つの Repository 契約で多数のストレージエンジンを駆動し、エンジンごとのアダプター crate を必要に応じて導入します。

## クイックスタート

必要な crate は四つ：`rushwind-core`、`rushwind-transport`、`rushwind-transport-axum`、`rushwind-http`（加えて tokio と axum）。三十行で、統一ミドルウェアスタック付きの最初の HTTP サービスが動きます：

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_core::App;
use rushwind_http::HttpEdge;
use rushwind_transport::StopSignal;
use rushwind_transport_axum::AxumServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // HttpEdge: デフォルトのリクエストミドルウェアスタック（panic 復旧 / リクエスト ID / アクセスログ）
    let router = HttpEdge::new().wrap(
        Router::new().route("/hello", get(|| async { "hello rushwind" })),
    );
    let server = AxumServer::new(SocketAddr::from(([127, 0, 0, 1], 0)), router)?;
    println!("listening on {}", server.endpoint()?);

    // App: 編成者 —— 並行起動、カスケードシャットダウン、フェーズごとの独立予算
    let app = App::builder()
        .name("hello-rushwind")
        .version("0.1.0")
        .erased_server(Arc::new(server))
        .stop_timeout(Duration::from_secs(5))
        .build();

    // Ctrl+C / SIGTERM / app.stop() のすべてがグレースフルシャットダウンに着地する
    app.run(StopSignal::new()).await?;
    Ok(())
}
```

完全な手順（ライフサイクル三原語 `Server` / `StopSignal` / `App` の解説つき）はチュートリアル [01 · ライフサイクルと最初のサービス](./docs/tutorials/01-hello-rushwind.md)（中国語）、全 13 章は [docs/tutorials](./docs/tutorials/README.md) にあります。動かせるサンプルは [examples/](./examples) にあります。プロジェクトが育ったら、組み立ては手書き不要です：`rushwind-bootstrap` が各ドメインのエンジン、HTTP サーバ、cron トランスポートを一枚の YAML から単一ライフサイクルへ組み立てます（チュートリアル [02 · アセンブラと設定](./docs/tutorials/02-bootstrap-and-config.md)（中国語）、完全なサンプル [`examples/bootstrap-demo`](./examples/bootstrap-demo)）。

## 実例とエコシステム

フレームワークの外に、RushWind の開発チェーンを構成する三つの公式リポジトリがあります。モノリスとマイクロサービスという二つの配備形態それぞれに、ゼロから動かせるリファレンス実装があります：

| リポジトリ | 位置づけ |
|:---|:---|
| [rushwind-toolkit](https://github.com/tx7do/rushwind-toolkit) | エコシステム・ツールボックス：`rush` CLI + `rush-gen` コアライブラリ——ドメイン・エンティティのバックエンド全チェーン生成、データベース内省からのエンティティ仕様逆引き、フロント CRUD ページ生成、プロジェクト・スキャフォールド（memory/sqlite/postgres の三変種）、契約マニフェスト検証、差分テストベッド |
| [rushwind-admin](https://github.com/tx7do/rushwind-admin) | モノリス構成のリファレンス実装：契約駆動のエンタープライズ管理バックエンド——単体バックエンド（REST + SSE）に Vue3 Vben ／ Vue3 Element Plus ／ React19 の三フロントが対接、マルチテナント RBAC、監査ログの保全、TOTP MFA、等保 2.0 準拠 |
| [rushwind-cms](https://github.com/tx7do/rushwind-cms) | 最簡 3 サービス・マイクロサービスのリファレンス実装：Rust フルスタック Headless CMS——core-service（ドメイン層、内部向け gRPC）+ admin ／ app の二 BFF（外部向け REST + SSE）、同一 proto 契約が REST と gRPC の両面を産出 |

## リポジトリ構成

最初の二節はフレームワーク（編成 + トランスポート契約 + HTTP フェース）、その後はプラグインドメイン——各ドメイン = 契約 crate 一つ + 必要に応じて導入するエンジンマトリクスで、それぞれ適合性スイートを通過します。

### コアとトランスポート

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-core` | ライフサイクル編成：並行起動、カスケードシャットダウン、フェーズ別の時間予算、結果の観察 |
| `crates/rushwind-transport` | 契約層：`Server` trait、`StopSignal`、`Instance`、`ServerError` |
| `crates/rushwind-transport-axum` | axum アダプター：`Router` をライフサイクルに接続。グレースフルシャットダウンのマッピングはアーキテクチャ文書へ |
| `crates/rushwind-transport-ws` | WS セッションルートビルダー：ゲートチェーン + アドミッションポリシー + セッションシャットダウン bus。契約は [session-middleware.md](./docs/session-middleware.md)（中国語）へ |
| `crates/rushwind-transport-quic` | QUIC アダプター：quinn の accept ループをライフサイクルに接続、フルセッションチェーン（ピアアドレスによるアドミッション、リモート IP ごとのセッション割当）；`stop()` は実際の解放（Endpoint::close） |
| `crates/rushwind-transport-webtransport` | WebTransport アダプター：wtransport エンドポイントをライフサイクルに接続、フルセッションチェーン（セッション要求時の HTTP 族ゲートチェーン、アトミックなアドミッション、ハンドシェイク締切）；`stop()` は実リリース |
| `crates/rushwind-transport-h3` | HTTP/3 アダプター：h3/h3-quinn のリクエストサービスをライフサイクルに接続、リクエスト時点のゲートチェーン（拒否はステータス応答にマッピング）と接続レベルのアトミックなアドミッション；`stop()` は実リリース |
| `crates/rushwind-transport-grpc` | gRPC トランスポート：tonic 0.14 が呼び出し側提供の `Routes` をサービス（proto-build の tonic generator がドメインサービスをコンパイル、配備側がスタブを上書き）、リスナー構築時に即時バインド、停止シグナルでグレースフル・ドレイン、エンドポイント `grpc://host:port` |
| `crates/rushwind-transport-mqtt` | MQTT 消費ブリッジ：外部 broker を購読（購読ごとの handler 登録 + 正規ワイルドカード振り分け）、再接続バックオフ + 購読再構築、handler への直列ポンプ |
| `crates/rushwind-transport-sse` | SSE 通知トランスポート：`/events` のワイヤ契約（CORS プリフライト、三源トークン、401 プレーンテキストエラー行、ユーザ横断ストリームの防止、ユーザごとにフィルタ済みブロードキャストストリーム）+ インプロセス Hub + bootstrap ファクトリ。認可は `Gate` trait で挿し替え可能 |
| `crates/rushwind-transport-cron` | cron トランスポート：分に整列した周期スケジューラを `Server` ライフサイクルに接続、cron 式が実時刻の分と一致するたびに登録済みジョブを発火 |

### HTTP フェース

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-http` | HTTP エッジ：gRPC 整合のエラーエンベロープ `HttpError`——code/reason/message/details、`AuthnError`/`StorageError` の内蔵変換。リクエストミドルウェアスタック（recovery / request-id / logging / CORS / timeout）と `HttpEdge` アセンブラ。`with_authn` / `with_authorization` が認証・認可契約を axum ルートに接続、`Authenticated` エクストラクタ。feature ゲートの `/healthz`+`/readyz` と `/metrics` マウント。設計は [docs/http-edge.md](./docs/http-edge.md)（中国語）へ |
| `crates/rushwind-http-binding` | proto-HTTP ワイヤ形式契約（記述子プールは呼び出し側が提供）：フォームバインダ（ドットパス、二種のフィールド名表記、map/list/oneof 構造、周知リーフのホワイトリスト）、Content-Type codec 解決、認証前 bind 層、protojson 応答 codec、ルートごとのライフサイクル尾部、四フィールド・ステータスエラーエンベロープ |
| `crates/rushwind-gen-http` | 記述子駆動の proto-HTTP ルート面コードジェネレータ：binding ごとのルート表 + フォームバインド計画、(package, reason)→HTTP ステータスのエラー表、service ごとに一つの trait + 空プレースホルダ実装、public/gated マウント発射器 |
| `crates/rushwind-proto-build` | 契約ビルドエンジン（build-dep）：buf コメント閉包（オプションのバイト忠実性）+ protox の型面フィルタ + prost/pbjson（tonic 任意）+ gen-http face 発射（バイト単位のサブ閉包スライス）。配備の build.rs は設定に縮む |
| `crates/rushwind-redact` | 静的レスポンスのマスキング：`(redact.v1)` オプションを fail-closed で計画にコンパイルし、シリアライズ前に動的メッセージをインプレース書き換え（mask/email/固定値/element ネスト/method_skip）、意味論は protoc-gen-go-redact の生成物に固定 |

### ストレージドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-storage` | ストレージ契約：`Repository` trait、三種のページング（Page/Offset/Token）、フィルタツリー、Viewer 五レベル・テナンシ、FieldMask、監査フック |
| `crates/rushwind-storage-memory` | インメモリ参照エンジン：フィルタ／ソート／カーソルの意味論的基準。依存ゼロ |
| `crates/rushwind-storage-seaorm` | SeaORM エンジン：SQLite/PostgreSQL/MySQL 三バックエンド同時有効、三方言の SQL スナップショットで描画を固定、SQLite が適合スイートを通過、live スイートは CI コンテナで走る |
| `crates/rushwind-storage-seaorm-support` | 型付きエンティティサービス橋：`PagingRequest` の select 組み立て（query 構文フィルタ/orderBy/三状態スライス、列型に応じたバインディング）、SeaORM 失敗→エンベロープ、エンティティ時刻↔proto Timestamp |
| `crates/rushwind-storage-seaorm-migration` | SeaORM サービス向けスキーマ移行の機構：エンティティ定義から create-table 文を導出し、追跡付き sea-orm-migration として適用 |
| `crates/rushwind-storage-mongodb` | MongoDB エンジン：FilterExpr→BSON 翻訳をオフライン単体テストで固定、LIKE 族はエスケープ正規表現にコンパイル、live スイートは CI コンテナで走る |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch エンジン：REST + 書き込み時 refresh、`.keyword` による完全一致、bulk によるアトミックなバッチ書き込み |
| `crates/rushwind-storage-opensearch` | OpenSearch エンジン：ES ワイヤ形式の薄い再利用（ワイヤ互換） |
| `crates/rushwind-storage-cassandra` | Cassandra エンジン：bucket 固定パーティション + 契約側エバリュエータによるフィルタ、LWT によるアトミックなバッチ書き込み |
| `crates/rushwind-storage-influxdb` | InfluxDB エンジン：measurement をテーブルとし、id を series タグとし、削除は InfluxQL |
| `crates/rushwind-storage-clickhouse` | ClickHouse エンジン：SQL over HTTP、mutations_sync による read-your-writes、プローブ式の競合検知 |
| `crates/rushwind-storage-cache` | Cache-Aside デコレータ：SingleFlight でスタンピードを合流、キャッシュキーに viewer スコープを含め、generation で陳腐なバックフィルを防止 |
| `crates/rushwind-storage-soft-delete` | ソフトデリート・デコレータ：墓碑書き込み、全読み取り経路でのフィルタ、restore/purge。エンジン非依存 |
| `crates/rushwind-storage-observe` | オブザーバビリティ・デコレータ：呼び出しごとに一つの `tracing` span（table/op/outcome）。OTel エクスポートは subscriber の選択に委ねる |
| `crates/rushwind-storage-tree` | ツリークエリ：children/roots/ancestors/subtree。契約レベルの走査 + 環検出で、任意のエンジンで使える |
| `crates/rushwind-storage-proto` | proto 契約ワイヤ形式：`proto/rushwind/storage/v1/query.proto` から生成（prost + pbjson）、29 演算子マッピング + AIP テキスト解析 |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive マクロ：DTO↔Record マッピングをコンパイル時に生成。スカラー族のワイドニング、`#[record(as_text)]` 列挙、`#[record(with = "…")]` カスタム変換、`#[record(rename)]` 列名 |
| `crates/rushwind-storage-axum` | HTTP エンドポイント層：任意の Repository を CRUD ルートとしてマウント、一覧クエリの二入口（protojson `q` / AIP `filter`）、viewer フックでテナンシを収口 |

### ベクトル検索ドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-vector` | ベクトル検索契約（RAG／意味検索）：統一 `VectorQuery`、score は常に大きいほど類似（余弦 `1−d`／L2 `1/(1+d)`／ドット積の生値）、TopK は非ページング、次元規律、pgvector テキスト codec——純粋な型と純粋な関数 |
| `crates/rushwind-vector-memory` | インメモリ参照エンジン：総当たり top-k + 次元固定／拒否。外部エンジン・アダプターの意味論的基準。criterion ベンチマーク（検索コストの基準線）つき |
| `crates/rushwind-vector-qdrant` | Qdrant エンジン：REST に手書き reqwest・SDK なし——metric 接尾辞つきの名前付きベクトル（`{field}__cosine/euclidean/dot`。サーバ側 top-k がすべての契約 metric で正しく、文書化済みの 3× ストレージ・コスト。既存の素フィールド単 metric コレクションは単 metric モードで動作）、次元規律はサーバ設定から読む、自由文字列 id は決定論的 SHA-1→UUID マッピング + 元 id を payload に添えて返す |
| `crates/rushwind-vector-milvus` | Milvus エンジン：v2 RESTful に手書き reqwest——metric 接尾辞つきの三コレクション（サーバ側 top-k がすべての契約 metric で正しい）、次元規律は describe から読む、listCollections 走査式のクロスフィールド削除 |
| `crates/rushwind-vector-weaviate` | Weaviate エンジン：REST batch + GraphQL nearVector に手書き reqwest——metric 接尾辞つきの三クラス（CamelCase 浄化）、エンジン側の次元レジストリ（Weaviate の schema は次元を報告しない）、dot の面は順序を保つ余弦面だと正直に宣言 |
| `crates/rushwind-vector-neo4j` | Neo4j エンジン：HTTP トランザクション Cypher に手書き reqwest——metric 接尾辞つきの三ラベル + ベクトルインデックス（次元はサーバ側で固定、エラー文面を型付きミスマッチへマッピング）、kNN クエリ、ラベルレジストリによるクロスフィールド削除。cosine 類似度とドット積の生値は直通 |

### セキュリティドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-authn` | 認証契約：`Authenticator` trait（抽出／検証の二半）、`AuthClaims` クレーム束、エラー分類学、`AuthenticationGate` チェーンアドミッション層。[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authn-apikey` | API-key エンジン：静的キー集合／キーごとの claims／検証コールバック |
| `crates/rushwind-authn-basicauth` | Basic-Auth エンジン：RFC 7617 の資格情報を静的ユーザ表または検証コールバックに対して検証 |
| `crates/rushwind-authn-hmac` | HMAC エンジン：keyID.timestamp.signature の署名検証、時計ずれウィンドウつき |
| `crates/rushwind-authn-jwt` | JWT エンジン：HS/RS/PS/ES/EdDSA 族の鋳造と検証 |
| `crates/rushwind-authn-oidc` | OpenID Connect エンジン：discovery + JWKS を構築時に取得して kid ごとにキャッシュ、検証はローカル同期（チェーン参加可能）、refresh は明示／バックグラウンドタスク、iss/aud/exp を厳格に固定、minting は拒否 |
| `crates/rushwind-authn-noop` | Noop エンジン：すべて通し、空の資格情報を鋳造 |
| `crates/rushwind-authn-presharedkey` | 事前共有キー・エンジン：集合メンバーシップ検証、鋳造はランダム抽出 |
| `crates/rushwind-authn-session` | セッションエンジン：不透明セッション ID + 差し替え可能な SessionStore |
| `crates/rushwind-authn-gate` | ゲート・セッション段階：`AccessTokenChecker` 契約（Redis のホワイトリスト／ブラックリスト）と authenticate-then-check の段階ヘッド、trace-id 解析、未検証 bearer のスニッフィング。エンベロープ描画は配備側のエラー表に残す |
| `crates/rushwind-authz` | 認可契約：`Engine` trait（単一裁定 + 三種のバッチフィルタ）、Subject/Action/Resource/Project モデル、ポリシー JSON 相互運用。[docs/security-authn-authz.md](./docs/security-authn-authz.md)（中国語）参照 |
| `crates/rushwind-authz-acl` | ACL エンジン：順序付き allow/deny 規則 + ワイルドカード照合、デフォルト拒否・拒否優先 |
| `crates/rushwind-authz-rbac` | RBAC エンジン：ロール→権限・ユーザ→ロールの二表、伝播継承に環検出つき |
| `crates/rushwind-authz-noop` | Noop エンジン：単一裁定は常に通過、バッチフィルタは常に空 |
| `crates/rushwind-password` | パスワードハッシュ契約：`PasswordHasher` の一つの PHC 文字局面——検証は既存ハッシュに埋め込まれたパラメータが駆動し、`needs_rehash` がアップグレードループのもう半分 |
| `crates/rushwind-password-argon2` | Argon2id エンジン：OWASP 既定コスト（m=19456,t=2,p=1）。レガシー面（argon2i/v16）は検証は通るが要再ハッシュとして読む |
| `crates/rushwind-password-bcrypt` | bcrypt エンジン：MCF 文字列 + 定数時間検証。72 バイト切り詰めはアルゴリズム自体の挙動（文書も釘テストも正直に記す） |
| `crates/rushwind-password-scrypt` | scrypt エンジン：OWASP 既定（ln=17,r=8,p=1）。既存ハッシュに埋め込まれた ln/r/p が検証を駆動 |
| `crates/rushwind-password-pbkdf2` | PBKDF2-HMAC-SHA256 エンジン：OWASP 既定 60 万回。SHA-512 検証は許可、SHA-1 は InvalidHash で拒否 |

### 設定ドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-config` | 設定ソース契約：`Source` trait（load + 任意の watch/watch_value 能力デフォルトメソッド）、`SignalStream`/`ValueStream` ストリーム契約、`FallbackSource` 優先度合成（最初の回答が勝ち + 実効値変更ストリームの統合、タスク境界なし） |
| `crates/rushwind-config-env` | 環境変数エンジン：既定キー + プレフィックス解析。未設定変数は「不在」であってエラーではない |
| `crates/rushwind-config-file` | ファイルエンジン：全ファイル読み取り + 親ディレクトリ監視（エディタのアトミックなリネームに安全）、イベントバースト合流、陳腐値の内容抑制、ストリームは Drop で停止 |
| `crates/rushwind-config-http` | HTTP 設定ソース：URL＝キーの GET + ポーリング ValueWatcher |
| `crates/rushwind-config-etcd` | etcd 設定ソース：キーごとの GET + ネイティブ Watch、signal/push 二モード |
| `crates/rushwind-config-consul` | Consul KV 設定ソース：キーごとの GET + blocking query による push 式 watch |
| `crates/rushwind-config-embedded` | コンパイル時埋め込みソース：`include_bytes!`/`include_str!` が流し込む静的キー→バイトツリー、I/O ゼロ、watch 不可（リンク時に確定） |
| `crates/rushwind-config-apollo` | Apollo 設定ソース：configs REST 読み取り（canonical JSON、404＝不在）+ notifications/v2 ロングポーリング watch。手書き reqwest・SDK なし |
| `crates/rushwind-config-nacos` | Nacos 設定ソース：v1 cs/configs REST 読み取り（原文直渡し、404＝不在、tenant/accessToken 任意）+ ポーリング watch。手書き reqwest・SDK なし |
| `crates/rushwind-config-kubernetes` | Kubernetes ConfigMap 設定ソース：v1 configmaps REST 読み取り（canonical key-sorted JSON、404＝不在、namespace/bearer token 任意）+ ポーリング watch。手書き reqwest・SDK なし |

### レジストリドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-registry` | レジストリ契約：`Registrar` + `Discovery` の二 trait、キー配置／ワイヤ形式は golden テストでバイト単位に固定 |
| `crates/rushwind-registry-etcd` | etcd アダプター：登録 + 発見、リース TTL + 自己修復 keepalive、ハンドル Drop で失効に戻す。live スイート（CI の etcd コンテナ）が相互運用を固定 |
| `crates/rushwind-registry-consul` | Consul アダプター：agent HTTP API 上の登録と発見 |
| `crates/rushwind-registry-eureka` | Eureka アダプター：eureka v2 REST API 上の登録と発見 |
| `crates/rushwind-registry-kubernetes` | Kubernetes アダプター：kube クライアントによる in-cluster pod ラベル登録 + pod watch 発見 |
| `crates/rushwind-registry-nacos` | Nacos アダプター：nacos-sdk naming クライアント上の登録と発見 |
| `crates/rushwind-registry-polaris` | Polaris アダプター：polaris v1 HTTP クライアント API 上の登録と発見 |
| `crates/rushwind-registry-servicecomb` | ServiceComb センターアダプター：v4 登録 API 上の登録・ハートビート・WebSocket watch |
| `crates/rushwind-registry-zookeeper` | ZooKeeper アダプター：ZooKeeper プロトコル上の登録と発見 |

### オブザーバビリティドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-metrics` | メトリクス契約：`Metrics` trait（counter 加算 / histogram 記録 / gauge 設定）、ラベルの正規順序、記録が呼び出し側を失敗させることはない |
| `crates/rushwind-metrics-prometheus` | Prometheus エンジン：名前ごとの遅延登録 + クラス別キャッシュ表、`encode()` がテキスト形式を描画して /metrics ルートに掲げる |
| `crates/rushwind-metrics-otel` | OTel エンジン：OTLP エクスポート（gRPC／HTTP バイナリ protobuf）、計器の遅延生成キャッシュ、gauge は up-down counter で代位 |
| `crates/rushwind-metrics-datadog` | Datadog エンジン：UDP 上の手書き DogStatsD ワイヤプロトコル、ラベル順序、サンプルレート接尾辞、任意のバッチ緩衝 |
| `crates/rushwind-tracer` | トレーシング契約：OTLP tracer-provider 組み立て + W3C trace-context キャリア補助 |
| `crates/rushwind-health` | ヘルスチェック契約：Status/Result/Checker + チェックごとに独立タイムアウトするアグリゲータ + TCP/HTTP チェッカー + axum ハンドラ |

### レジリエンスドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-retry` | 合成可能なリトライ：指数バックオフ + ジッタ、リトライ述語、総タイムアウト |
| `crates/rushwind-ratelimit` | レートリミット契約：アルゴリズム非依存の `Limiter` trait |
| `crates/rushwind-ratelimit-tokenbucket` | トークンバケット・エンジン：レートによる補填 + バースト容量、Allow/Wait/Close、時計注入可（記帳は注入時計で） |
| `crates/rushwind-ratelimit-bbr` | BBR 式適応エンジン：スライディングウィンドウ式スループット推定 + inflight 上限、時計注入可（窓回転は注入時計で） |
| `crates/rushwind-ratelimit-slidingwindow` | スライディングウィンドウ・ログ・エンジン：任意のスライディングウィンドウ内で高々 `limit` 回のアドミッション、境界は正確で固定窓の二重予算隙間なし、ログメモリは `limit` で上限、時計注入可 |
| `crates/rushwind-ratelimit-leakybucket` | リーキーバケット（トラフィックシェーピング）エンジン：バックログは定速で排出、アドミッションは `drain_rate` で絞る、遊休は額を貯めない（トークンバケットのバースト意味論の対極）、時計注入可 |
| `crates/rushwind-circuitbreaker` | サーキットブレーカー契約：`State` + `CircuitBreaker` trait（Allow/MarkSuccess/MarkFailure/Execute/State/Close） |
| `crates/rushwind-circuitbreaker-vegas` | Vegas 式エンジン：遅延膨張の検出 |
| `crates/rushwind-circuitbreaker-sres` | Google SRE 確率式エンジン：硬開・硬閉の代わりに受入率の優雅な減衰 |
| `crates/rushwind-circuitbreaker-hystrix` | Hystrix 式エンジン：エラー率閾値 + スリープウィンドウ + 半開プローブ |

### キャッシュドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-cache` | KV キャッシュ契約：get/set/SetNX/バッチ + TTL |
| `crates/rushwind-cache-local` | インプロセス KV エンジン：TTL 遅延失効 + 容量退避 |
| `crates/rushwind-cache-redis` | Redis エンジン：GET/SET/SETNX/DEL/EXISTS + MGET とパイプラインバッチ |

### ロックドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-lock` | 分散ロック契約：try-once 取得 + 所有権トークン + TTL 強制（forever-lock なし）+ 所有者検証つき release/extend |
| `crates/rushwind-lock-memory` | インプロセス・エンジン：TTL 遅延失効の map + トークン検証——単一プロセスの配備とテスト用。意味論は分散エンジンと同一スイート |
| `crates/rushwind-lock-redis` | Redis エンジン：`SET NX PX` 取得 + トークン比較の Lua によるアトミックな release/extend。live スイートは CI の redis コンテナで |

### メッセージング・トランザクション・タスク

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-broker` | メッセージ契約：`Broker` / `Subscriber` trait、Message/Event 形状、JSON handler 補助、`AckMode::{Auto, Manual}` と `Event::settle_with` 双フック決済、`NackAction::{Requeue, Drop}`、`with_error_handler` 観察点 |
| `crates/rushwind-broker-kafka` | Kafka エンジン：純 Rust プロトコル・クライアント samsa 上の topic ごとプロデューサ + 消費者グループ購読（librdkafka の C ツールチェーン不要）。バッチ・オフセット自動コミットで ack 面なし（文書が正直に宣言） |
| `crates/rushwind-broker-pulsar` | Pulsar エンジン：pulsar-rs の多 topic プロデューサ + Shared コンシューマ、固定サブスクリプション名。Manual 決済は mpsc でポンプへ戻る（ack_with_id/nack_with_id） |
| `crates/rushwind-broker-gcpubsub` | Google Cloud Pub/Sub エンジン：v1 REST を手書き reqwest で——publish/pull/acknowledge。manual 決済は ack deadline ゼロ化（ゼロは即再配信可）、トークンの供給と更新は配備側 |
| `crates/rushwind-broker-sqs` | Amazon SQS エンジン：Query API を手書き reqwest + 手書き SigV4 で（固定参照ベクトルが署名チェーンを固定）——SendMessage/ReceiveMessage/DeleteMessage/ChangeMessageVisibility。manual 決済＝削除または可視性ゼロ |
| `crates/rushwind-broker-rabbitmq` | RabbitMQ エンジン：lapin 上の AMQP 0-9-1 公開／購読、amq.topic 交換機経由。AMQP acker/nacker が決済双フックに直結、nack は `NackAction` に従い requeue |
| `crates/rushwind-broker-nats` | NATS エンジン：async-nats 上の core-NATS 公開／購読。at-most-once で ack 面なし（文書が正直に宣言） |
| `crates/rushwind-broker-redis` | Redis エンジン：topic ごとの専用購読接続上の pub/sub。確認面なし（文書が正直に宣言） |
| `crates/rushwind-broker-mqtt` | MQTT エンジン：rumqttc 上の公開／購読、再接続時に自動再購読。PUBACK を ack フックに掛け、nack フックは「PUBACK を握りつぶす」（MQTT 3.1.1 に NACK フレームはない） |
| `crates/rushwind-broker-stomp` | STOMP エンジン：裸 TCP 上のミニマルな STOMP 1.2 クライアント、RabbitMQ stomp プラグイン向け。`ack:client` 購読で、ACK/NACK フレームがプロトコル本来の二動作 |
| `crates/rushwind-transaction` | 分散トランザクション契約：エンジンの上にある最小クライアント面 |
| `crates/rushwind-transaction-dtm` | DTM エンジン：DTM HTTP プロトコル（reqwest）上の saga / TCC / 二段階メッセージ / XA |
| `crates/rushwind-apalis-postgres` | タスクキューの Postgres ストレージバックエンド（apalis）：SKIP LOCKED 取得、可視性タイムアウト、孤立回復 |

### エンコーディングドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-encoding` | エンコーディング契約：名前登録された codec の後ろにある serde マーシャリング |
| `crates/rushwind-encoding-json` | JSON エンジン：serde_json を名前付き codec レジストリに登録 |
| `crates/rushwind-encoding-msgpack` | MessagePack エンジン：rmp-serde |
| `crates/rushwind-encoding-yaml` | YAML エンジン：serde_yaml |
| `crates/rushwind-encoding-toml` | TOML エンジン：toml |
| `crates/rushwind-encoding-cbor` | CBOR エンジン：ciborium |
| `crates/rushwind-encoding-bson` | BSON エンジン：bson |
| `crates/rushwind-encoding-xml` | XML エンジン：quick-xml |
| `crates/rushwind-encoding-proto` | Protobuf エンジン：prost バイナリ proto を型付き sidecar として |

### スクリプトドメイン

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-script` | スクリプトエンジン契約：能力分割 trait 族（loader / executor / global / function / module / watch の六能力集約、sandbox / runtime-hook / sync / quota の四能力独立）、probe メソッドが能力検出面、`ScriptValue` データ橋、名前キー・ファクトリレジストリ、`EnginePool` / `AutoGrowEnginePool`、`Manager`。ソース契約 `ScriptSource` / `SignalStream` とローカルキャリア・組合せ（memory、file mtime ポーリング、静的ツリー + プレフィックス、二戦略多源集約、TTL 失効監視キャッシュ、変換チェーン） |
| `crates/rushwind-script-wasm` | Wasm エンジン：純インタプリタ wasmi 上のモジュールインスタンス化と `_start` エクスポート呼び出し、スカラー global の読み書き、シグネチャ厳格にマーシャルしたエクスポート関数呼び出し。非同期ホスト関数は誠実に拒否（同期インタプリタに橋はない）、残りの能力は恒久拒否 |
| `crates/rushwind-script-cel` | CEL エンジン：cel-rust による式コンパイルと評価、`ScriptValue` 変数橋、map のプレフィックス付きグローバル変数への平坦化 |
| `crates/rushwind-script-lua` | Lua エンジン：mlua の vendored Lua 5.4、標準ライブラリ・ホワイトリスト・サンドボックス、ホスト関数登録、命令配額フックによる真中断と事後タイムアウト検査、watch リロード |
| `crates/rushwind-script-javascript` | JavaScript エンジン：専用アクタースレッドで走る boa（コマンドチャネル + 一回限りの応答、直列実行）、global／module／スクリプト関数の橋渡し、結果配列の意味論、事後配額検査 |
| `crates/rushwind-script-starlark` | Starlark エンジン：starlark-rust 標準方言のモジュール評価、ホスト環境注入、スクリプト関数呼び出し、JSON シリアライズ値の読み戻し、watch 再キュー |
| `crates/rushwind-script-config` | 設定ソース橋：任意の設定ドメイン `Source` をスクリプト `ScriptSource` に適合——不在は not found へ、エラー分類橋（NotWatchable→能力非対応）、シグナルストリーム直通 |

### AI とオブジェクトストレージ

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-ai` | AI モデル契約：chat 補完 + ストリーミング（組み立て済みの完全なツール呼び出しイベントを含む）+ embeddings の三局面 trait。オブジェクト安全（`Arc<dyn …>` が配備形状） |
| `crates/rushwind-ai-openai` | OpenAI 互換エンジン：reqwest 上の chat／ストリーミング／embeddings（SSE ツール呼び出し delta を index ごとに組み立て）、OpenAI・Qwen・Ollama にそのまま対応 |
| `crates/rushwind-oss` | オブジェクトストレージ契約：S3 互換ストレージ上の put/get/delete/list と presign、加えて multipart アップロードの四面（begin/put/complete/abort、部分番号 1..=10000、連続組み立て。presign は署名権威エンジンのみ、既定 Unsupported） |
| `crates/rushwind-oss-s3` | S3 エンジン：reqwest 上の SigV4 署名 REST、AWS S3 と MinIO をカバー |
| `crates/rushwind-oss-local` | ローカルディスク・エンジン：bucket ごとに一インスタンス、put でディレクトリ実体化、欠損読み取りは NotFound、削除は冪等、キーによるパストラバーサルを拒否。multipart は一時ディレクトリで実装 |

### 組み立てとテスト

| 位置 | 責務 |
|:---|:---|
| `crates/rushwind-bootstrap` | 設定駆動の組み立て：YAML → 各ドメインのエンジン（ストレージ／レジストリ／認証／認可／broker／キャッシュ／サーキットブレーカー／レートリミット／ロック／ベクトル検索／AI／オブジェクトストレージ／メトリクス／設定ソース／スクリプト）+ HTTP サーバ（ルートパッケージ、エッジ・ミドルウェアスタックと CORS、サブツリーごとの認証認可ラップ、feature ゲートのヘルス・メトリクス・マウント）+ cron トランスポート、すべて単一ライフサイクルへ合流 |
| `crates/rushwind-testkit` | アダプター横断の適合性テストスイート（ライフサイクル／ストレージ／パスワード／ベクトル／ロック、feature ゲート）——あらゆるトランスポート・エンジンが全体を通過必須 |

### サンプル

| 位置 | 責務 |
|:---|:---|
| `examples/multi-server` | 二サーバーのライフサイクル・デモ（カスケードシャットダウン、フェーズ順序） |
| `examples/axum-admin` | axum アダプター・デモ：ヘルスルート + シグナル駆動のグレースフルシャットダウン |
| `examples/ws-gateway` | WS ゲートウェイ・デモ：ゲート拒否 + セッション上限 + ライフサイクル連動のセッション・クローズ |
| `examples/quic-gateway` | QUIC ゲートウェイ・デモ：ループバック・ゲート + セッション上限 + ハンドシェイク締切 + エンドポイント連動クローズ |
| `examples/mqtt-ingest` | MQTT 消費デモ：外部 broker に接続し、シグナル駆動のクリーンな終了 |
| `examples/bootstrap-demo` | 組み立てデモ：一枚の YAML + memory エンジン・ファクトリ + ルートパッケージで完全なサービスを起動 |
| `examples/storage-basics` | 同じ Repository コードが memory と SQLite の双エンジンで走り、出力は一行単位で一致 |
| `examples/vector-multi-engine` | ベクトル検索デモ：一つの検索コード経路が memory + qdrant/milvus/weaviate/neo4j の四外部エンジンで走る（それぞれ `VECTOR_*_URL` 環境変数で有効化） |
| `examples/broker-manual-ack` | `AckMode::Manual` のエンドツーエンド・デモ（MQTT）：well-formed メッセージは ack、毒 payload は nack 再キュー、エラー観察点はサブスクリプションを殺さない |
| `examples/oss-multipart` | マルチパート・アップロード・デモ：順不同の部分が部分番号で組み上がり、abort は何も残さず、S3 半分は presigned GET を鋳いて無資格情報で取得 |
| `examples/apalis-postgres-demo` | タスクキュー・デモ：Postgres ストレージ上の配信／スケジューリング／消費の全工程（取得、リトライ・バックオフ、デッドレター、孤立回復） |

## ライフサイクル

```text
┌─ 起動：全 Server が並行稼働 ──────────────────────────┐
│  トリガ集合：OS シグナル / 内部 stop() / 外部シグナル / │
│            いずれかの Server の終了                     │
└────────────────────────────┬─────────────────────────┘
                             ▼
        フェーズ 2：before フック（順次実行、各自独立の予算）
                             ▼
        フェーズ 3：全 Server.stop が並行実行（各自独立の予算、panic 隔離）
                             ▼
        フェーズ 4：after フック（順次実行、各自独立の予算）
                             ▼
        終局：outcome() / subscribe_done() が外部から観察可能
```

各フェーズの予算は**そのフェーズが始まる瞬間**にその場で新たに生成され、より早い文脈から決して継承されません。停止シグナルを無視する Server はドレイン締切で**ドロップ**され（Rust の drop がキャンセルそのもの）、ハングした `stop()` は締切で切断されて `Timeout` を記録します。規範的詳細は [docs/architecture.md](./docs/architecture.md)（中国語）へ。

## 文書とチュートリアル

| 文書 | 内容 |
|:---|:---|
| [docs/architecture.md](./docs/architecture.md) | アーキテクチャと契約意味論：予算、シャットダウン・マッピング、適合スイートの用例目録（中国語） |
| [docs/http-edge.md](./docs/http-edge.md) | HTTP エッジとミドルウェアスタックの設計（中国語） |
| [docs/session-middleware.md](./docs/session-middleware.md) | WS/QUIC セッション・ミドルウェア契約（中国語） |
| [docs/security-authn-authz.md](./docs/security-authn-authz.md) | 認証／認可層の契約と脅威面速記（中国語） |
| [docs/threat-model.md](./docs/threat-model.md) | 脅威モデル（中国語） |
| [docs/tutorials](./docs/tutorials/README.md) | 全 13 章のチュートリアル：ライフサイクル → 組み立てと設定 → ストレージ契約 → クエリ構文 → HTTP スタック → 認証認可 → メッセージング → キャッシュ → レジストリとレジリエンス → オブザーバビリティ → スクリプト → 多プロトコル・トランスポート → テスト（中国語） |

## アダプター作者へ

新しいトランスポート = `Server` を実装 + 適合性スイートを通過、この二つ：

```rust
// crates/rushwind-transport-<your stack>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

新しいストレージ・エンジンも同じく = `Repository` を実装 + ストレージ・スイートを通過：

```rust
// crates/rushwind-storage-<your engine>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

`cargo test` 全緑こそが適合の定義です。契約意味論とスイート用例目録は [docs/architecture.md](./docs/architecture.md)（中国語）、貢献手順は [CONTRIBUTING.md](./CONTRIBUTING.md)（中国語）へ。

## セキュリティ

- 悪意ある `stop()` でもプロセスは止められません：各フェーズにハードな予算がある
- サーバーの panic は記録として隔離され、兄弟サーバーの後片付けが飛ばされることは決してない
- 脆弱性報告の手順は [SECURITY.md](./SECURITY.md)、脅威モデルは [docs/threat-model.md](./docs/threat-model.md)、認証／認可層の契約と脅威面速記は [docs/security-authn-authz.md](./docs/security-authn-authz.md) へ

## ライセンス

[MIT License](./LICENSE)
