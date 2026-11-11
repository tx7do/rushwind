//! Wire conformance for the OpenAI-compatible engine, against a
//! local axum mock endpoint: endpoint resolution and bearer/org
//! headers, the chat/tool-call round trip, SSE streaming, the
//! embeddings parse, and the error mapping (status / decode).

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rushwind_ai::{
    AiError, ChatModel, ChatRequest, CloudConfig, Config, LocalConfig, Message, ModelType, Tool,
};
use rushwind_ai_openai::{ChatStreamEvent, OpenAiClient};

/// What one request looked like on the wire.
#[derive(Debug)]
struct Recorded {
    method: String,
    path: String,
    authorization: Option<String>,
    organization: Option<String>,
    content_type: Option<String>,
    body: serde_json::Value,
}

/// The canned reply the mock endpoint answers every request with.
#[derive(Clone)]
enum Reply {
    Json(serde_json::Value),
    Status(StatusCode, String),
    Sse(String),
}

#[derive(Clone)]
struct AppState {
    recorded: Arc<Mutex<Option<Recorded>>>,
    reply: Reply,
}

async fn handler(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    *state.recorded.lock().unwrap() = Some(Recorded {
        method: method.to_string(),
        path: uri.path().to_string(),
        authorization: headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        organization: headers
            .get("OpenAI-Organization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        content_type,
        body: serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    });
    match &state.reply {
        Reply::Json(value) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/json")],
            value.to_string(),
        )
            .into_response(),
        Reply::Status(code, text) => (*code, text.clone()).into_response(),
        Reply::Sse(text) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/event-stream")],
            text.clone(),
        )
            .into_response(),
    }
}

/// Spawns the mock endpoint on an ephemeral port; returns the base
/// URL and the single-request recorder.
async fn spawn(reply: Reply) -> (String, Arc<Mutex<Option<Recorded>>>) {
    let recorded = Arc::new(Mutex::new(None));
    let state = AppState {
        recorded: recorded.clone(),
        reply,
    };
    let app = axum::Router::new().fallback(handler).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), recorded)
}

fn cloud_client(base_url: &str, model: &str) -> OpenAiClient {
    OpenAiClient::new(Config {
        model_type: ModelType::Cloud,
        model_name: model.to_string(),
        timeout_seconds: 0,
        cloud: Some(CloudConfig {
            api_key: "sk-test".to_string(),
            base_url: base_url.to_string(),
            organization: String::new(),
        }),
        local: None,
    })
    .unwrap()
}

fn chat_request(content: &str) -> ChatRequest {
    ChatRequest {
        messages: vec![Message::user(content)],
        ..ChatRequest::default()
    }
}

const CHAT_REPLY: &str = r#"{
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "Hello there"},
        "finish_reason": "stop"
    }],
    "usage": {"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11}
}"#;

#[tokio::test]
async fn cloud_chat_round_trip_carries_auth_and_config_model() {
    let (base, recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let client = cloud_client(&base, "gpt-4o");

    let response = client.chat(chat_request("hello")).await.unwrap();

    assert_eq!(response.message.role, rushwind_ai::Role::Assistant);
    assert_eq!(response.message.content.as_deref(), Some("Hello there"));
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
    let usage = response.usage.unwrap();
    assert_eq!(
        (
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens
        ),
        (9, 2, 11)
    );

    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.method, "POST");
    assert_eq!(recorded.path, "/chat/completions");
    assert_eq!(recorded.authorization.as_deref(), Some("Bearer sk-test"));
    assert_eq!(recorded.organization, None);
    assert_eq!(recorded.content_type.as_deref(), Some("application/json"));
    assert_eq!(recorded.body["model"], "gpt-4o");
    assert_eq!(recorded.body["stream"], false);
    assert_eq!(recorded.body["messages"][0]["role"], "user");
    assert_eq!(recorded.body["messages"][0]["content"], "hello");
}

#[tokio::test]
async fn organization_header_travels_when_set() {
    let (base, recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let client = OpenAiClient::new(Config {
        model_type: ModelType::Cloud,
        model_name: "gpt-4o".to_string(),
        timeout_seconds: 0,
        cloud: Some(CloudConfig {
            api_key: "sk-test".to_string(),
            base_url: base.clone(),
            organization: "org-abc".to_string(),
        }),
        local: None,
    })
    .unwrap();
    client.chat(chat_request("hello")).await.unwrap();
    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.organization.as_deref(), Some("org-abc"));
}

#[tokio::test]
async fn request_model_overrides_the_config_model() {
    let (base, recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let client = cloud_client(&base, "gpt-4o");
    let request = ChatRequest {
        model: Some("qwen-max".to_string()),
        ..chat_request("hello")
    };
    client.chat(request).await.unwrap();
    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.body["model"], "qwen-max");
}

#[tokio::test]
async fn local_mode_hits_v1_with_none_bearer() {
    let (base, recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let address = base.trim_start_matches("http://").to_string();
    let (host, port) = address.split_once(':').unwrap();
    let client = OpenAiClient::new(Config {
        model_type: ModelType::Local,
        model_name: "llama3".to_string(),
        timeout_seconds: 0,
        cloud: None,
        local: Some(LocalConfig {
            host: host.to_string(),
            port: port.parse().unwrap(),
        }),
    })
    .unwrap();

    let response = client.chat(chat_request("hello")).await.unwrap();
    assert_eq!(response.message.content.as_deref(), Some("Hello there"));

    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.path, "/v1/chat/completions");
    assert_eq!(recorded.authorization.as_deref(), Some("Bearer none"));
    assert_eq!(recorded.body["model"], "llama3");
}

#[tokio::test]
async fn tool_definition_and_tool_call_round_trip() {
    let canned = serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": "{\"city\":\"Hangzhou\"}"}
                }]
            },
            "finish_reason": "tool_calls"
        }]
    });
    let (base, recorded) = spawn(Reply::Json(canned)).await;
    let client = cloud_client(&base, "gpt-4o");

    let request = ChatRequest {
        messages: vec![Message::user("weather in Hangzhou?")],
        tools: vec![Tool::function(
            "get_weather",
            "Look up the weather of a city",
            serde_json::json!({
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"]
            }),
        )],
        ..ChatRequest::default()
    };
    let response = client.chat(request).await.unwrap();

    let tool_calls = &response.message.tool_calls;
    assert_eq!(tool_calls.len(), 1);
    assert_eq!(tool_calls[0].id, "call_1");
    assert_eq!(tool_calls[0].function.name, "get_weather");
    assert_eq!(tool_calls[0].function.arguments, "{\"city\":\"Hangzhou\"}");
    assert_eq!(response.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(response.message.content, None);

    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.body["tools"][0]["type"], "function");
    assert_eq!(recorded.body["tools"][0]["function"]["name"], "get_weather");
    assert_eq!(
        recorded.body["tools"][0]["function"]["parameters"]["required"][0],
        "city"
    );
}

#[tokio::test]
async fn server_error_maps_to_typed_status() {
    let (base, _recorded) = spawn(Reply::Status(
        StatusCode::INTERNAL_SERVER_ERROR,
        "boom".into(),
    ))
    .await;
    let client = cloud_client(&base, "gpt-4o");

    let error = client.chat(chat_request("hello")).await.unwrap_err();
    match error {
        AiError::Status { code, body } => {
            assert_eq!(code, 500);
            assert_eq!(body, "boom");
        }
        other => panic!("expected status error, got: {other}"),
    }

    // The streaming path checks the status before switching to SSE.
    let error = match client.chat_stream(chat_request("hello")).await {
        Err(error) => error,
        Ok(_) => panic!("expected the stream request to fail with a status error"),
    };
    assert!(matches!(error, AiError::Status { code: 500, .. }));
}

#[tokio::test]
async fn undecodable_bodies_map_to_typed_decode_errors() {
    // A 200 whose body is not a completion object.
    let (base, _) = spawn(Reply::Json(serde_json::json!("not an object"))).await;
    let client = cloud_client(&base, "gpt-4o");
    let error = client.chat(chat_request("hello")).await.unwrap_err();
    assert!(matches!(error, AiError::Decode(_)));

    // A 200 completion object without choices.
    let (base, _) = spawn(Reply::Json(serde_json::json!({"choices": []}))).await;
    let client = cloud_client(&base, "gpt-4o");
    let error = client.chat(chat_request("hello")).await.unwrap_err();
    match error {
        AiError::Decode(message) => assert!(message.contains("no choices")),
        other => panic!("expected decode error, got: {other}"),
    }
}

#[tokio::test]
async fn sse_stream_assembles_deltas_and_finish() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (base, recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client = cloud_client(&base, "gpt-4o");

    let mut stream = client.chat_stream(chat_request("hello")).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }

    assert_eq!(
        events,
        vec![
            ChatStreamEvent::Content("Hel".to_string()),
            ChatStreamEvent::Content("lo".to_string()),
            ChatStreamEvent::Finish {
                reason: "stop".to_string()
            },
        ]
    );
    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.path, "/chat/completions");
    assert_eq!(recorded.body["stream"], true);
}

#[tokio::test]
async fn sse_stream_assembles_tool_call_fragments() {
    // The provider shape: id+name on the first fragment, the arguments
    // string split across three.
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"city\\\":\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Hangzhou\\\"}\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (base, _recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client = cloud_client(&base, "gpt-4o");

    let mut stream = client.chat_stream(chat_request("weather?")).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }

    assert_eq!(
        events,
        vec![
            ChatStreamEvent::ToolCall(rushwind_ai::ToolCall {
                id: "call_1".to_string(),
                kind: rushwind_ai::ToolKind::Function,
                function: rushwind_ai::ToolCallFunction {
                    name: "get_weather".to_string(),
                    arguments: "{\"city\":\"Hangzhou\"}".to_string(),
                },
            }),
            ChatStreamEvent::Finish {
                reason: "tool_calls".to_string()
            },
        ],
        "the fragments must assemble into one complete call, emitted before the finish"
    );
}

#[tokio::test]
async fn two_tool_calls_assemble_by_index() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"function\":{\"name\":\"f\",\"arguments\":\"1\"}},{\"index\":1,\"id\":\"call_b\",\"function\":{\"name\":\"g\",\"arguments\":\"2\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (base, _recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client = cloud_client(&base, "gpt-4o");
    let mut stream = client.chat_stream(chat_request("x")).await.unwrap();

    let mut calls = Vec::new();
    while let Some(event) = stream.next().await {
        if let ChatStreamEvent::ToolCall(call) = event.unwrap() {
            calls.push(call);
        }
    }
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].id, "call_a");
    assert_eq!(calls[1].id, "call_b");
    assert_eq!(calls[0].function.arguments, "1");
    assert_eq!(calls[1].function.arguments, "2");
}

#[tokio::test]
async fn eof_without_finish_reason_still_flushes_partial_tool_calls() {
    let sse = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"ci\"}}]}}]}\n\n";
    // The peer closes without a finish chunk — EOF flushes what arrived.
    let (base, _recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client = cloud_client(&base, "gpt-4o");
    let mut stream = client.chat_stream(chat_request("x")).await.unwrap();

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    assert_eq!(
        events,
        vec![ChatStreamEvent::ToolCall(rushwind_ai::ToolCall {
            id: "call_1".to_string(),
            kind: rushwind_ai::ToolKind::Function,
            function: rushwind_ai::ToolCallFunction {
                name: "get_weather".to_string(),
                arguments: "{\"ci".to_string(),
            },
        })],
        "what arrived is yielded assembled; the stream simply ends"
    );
}

/// The streaming and embedding faces ride trait objects — the
/// deployment shape — against the live mock.
#[tokio::test]
async fn streaming_and_embedding_faces_work_through_trait_objects() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (base, _recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client: Arc<dyn rushwind_ai::StreamingChatModel> = Arc::new(cloud_client(&base, "gpt-4o"));

    let mut stream = client.chat_stream(chat_request("hello")).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    assert_eq!(
        events,
        vec![
            ChatStreamEvent::Content("Hi".to_string()),
            ChatStreamEvent::Finish {
                reason: "stop".to_string()
            },
        ]
    );

    let canned = serde_json::json!({
        "data": [{"index": 0, "embedding": [1.0, 2.0]}]
    });
    let (base, recorded) = spawn(Reply::Json(canned)).await;
    let embedder: Arc<dyn rushwind_ai::EmbeddingModel> = Arc::new(cloud_client(&base, "chat-x"));
    let vectors = embedder
        .embed(Some("text-embedding-3-small"), &["doc"])
        .await
        .unwrap();
    assert_eq!(vectors, vec![vec![1.0, 2.0]]);
    // The explicit model overrides the configured default on the wire.
    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.body["model"], "text-embedding-3-small");
}

#[tokio::test]
async fn done_marker_alone_yields_an_empty_stream() {
    let sse = "data: [DONE]\n\n";
    let (base, _recorded) = spawn(Reply::Sse(sse.to_string())).await;
    let client = cloud_client(&base, "gpt-4o");
    let mut stream = client.chat_stream(chat_request("hello")).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.unwrap());
    }
    assert!(events.is_empty());
}

#[tokio::test]
async fn embeddings_parse_sorted_by_index() {
    let canned = serde_json::json!({
        "object": "list",
        "data": [
            {"object": "embedding", "index": 1, "embedding": [0.3, 0.4]},
            {"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}
        ]
    });
    let (base, recorded) = spawn(Reply::Json(canned)).await;
    let client = OpenAiClient::new(Config {
        model_type: ModelType::Cloud,
        model_name: "text-embedding-3-small".to_string(),
        timeout_seconds: 0,
        cloud: Some(CloudConfig {
            api_key: "sk-test".to_string(),
            base_url: base.clone(),
            organization: String::new(),
        }),
        local: None,
    })
    .unwrap();

    let vectors = client.embed(&["doc one", "doc two"]).await.unwrap();
    assert_eq!(vectors, vec![vec![0.1, 0.2], vec![0.3, 0.4]]);

    // The recorder holds only the last request — assert it before
    // embed_query overwrites it.
    let recorded = recorded.lock().unwrap().take().unwrap();
    assert_eq!(recorded.path, "/embeddings");
    assert_eq!(recorded.body["model"], "text-embedding-3-small");
    assert_eq!(
        recorded.body["input"],
        serde_json::json!(["doc one", "doc two"])
    );

    let query = client.embed_query("what is Rust?").await.unwrap();
    assert_eq!(query, vec![0.1, 0.2]);
}

#[tokio::test]
async fn from_settings_builds_and_rejects_malformed_shapes() {
    let (base, recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let client = OpenAiClient::from_settings(serde_json::json!({
        "model_type": "cloud",
        "model_name": "gpt-4o",
        "timeout_seconds": 30,
        "cloud": {"api_key": "sk-test", "base_url": base}
    }))
    .unwrap();
    assert_eq!(client.model(), "gpt-4o");
    client.chat(chat_request("hello")).await.unwrap();
    assert!(recorded.lock().unwrap().is_some());

    let error = OpenAiClient::from_settings(serde_json::json!({
        "model_name": "gpt-4o"
    }))
    .unwrap_err();
    assert!(matches!(error, AiError::Config(_)));

    let error = OpenAiClient::from_settings(serde_json::json!({
        "model_type": "cloud",
        "model_name": "gpt-4o"
    }))
    .unwrap_err();
    assert!(matches!(error, AiError::MissingCloudConfig));
}

#[tokio::test]
async fn empty_model_everywhere_is_typed_error() {
    let (base, _recorded) = spawn(Reply::Json(serde_json::from_str(CHAT_REPLY).unwrap())).await;
    let client = OpenAiClient::new(Config {
        model_type: ModelType::Cloud,
        model_name: String::new(),
        timeout_seconds: 0,
        cloud: Some(CloudConfig {
            api_key: "sk-test".to_string(),
            base_url: base.clone(),
            organization: String::new(),
        }),
        local: None,
    })
    .unwrap();
    let error = client.chat(chat_request("hello")).await.unwrap_err();
    assert!(matches!(error, AiError::EmptyModel));
}
