// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Response, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use tempfile::tempdir;

use super::*;
use crate::scenario::{GeneratedScenario, GeneratorConfig};
use crate::token_shape::SafeTokenAlphabet;
use crate::trace::{AgentContext, LoadedTrace, TraceRequest};

#[test]
fn target_normalization_accepts_base_or_endpoint() {
    assert_eq!(
        normalize_target("http://localhost:8000"),
        "http://localhost:8000/v1/chat/completions"
    );
    assert_eq!(
        normalize_target("http://localhost:8000/v1/chat/completions"),
        "http://localhost:8000/v1/chat/completions"
    );
}

#[test]
fn calculates_nearest_rank_percentiles() {
    let values = (1..=100).map(|value| value as f64).collect();
    let result = percentiles(values);
    assert_eq!(result.p50, 50.0);
    assert_eq!(result.p95, 95.0);
    assert_eq!(result.p99, 99.0);
}

#[test]
fn hard_dispatch_limit_uses_the_unrounded_maximum() {
    let mut accumulator = RunAccumulator::new(10.0).unwrap();
    accumulator.request_count = 1;
    accumulator.dispatch_at_or_below_p99_limit = 1;
    accumulator.dispatch_max_ms = 5.000_4;
    assert!(!dispatch_timing_matches(&accumulator, 5.0));
}

#[test]
fn detects_output_chunks() {
    assert!(!chunk_contains_output(
        &json!({"choices":[{"delta":{"role":"assistant"}}]})
    ));
    assert!(chunk_contains_output(
        &json!({"choices":[{"delta":{"content":"x"}}]})
    ));
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Option<(HeaderMap, serde_json::Value)>>>);

async fn shape_endpoint(
    State(capture): State<Capture>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response<Body> {
    *capture.0.lock().unwrap() = Some((headers, body));
    if capture
        .0
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|(headers, _)| headers.get("x-dynamo-session-final"))
        .is_some_and(|value| value == "true")
    {
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from("data: [DONE]\n\n"))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n",
            "data: [DONE]\n\n"
        )))
        .unwrap()
}

#[tokio::test]
async fn replay_sends_shape_and_agent_headers() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let request = TraceRequest {
        ordinal: 0,
        source_request_id: "source-1".to_string(),
        source_x_request_id: None,
        source_model: None,
        input_tokens: 3,
        output_tokens: 2,
        request_received_ms: 1000,
        trace_block_size: 16,
        input_sequence_hashes: vec![11],
        agent_context: Some(AgentContext {
            session_id: "thread-1".to_string(),
            parent_session_id: None,
            session_final: None,
            compaction: Some(serde_json::json!({"phase": "mid_turn"})),
            input_trigger: Some("tool_result".to_string()),
        }),
    };
    let trace = LoadedTrace {
        requests: vec![request.clone()],
        manifest: TraceManifest {
            request_count: 1,
            zero_output_requests: 0,
            session_count: 1,
            requests_with_agent_context: 1,
            first_request_received_ms: 1000,
            last_request_received_ms: 1000,
            duration_ms: 0,
            input_tokens: 3,
            output_tokens: 2,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let dictionary = TokenDictionary::build(
        &[request],
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_replay(
        trace,
        dictionary,
        ReplayOptions {
            agent: AgentKind::Codex,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 1,
            warmup_connections: 0,
            http_transport: HttpTransport::Http2PriorKnowledge,
            prepare_lookahead: Some(Duration::from_millis(100)),
            result_flush_interval: 1,
            max_dispatch_p99_ms: 100.0,
            max_dispatch_max_ms: 100.0,
            start_delay: Duration::from_millis(5),
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            token_path_verified: false,
            engine_cache_mode: BTreeMap::new(),
            static_headers: Vec::new(),
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.succeeded, 1);
    assert_eq!(summary.output_length_matches, 1);
    assert!(summary.passed);
    assert!(summary.total_time_ms >= 5.0);
    let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
    assert_eq!(headers.get("thread-id").unwrap(), "thread-1");
    assert_eq!(headers.get("x-dynamo-session-id").unwrap(), "thread-1");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            headers
                .get("x-codex-turn-metadata")
                .unwrap()
                .to_str()
                .unwrap(),
        )
        .unwrap(),
        serde_json::json!({
            "request_kind": "compaction",
            "compaction": {"phase": "mid_turn"}
        })
    );
    assert_eq!(body["max_tokens"], 2);
    assert!(body.get("min_tokens").is_none());
    assert_eq!(body["ignore_eos"], true);
    assert_eq!(body["messages"][0]["role"], "assistant");
    assert_eq!(body["messages"][1]["role"], "tool");
    assert_eq!(
        body["messages"][1]["tool_call_id"],
        "agent-loadgen-shape-tool"
    );
    assert_eq!(body["nvext"]["token_data"].as_array().unwrap().len(), 3);
    assert!(output.path().join("run.json").is_file());
    assert!(output.path().join("requests.jsonl").is_file());
    server.abort();
}

#[tokio::test]
async fn replay_sends_session_close_without_model_shape() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let request = TraceRequest {
        ordinal: 0,
        source_request_id: "source-close".to_string(),
        source_x_request_id: None,
        source_model: None,
        input_tokens: 3,
        output_tokens: 0,
        request_received_ms: 1000,
        trace_block_size: 16,
        input_sequence_hashes: vec![11],
        agent_context: Some(AgentContext {
            session_id: "thread-1".to_string(),
            parent_session_id: None,
            session_final: Some(true),
            compaction: None,
            input_trigger: Some("other".to_string()),
        }),
    };
    let trace = LoadedTrace {
        requests: vec![request.clone()],
        manifest: TraceManifest {
            request_count: 1,
            zero_output_requests: 1,
            session_count: 1,
            requests_with_agent_context: 1,
            first_request_received_ms: 1000,
            last_request_received_ms: 1000,
            duration_ms: 0,
            input_tokens: 3,
            output_tokens: 0,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let dictionary = TokenDictionary::build(
        &[request],
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_replay(
        trace,
        dictionary,
        ReplayOptions {
            agent: AgentKind::Codex,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 1,
            warmup_connections: 0,
            http_transport: HttpTransport::Http2PriorKnowledge,
            prepare_lookahead: Some(Duration::from_millis(100)),
            result_flush_interval: 1,
            max_dispatch_p99_ms: 100.0,
            max_dispatch_max_ms: 100.0,
            start_delay: Duration::from_millis(5),
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            token_path_verified: true,
            engine_cache_mode: BTreeMap::from([("ownership".to_string(), "session".to_string())]),
            static_headers: Vec::new(),
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.model_turns, 0);
    assert_eq!(summary.session_closes, 1);
    assert_eq!(summary.control_only_matches, 1);
    assert!(summary.passed);
    assert!(summary.capacity_performance_conclusions_allowed);
    let (headers, body) = capture.0.lock().unwrap().clone().unwrap();
    assert_eq!(headers.get("x-dynamo-session-final").unwrap(), "true");
    assert!(body.get("nvext").is_none());
    assert!(body.get("max_tokens").is_none());
    assert!(body.get("ignore_eos").is_none());
    let run: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(output.path().join("run.json")).unwrap())
            .unwrap();
    assert_eq!(run["protocol_surface"], "chat_completions");
    assert_eq!(run["traffic_kind"], "captured_trace");
    assert_eq!(run["token_path_verified"], true);
    assert_eq!(run["engine_cache_mode"]["ownership"], "session");
    assert!(run.get("fidelity").is_none());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recorded_scheduler_handles_tied_millisecond_arrivals() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let requests = (0..96)
        .map(|ordinal| TraceRequest {
            ordinal,
            source_request_id: format!("source-{ordinal}"),
            source_x_request_id: None,
            source_model: None,
            input_tokens: 3,
            output_tokens: 2,
            request_received_ms: 1_000 + (ordinal / 3) as u64,
            trace_block_size: 16,
            input_sequence_hashes: vec![11],
            agent_context: Some(AgentContext {
                session_id: format!("thread-{}", ordinal % 12),
                parent_session_id: None,
                session_final: None,
                compaction: None,
                input_trigger: Some("user_message".to_string()),
            }),
        })
        .collect::<Vec<_>>();
    let dictionary = TokenDictionary::build(
        &requests,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let trace = LoadedTrace {
        requests,
        manifest: TraceManifest {
            request_count: 96,
            zero_output_requests: 0,
            session_count: 12,
            requests_with_agent_context: 96,
            first_request_received_ms: 1_000,
            last_request_received_ms: 1_031,
            duration_ms: 31,
            input_tokens: 288,
            output_tokens: 192,
            distinct_sequence_hashes: 1,
            trace_block_size: 16,
            source_digest_sha256: "source".to_string(),
        },
    };
    let output = tempdir().unwrap();
    let summary = run_replay(
        trace,
        dictionary,
        ReplayOptions {
            agent: AgentKind::Codex,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 96,
            warmup_connections: 0,
            http_transport: HttpTransport::Auto,
            prepare_lookahead: Some(Duration::from_millis(100)),
            result_flush_interval: 1,
            max_dispatch_p99_ms: 100.0,
            max_dispatch_max_ms: 100.0,
            start_delay: Duration::from_millis(25),
            timeout: Duration::from_secs(5),
            time_scale: 1.0,
            token_path_verified: false,
            engine_cache_mode: BTreeMap::new(),
            static_headers: Vec::new(),
        },
    )
    .await
    .unwrap();

    assert_eq!(summary.succeeded, 96);
    assert_eq!(summary.output_length_matches, 96);
    assert!(summary.scheduler_wake_lag_ms.p99 < 100.0);
    assert!(summary.dispatch_lag_ms.p99 < 100.0);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_graph_releases_tool_successor_after_completion() {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/chat/completions", post(shape_endpoint))
        .with_state(capture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config: GeneratorConfig = toml::from_str(
        r#"
                schema_version = 3
                agent = "codex"
                seed = 9

                [load]
                root_sessions = 1
                concurrent_agents = 1

                [trajectory]
                turns = { kind = "fixed", value = 2 }
                output_tokens = { kind = "fixed", value = 2 }

                [tokens]
                system_prefix_tokens = { kind = "fixed", value = 16 }
                tool_catalog_tokens = { kind = "fixed", value = 16 }
                repository_tokens = { kind = "fixed", value = 16 }
                session_tokens = { kind = "fixed", value = 16 }
                user_tokens = { kind = "fixed", value = 16 }

                [behavior]
                tool_probability = 1.0
                parallel_tool_probability = 0.0
                subagent_probability = 0.0
                swarm_probability = 0.0
                completion_probability = 0.0

                [compaction]
                enabled = false

                [subagents]
                max_depth = 0
            "#,
    )
    .unwrap();
    let scenario = GeneratedScenario::generate(config.resolve().unwrap()).unwrap();
    assert_eq!(scenario.nodes.len(), 3);
    assert_eq!(scenario.nodes[1].dependencies, vec![0]);
    assert_eq!(scenario.nodes[2].dependencies, vec![1]);
    let dictionary = TokenDictionary::new(
        scenario.trace_manifest.trace_block_size,
        scenario.trace_manifest.distinct_sequence_hashes,
        SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap(),
    )
    .unwrap();
    let output = tempdir().unwrap();
    let summary = run_generated_scenario(
        &scenario,
        dictionary,
        ReplayOptions {
            agent: AgentKind::Codex,
            model: "test-model".to_string(),
            target: format!("http://{address}"),
            output_dir: output.path().to_path_buf(),
            max_in_flight: 2,
            warmup_connections: 0,
            http_transport: HttpTransport::Auto,
            prepare_lookahead: None,
            result_flush_interval: 1,
            max_dispatch_p99_ms: 100.0,
            max_dispatch_max_ms: 100.0,
            start_delay: Duration::from_millis(5),
            timeout: Duration::from_secs(5),
            time_scale: 100.0,
            token_path_verified: false,
            engine_cache_mode: BTreeMap::new(),
            static_headers: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert_eq!(summary.succeeded, 3);
    assert_eq!(summary.model_turns, 2);
    assert_eq!(summary.session_closes, 1);
    assert!(summary.passed);
    assert!(output.path().join("scenario.json").is_file());
    server.abort();
}

#[test]
fn scaled_offsets_use_integer_nanoseconds() {
    assert_eq!(scaled_offset_ns(1, 1.0).unwrap(), 1_000_000);
    assert_eq!(scaled_offset_ns(1, 2.0).unwrap(), 500_000);
    assert_eq!(scaled_offset_ns(1, 3.0).unwrap(), 333_333);
}
