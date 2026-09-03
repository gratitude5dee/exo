//! `exo agentd`: the executor run surface.
//!
//! Exposes one agent over the same HTTP contract Hermes' `api_server`
//! speaks (`POST /v1/runs` + SSE `/v1/runs/{id}/events`, stop/approval,
//! `/api/sessions`, `/health`) so a control plane can drive exo turns without
//! linking the executor in-process. A session id maps to a conversation slug
//! on the served agent; a run is one turn executed through
//! `HarnessConversation::send_stream`.
//!
//! This is distinct from `exo serve`, which is the loopback-only unary
//! exoharness substrate (`POST /request`). See `exoharness/docs/runs.md`.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use actix_web::http::header;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use anyhow::{Context as _, Result, bail};
use executor::{
    CreateConversationRequest, ExecutionCancellation, ExecutionStreamEvent, Harness, HarnessAgent,
    HarnessConversation, SendRequest, ToolArguments, ToolResult, Uuid7,
};
use lingua::Message;
use lingua::universal::{AssistantContent, AssistantContentPart, UserContent, UserContentPart};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::tui::chunk_text;

pub(crate) const AGENTD_TRACING_TARGET: &str = "exo::agentd";
pub(crate) const API_SERVER_KEY_ENV: &str = "API_SERVER_KEY";

#[derive(Debug, Clone)]
pub(crate) struct AgentdConfig {
    pub bind: SocketAddr,
    pub agent: String,
    pub api_key: Option<String>,
}

/// SSE payloads emitted on `/v1/runs/{id}/events`. `event` doubles as the
/// SSE event name and the discriminator inside `data:` so both styles of
/// consumer (named-event listeners and `data:`-only frame scanners) work.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event")]
pub(crate) enum RunEvent {
    #[serde(rename = "run.started")]
    Started { run_id: String, session_id: String },
    #[serde(rename = "message.delta")]
    MessageDelta { delta: String },
    #[serde(rename = "tool.started")]
    ToolStarted {
        tool_call_id: String,
        tool: String,
        arguments: ToolArguments,
    },
    #[serde(rename = "tool.completed")]
    ToolCompleted {
        tool_call_id: String,
        tool: String,
        result: ToolResult,
    },
    #[serde(rename = "run.completed")]
    Completed { run_id: String, output: String },
    #[serde(rename = "run.failed")]
    Failed { run_id: String, error: String },
}

impl RunEvent {
    fn name(&self) -> &'static str {
        match self {
            RunEvent::Started { .. } => "run.started",
            RunEvent::MessageDelta { .. } => "message.delta",
            RunEvent::ToolStarted { .. } => "tool.started",
            RunEvent::ToolCompleted { .. } => "tool.completed",
            RunEvent::Completed { .. } => "run.completed",
            RunEvent::Failed { .. } => "run.failed",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, RunEvent::Completed { .. } | RunEvent::Failed { .. })
    }

    pub(crate) fn to_sse_frame(&self) -> String {
        let data = serde_json::to_string(self).expect("run event serializes");
        format!("event: {}\ndata: {data}\n\n", self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunStatus {
    Running,
    Completed,
    Failed,
    Stopped,
}

/// Terminal runs kept for late `/events` subscribers before being evicted.
pub(crate) const RETAINED_TERMINAL_RUNS: usize = 64;
/// Live events a subscriber may fall behind by before it is disconnected.
const SUBSCRIBER_BACKLOG: usize = 1024;

struct RunLog {
    events: Vec<RunEvent>,
    status: RunStatus,
    subscribers: Vec<mpsc::Sender<RunEvent>>,
    approval: Option<ApprovalRequest>,
}

pub(crate) struct Run {
    id: String,
    session_id: String,
    log: Mutex<RunLog>,
    task: Mutex<Option<JoinHandle<()>>>,
    cancellation: Mutex<Option<ExecutionCancellation>>,
}

impl Run {
    pub(crate) fn new(id: String, session_id: String) -> Arc<Self> {
        Arc::new(Self {
            id,
            session_id,
            log: Mutex::new(RunLog {
                events: Vec::new(),
                status: RunStatus::Running,
                subscribers: Vec::new(),
                approval: None,
            }),
            task: Mutex::new(None),
            cancellation: Mutex::new(None),
        })
    }

    pub(crate) fn publish(&self, event: RunEvent) {
        let mut log = self.log.lock().expect("run log poisoned");
        if log.status != RunStatus::Running {
            return;
        }
        match &event {
            RunEvent::Completed { .. } => log.status = RunStatus::Completed,
            RunEvent::Failed { .. } => log.status = RunStatus::Failed,
            _ => {}
        }
        log.subscribers
            .retain(|subscriber| subscriber.try_send(event.clone()).is_ok());
        let terminal = event.is_terminal();
        log.events.push(event);
        if terminal {
            log.subscribers.clear();
        }
    }

    /// Replay everything so far, then follow live events until the run ends.
    /// A subscriber that falls more than `SUBSCRIBER_BACKLOG` events behind is
    /// dropped rather than buffered without bound.
    pub(crate) fn subscribe(&self) -> mpsc::Receiver<RunEvent> {
        let mut log = self.log.lock().expect("run log poisoned");
        let (tx, rx) = mpsc::channel(log.events.len() + SUBSCRIBER_BACKLOG);
        for event in &log.events {
            if tx.try_send(event.clone()).is_err() {
                break;
            }
        }
        if log.status == RunStatus::Running {
            log.subscribers.push(tx);
        }
        rx
    }

    fn attach_cancellation(&self, cancellation: Option<ExecutionCancellation>) {
        *self.cancellation.lock().expect("run cancellation poisoned") = cancellation;
    }

    /// Cancel the executor's turn (the producer still finishes the turn
    /// record) and the relay, then mark the run stopped. Returns false when the run
    /// had already reached a terminal state.
    pub(crate) fn stop(&self) -> bool {
        let cancellation = self
            .cancellation
            .lock()
            .expect("run cancellation poisoned")
            .take();
        if let Some(cancellation) = &cancellation {
            cancellation.cancel();
        }
        let handle = self.task.lock().expect("run task poisoned").take();
        if let Some(handle) = &handle {
            handle.abort();
        }
        if handle.is_none() && cancellation.is_none() {
            return false;
        }
        if self.status() != RunStatus::Running {
            return false;
        }
        self.publish(RunEvent::Failed {
            run_id: self.id.clone(),
            error: "run stopped".to_string(),
        });
        self.log.lock().expect("run log poisoned").status = RunStatus::Stopped;
        true
    }

    fn status(&self) -> RunStatus {
        self.log.lock().expect("run log poisoned").status
    }

    fn is_terminal(&self) -> bool {
        self.status() != RunStatus::Running
    }
}

/// Evict the oldest terminal runs beyond `RETAINED_TERMINAL_RUNS`. Run ids are
/// UUIDv7, so lexical order is creation order.
pub(crate) fn evict_terminal_runs(runs: &mut HashMap<String, Arc<Run>>) {
    let mut terminal: Vec<&String> = runs
        .iter()
        .filter(|(_, run)| run.is_terminal())
        .map(|(id, _)| id)
        .collect();
    if terminal.len() <= RETAINED_TERMINAL_RUNS {
        return;
    }
    terminal.sort();
    let excess = terminal.len() - RETAINED_TERMINAL_RUNS;
    let evict: Vec<String> = terminal.into_iter().take(excess).cloned().collect();
    for id in evict {
        runs.remove(&id);
    }
}

struct AgentdState {
    agent: Arc<dyn HarnessAgent>,
    api_key: Option<String>,
    runs: Mutex<HashMap<String, Arc<Run>>>,
}

impl AgentdState {
    fn authorize(&self, request: &HttpRequest) -> Result<(), HttpResponse> {
        let Some(expected) = self.api_key.as_deref() else {
            return Ok(());
        };
        let presented = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if presented == Some(expected) {
            Ok(())
        } else {
            Err(HttpResponse::Unauthorized().json(ErrorBody {
                error: "invalid or missing bearer token".to_string(),
            }))
        }
    }

    fn run(&self, run_id: &str) -> Option<Arc<Run>> {
        self.runs
            .lock()
            .expect("runs poisoned")
            .get(run_id)
            .cloned()
    }

    async fn resolve_conversation(&self, session_id: &str) -> Result<Arc<dyn HarnessConversation>> {
        if let Some(conversation) = self.agent.get_conversation(session_id).await? {
            return Ok(conversation);
        }
        self.agent
            .create_conversation(CreateConversationRequest {
                slug: Some(session_id.to_string()),
                name: Some(session_id.to_string()),
                sandbox_image: None,
                sandbox_provider: None,
                shell_program: None,
            })
            .await
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

fn error_response(status: actix_web::http::StatusCode, error: impl ToString) -> HttpResponse {
    HttpResponse::build(status).json(ErrorBody {
        error: error.to_string(),
    })
}

fn not_found(what: &str) -> HttpResponse {
    error_response(
        actix_web::http::StatusCode::NOT_FOUND,
        format!("{what} not found"),
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConversationHistoryMessage {
    pub role: String,
}

/// `POST /v1/runs` body. `conversation_history` is accepted for contract
/// parity with Hermes but not replayed: the exo conversation is durable, so
/// the turn already sees every prior message in the session.
#[derive(Debug, Deserialize)]
pub(crate) struct CreateRunRequest {
    pub input: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub conversation_history: Vec<ConversationHistoryMessage>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct CreateRunResponse {
    run_id: String,
    session_id: String,
}

#[derive(Debug, Serialize)]
struct RunStatusResponse {
    run_id: String,
    session_id: String,
    status: RunStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApprovalRequest {
    pub approved: bool,
    #[serde(default)]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApprovalResponse {
    run_id: String,
    approved: bool,
    status: RunStatus,
}

#[derive(Debug, Serialize)]
struct SessionRow {
    id: String,
    title: String,
    message_count: usize,
}

#[derive(Debug, Deserialize)]
struct CreateSessionRequest {
    id: String,
    title: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MessageRow {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    agent: String,
    active_runs: usize,
}

fn user_content_text(content: &UserContent) -> String {
    match content {
        UserContent::String(text) => text.clone(),
        UserContent::Array(parts) => parts
            .iter()
            .filter_map(|part| match part {
                UserContentPart::Text(text) => Some(text.text.as_str()),
                UserContentPart::Image { .. } | UserContentPart::File { .. } => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

fn assistant_content_text(content: &AssistantContent) -> String {
    match content {
        AssistantContent::String(text) => text.clone(),
        AssistantContent::Array(parts) => parts
            .iter()
            .filter_map(|part| match part {
                AssistantContentPart::Text(text) => Some(text.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

/// Project the conversation transcript to the `{role, content}` rows the
/// session API returns. Tool/system/developer rows are omitted: consumers
/// only replay user/assistant text.
pub(crate) fn transcript_rows(messages: &[Message]) -> Vec<MessageRow> {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::User { content } => Some(MessageRow {
                role: "user",
                content: user_content_text(content),
            }),
            Message::Assistant { content, .. } => {
                let text = assistant_content_text(content);
                (!text.is_empty()).then_some(MessageRow {
                    role: "assistant",
                    content: text,
                })
            }
            Message::System { .. } | Message::Developer { .. } | Message::Tool { .. } => None,
        })
        .collect()
}

fn new_run_id() -> String {
    Uuid7::now().to_string()
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default()
}

async fn execute_run(run: Arc<Run>, conversation: Arc<dyn HarnessConversation>, input: String) {
    let started = now_millis();
    let mut stream = match conversation
        .send_stream(SendRequest {
            input: vec![Message::User {
                content: UserContent::String(input),
            }],
            session_id: None,
        })
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            run.publish(RunEvent::Failed {
                run_id: run.id.clone(),
                error: format!("{error:#}"),
            });
            return;
        }
    };
    run.attach_cancellation(stream.cancellation());
    let mut completed = false;
    let mut output = String::new();
    let mut tool_names: HashMap<String, String> = HashMap::new();
    while let Some(event) = stream.next().await {
        match event {
            Ok(ExecutionStreamEvent::FirstChunk { .. }) => {}
            Ok(ExecutionStreamEvent::Chunk(chunk)) => {
                let text = chunk_text(&chunk);
                if text.is_empty() {
                    continue;
                }
                output.push_str(&text);
                run.publish(RunEvent::MessageDelta { delta: text });
            }
            Ok(ExecutionStreamEvent::ToolCall {
                tool_call_id,
                tool_name,
                arguments,
            }) => {
                tool_names.insert(tool_call_id.clone(), tool_name.clone());
                run.publish(RunEvent::ToolStarted {
                    tool_call_id,
                    tool: tool_name,
                    arguments,
                });
            }
            Ok(ExecutionStreamEvent::ToolResult {
                tool_call_id,
                result,
            }) => {
                let tool = tool_names
                    .remove(&tool_call_id)
                    .unwrap_or_else(|| "tool".to_string());
                run.publish(RunEvent::ToolCompleted {
                    tool_call_id,
                    tool,
                    result,
                });
            }
            Ok(ExecutionStreamEvent::Completed(result)) => {
                if let Err(error) = conversation.close_session(result.session_id).await {
                    run.publish(RunEvent::Failed {
                        run_id: run.id.clone(),
                        error: format!("failed to close turn session: {error:#}"),
                    });
                    return;
                }
                completed = true;
            }
            Err(error) => {
                run.publish(RunEvent::Failed {
                    run_id: run.id.clone(),
                    error: format!("{error:#}"),
                });
                return;
            }
        }
    }
    if !completed {
        run.publish(RunEvent::Failed {
            run_id: run.id.clone(),
            error: "turn ended without completing".to_string(),
        });
        return;
    }
    if output.is_empty()
        && let Ok(messages) = conversation.messages().await
        && let Some(Message::Assistant { content, .. }) = messages.last()
    {
        output = assistant_content_text(content);
    }
    tracing::info!(
        target: AGENTD_TRACING_TARGET,
        run_id = %run.id,
        session_id = %run.session_id,
        elapsed_ms = %(now_millis().saturating_sub(started)),
        "run completed"
    );
    run.publish(RunEvent::Completed {
        run_id: run.id.clone(),
        output,
    });
}

async fn health(state: web::Data<Arc<AgentdState>>) -> impl Responder {
    let active_runs = state
        .runs
        .lock()
        .expect("runs poisoned")
        .values()
        .filter(|run| run.status() == RunStatus::Running)
        .count();
    HttpResponse::Ok().json(HealthResponse {
        status: "ok",
        agent: state.agent.record().slug.clone(),
        active_runs,
    })
}

async fn create_run(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    body: web::Json<CreateRunRequest>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let body = body.into_inner();
    if body.input.trim().is_empty() {
        return error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            "input must not be empty",
        );
    }
    let session_id = body.session_id.unwrap_or_else(new_run_id);
    let conversation = match state.resolve_conversation(&session_id).await {
        Ok(conversation) => conversation,
        Err(error) => {
            return error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                format!("failed to resolve session {session_id}: {error:#}"),
            );
        }
    };
    let run_id = new_run_id();
    let run = Run::new(run_id.clone(), session_id.clone());
    run.publish(RunEvent::Started {
        run_id: run_id.clone(),
        session_id: session_id.clone(),
    });
    {
        let mut runs = state.runs.lock().expect("runs poisoned");
        runs.insert(run_id.clone(), Arc::clone(&run));
        evict_terminal_runs(&mut runs);
    }
    tracing::info!(
        target: AGENTD_TRACING_TARGET,
        %run_id,
        %session_id,
        history_roles = ?body
            .conversation_history
            .iter()
            .map(|message| message.role.as_str())
            .collect::<Vec<_>>(),
        metadata_keys = body.metadata.len(),
        "run started"
    );
    let task = tokio::spawn(execute_run(Arc::clone(&run), conversation, body.input));
    *run.task.lock().expect("run task poisoned") = Some(task);
    HttpResponse::Accepted().json(CreateRunResponse { run_id, session_id })
}

async fn run_status(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let Some(run) = state.run(&path) else {
        return not_found("run");
    };
    HttpResponse::Ok().json(RunStatusResponse {
        run_id: run.id.clone(),
        session_id: run.session_id.clone(),
        status: run.status(),
    })
}

async fn run_events(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let Some(run) = state.run(&path) else {
        return not_found("run");
    };
    let frames = ReceiverStream::new(run.subscribe())
        .map(|event| Ok::<web::Bytes, actix_web::Error>(web::Bytes::from(event.to_sse_frame())));
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header((header::CACHE_CONTROL, "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(frames)
}

async fn stop_run(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let Some(run) = state.run(&path) else {
        return not_found("run");
    };
    run.stop();
    HttpResponse::Ok().json(RunStatusResponse {
        run_id: run.id.clone(),
        session_id: run.session_id.clone(),
        status: run.status(),
    })
}

/// Exo's executor has no human-in-the-loop gate today, so approvals are
/// recorded on the run (and a rejection stops it) without pausing execution.
async fn approve_run(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    path: web::Path<String>,
    body: web::Json<ApprovalRequest>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let Some(run) = state.run(&path) else {
        return not_found("run");
    };
    let approval = body.into_inner();
    let approved = approval.approved;
    tracing::info!(
        target: AGENTD_TRACING_TARGET,
        run_id = %run.id,
        approved,
        tool_call_id = ?approval.tool_call_id,
        "approval recorded"
    );
    run.log.lock().expect("run log poisoned").approval = Some(approval);
    if !approved {
        run.stop();
    }
    HttpResponse::Ok().json(ApprovalResponse {
        run_id: run.id.clone(),
        approved,
        status: run.status(),
    })
}

async fn session_row(conversation: &dyn HarnessConversation) -> Result<SessionRow> {
    let record = conversation.record();
    let message_count = transcript_rows(&conversation.messages().await?).len();
    Ok(SessionRow {
        id: record.slug.clone(),
        title: record.name.clone(),
        message_count,
    })
}

async fn list_sessions(request: HttpRequest, state: web::Data<Arc<AgentdState>>) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let records = match state.agent.list_conversations().await {
        Ok(records) => records,
        Err(error) => {
            return error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{error:#}"),
            );
        }
    };
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let conversation = match state.agent.get_conversation(&record.slug).await {
            Ok(Some(conversation)) => conversation,
            Ok(None) => continue,
            Err(error) => {
                return error_response(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{error:#}"),
                );
            }
        };
        match session_row(conversation.as_ref()).await {
            Ok(row) => rows.push(row),
            Err(error) => {
                return error_response(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("{error:#}"),
                );
            }
        }
    }
    HttpResponse::Ok().json(rows)
}

async fn create_session(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    body: web::Json<CreateSessionRequest>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let body = body.into_inner();
    match state.agent.get_conversation(&body.id).await {
        Ok(Some(_)) => {
            return error_response(
                actix_web::http::StatusCode::CONFLICT,
                format!("session {} already exists", body.id),
            );
        }
        Ok(None) => {}
        Err(error) => {
            return error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{error:#}"),
            );
        }
    }
    let created = state
        .agent
        .create_conversation(CreateConversationRequest {
            slug: Some(body.id.clone()),
            name: Some(body.title.unwrap_or_else(|| body.id.clone())),
            sandbox_image: None,
            sandbox_provider: None,
            shell_program: None,
        })
        .await;
    match created {
        Ok(conversation) => match session_row(conversation.as_ref()).await {
            Ok(row) => HttpResponse::Created().json(row),
            Err(error) => error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{error:#}"),
            ),
        },
        Err(error) => error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            format!("{error:#}"),
        ),
    }
}

async fn session_messages(
    request: HttpRequest,
    state: web::Data<Arc<AgentdState>>,
    path: web::Path<String>,
) -> impl Responder {
    if let Err(response) = state.authorize(&request) {
        return response;
    }
    let conversation = match state.agent.get_conversation(&path).await {
        Ok(Some(conversation)) => conversation,
        Ok(None) => return not_found("session"),
        Err(error) => {
            return error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("{error:#}"),
            );
        }
    };
    match conversation.messages().await {
        Ok(messages) => HttpResponse::Ok().json(transcript_rows(&messages)),
        Err(error) => error_response(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("{error:#}"),
        ),
    }
}

pub(crate) async fn serve_agentd(harness: Arc<dyn Harness>, config: AgentdConfig) -> Result<()> {
    if config.api_key.as_deref() == Some("") {
        bail!(
            "{API_SERVER_KEY_ENV} is set but empty; unset it for a loopback-only server or provide a real key"
        );
    }
    if config.api_key.is_none() && !config.bind.ip().is_loopback() {
        bail!(
            "exo agentd binds {} but {API_SERVER_KEY_ENV} is unset; a non-loopback run surface requires a bearer key",
            config.bind
        );
    }
    let agent = harness
        .get_agent(&config.agent)
        .await?
        .with_context(|| format!("agent `{}` not found", config.agent))?;
    let state = Arc::new(AgentdState {
        agent,
        api_key: config.api_key,
        runs: Mutex::new(HashMap::new()),
    });
    let listener = TcpListener::bind(config.bind)?;
    let addr = listener.local_addr()?;
    tracing::info!(
        target: AGENTD_TRACING_TARGET,
        %addr,
        agent = %config.agent,
        authenticated = state.api_key.is_some(),
        "serving exo agentd"
    );
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Arc::clone(&state)))
            .route("/health", web::get().to(health))
            .route("/v1/runs", web::post().to(create_run))
            .route("/v1/runs/{run_id}", web::get().to(run_status))
            .route("/v1/runs/{run_id}/events", web::get().to(run_events))
            .route("/v1/runs/{run_id}/stop", web::post().to(stop_run))
            .route("/v1/runs/{run_id}/approval", web::post().to(approve_run))
            .route("/api/sessions", web::get().to(list_sessions))
            .route("/api/sessions", web::post().to(create_session))
            .route(
                "/api/sessions/{session_id}/messages",
                web::get().to(session_messages),
            )
    })
    .disable_signals()
    .listen(listener)?
    .run()
    .await?;
    harness.flush_tracing().await?;
    Ok(())
}
