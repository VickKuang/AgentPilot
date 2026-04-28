use std::{
    collections::HashMap,
    env, fs,
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path as AxumPath, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, patch, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_postgres::NoTls;
use tower_http::services::ServeDir;
use tracing::{error, info};
use uuid::Uuid;

const STORE_FILE: &str = "data/store.json";

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<Store>>,
    backend: Arc<dyn StoreBackend>,
    realtime: Arc<RealtimeHub>,
    api_key: Option<String>,
    driver_base_url: Option<String>,
    integrations: IntegrationConfig,
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct Store {
    tasks: HashMap<Uuid, TestTask>,
    reports: HashMap<Uuid, TestReport>,
    tool_calls: HashMap<Uuid, Vec<ToolCallLog>>,
    snapshots: HashMap<Uuid, Vec<PageSnapshot>>,
}

impl Store {
    fn load_from_file() -> Self {
        let file = Path::new(STORE_FILE);
        if !file.exists() {
            return Self::default();
        }

        match fs::read_to_string(file) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    fn save_to_file(&self) -> Result<(), String> {
        fs::create_dir_all("data").map_err(|e| format!("create data dir failed: {}", e))?;
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| format!("serialize store failed: {}", e))?;
        fs::write(STORE_FILE, content).map_err(|e| format!("write store failed: {}", e))
    }
}

#[derive(Debug, Clone)]
struct RequestContext {
    tenant_id: String,
}

#[derive(Debug, Clone)]
struct IntegrationConfig {
    jira_webhook_url: Option<String>,
    feishu_webhook_url: Option<String>,
    wecom_webhook_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RealtimeEvent {
    event_type: String,
    task_id: Uuid,
    status: Option<TaskStatus>,
    message: String,
    timestamp: DateTime<Utc>,
}

#[derive(Default)]
struct RealtimeHub {
    channels: RwLock<HashMap<Uuid, broadcast::Sender<RealtimeEvent>>>,
}

impl RealtimeHub {
    async fn subscribe(&self, task_id: Uuid) -> broadcast::Receiver<RealtimeEvent> {
        let mut channels = self.channels.write().await;
        channels
            .entry(task_id)
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    async fn publish(&self, event: RealtimeEvent) {
        let mut channels = self.channels.write().await;
        let tx = channels
            .entry(event.task_id)
            .or_insert_with(|| broadcast::channel(256).0)
            .clone();
        let _ = tx.send(event);
    }
}

#[derive(Debug)]
struct PgStoreBackend {
    database_url: String,
}

trait StoreBackend: Send + Sync {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Store, String>> + Send + 'a>>;

    fn save<'a>(
        &'a self,
        store: Store,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Debug, Default)]
struct JsonStoreBackend;

impl StoreBackend for JsonStoreBackend {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Store, String>> + Send + 'a>>
    {
        Box::pin(async move { Ok(Store::load_from_file()) })
    }

    fn save<'a>(
        &'a self,
        store: Store,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { store.save_to_file() })
    }
}

impl StoreBackend for PgStoreBackend {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Store, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let (client, conn) = tokio_postgres::connect(&self.database_url, NoTls)
                .await
                .map_err(|e| format!("postgres connect failed: {}", e))?;
            tokio::spawn(async move {
                if let Err(err) = conn.await {
                    error!("postgres connection error: {}", err);
                }
            });
            client
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS app_store(
                        id SMALLINT PRIMARY KEY,
                        payload JSONB NOT NULL
                    )",
                )
                .await
                .map_err(|e| format!("create table failed: {}", e))?;
            let row = client
                .query_opt("SELECT payload FROM app_store WHERE id=1", &[])
                .await
                .map_err(|e| format!("query store failed: {}", e))?;
            match row {
                Some(r) => {
                    let payload: serde_json::Value = r.get(0);
                    serde_json::from_value(payload)
                        .map_err(|e| format!("decode store failed: {}", e))
                }
                None => Ok(Store::default()),
            }
        })
    }

    fn save<'a>(
        &'a self,
        store: Store,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let payload =
                serde_json::to_value(store).map_err(|e| format!("encode store failed: {}", e))?;
            let (client, conn) = tokio_postgres::connect(&self.database_url, NoTls)
                .await
                .map_err(|e| format!("postgres connect failed: {}", e))?;
            tokio::spawn(async move {
                if let Err(err) = conn.await {
                    error!("postgres connection error: {}", err);
                }
            });
            client
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS app_store(
                        id SMALLINT PRIMARY KEY,
                        payload JSONB NOT NULL
                    )",
                )
                .await
                .map_err(|e| format!("create table failed: {}", e))?;
            client
                .execute(
                    "INSERT INTO app_store(id,payload) VALUES(1,$1)
                     ON CONFLICT(id) DO UPDATE SET payload = EXCLUDED.payload",
                    &[&payload],
                )
                .await
                .map_err(|e| format!("save store failed: {}", e))?;
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TaskStatus {
    Pending,
    Running,
    Paused,
    Passed,
    Failed,
    Blocked,
    Terminated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ActionType {
    Observe,
    Tap,
    Input,
    Swipe,
    Verify,
    AgentAct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestTask {
    task_id: Uuid,
    tenant_id: String,
    task_name: String,
    user_goal: String,
    scenario: String,
    status: TaskStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    params: serde_json::Value,
    required_data: Vec<String>,
    missing_data: Vec<String>,
    planned_steps: Vec<PlannedStep>,
    step_logs: Vec<StepLog>,
    retries: u8,
    max_retries: u8,
    max_step_retries: u8,
    step_timeout_ms: u64,
    global_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlannedStep {
    step_order: u32,
    description: String,
    action_type: ActionType,
    action_params: serde_json::Value,
    expected_result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StepLog {
    step_order: u32,
    step_name: String,
    action_type: ActionType,
    action_params: serde_json::Value,
    expected_result: String,
    actual_result: String,
    status: String,
    retry_count: u8,
    screenshot_url: Option<String>,
    page_tree: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestReport {
    report_id: Uuid,
    task_id: Uuid,
    result: TaskStatus,
    summary: String,
    issue_summary: String,
    execution_steps: Vec<String>,
    actual_result: String,
    expected_result: String,
    steps: Vec<StepLog>,
    bug_report: Option<BugReport>,
    screenshots: Vec<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BugReport {
    bug_title: String,
    severity: String,
    reproduction_steps: Vec<String>,
    actual_result: String,
    expected_result: String,
    evidence: Vec<String>,
    possible_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolCallLog {
    id: Uuid,
    task_id: Uuid,
    step_order: u32,
    tool_name: String,
    request_payload: serde_json::Value,
    response_payload: serde_json::Value,
    success: bool,
    latency_ms: u128,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageSnapshot {
    id: Uuid,
    task_id: Uuid,
    step_order: u32,
    screenshot_url: String,
    page_tree: serde_json::Value,
    current_page: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreateTaskRequest {
    task_name: String,
    user_goal: String,
    params: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct UpdateTaskDataRequest {
    params: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct CreateTaskResponse {
    task_id: Uuid,
    scenario: String,
    status: TaskStatus,
    required_data: Vec<String>,
    missing_data: Vec<String>,
    planned_steps: Vec<PlannedStep>,
}

#[derive(Debug, Serialize)]
struct TaskProgress {
    task_id: Uuid,
    status: TaskStatus,
    total_steps: usize,
    done_steps: usize,
    success_steps: usize,
    failed_steps: usize,
    progress_percent: u8,
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

fn request_context(headers: &HeaderMap, state: &AppState) -> Result<RequestContext, ApiError> {
    let tenant_id = headers
        .get("x-tenant-id")
        .and_then(|x| x.to_str().ok())
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .ok_or(ApiError {
            code: "missing_tenant",
            message: "missing x-tenant-id header".to_string(),
        })?;

    if let Some(expected) = &state.api_key {
        let got = headers
            .get("x-api-key")
            .and_then(|x| x.to_str().ok())
            .map(str::to_string)
            .ok_or(ApiError {
                code: "missing_api_key",
                message: "missing x-api-key header".to_string(),
            })?;
        if got != *expected {
            return Err(ApiError {
                code: "invalid_api_key",
                message: "invalid x-api-key".to_string(),
            });
        }
    }

    Ok(RequestContext { tenant_id })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let backend: Arc<dyn StoreBackend> = match env::var("DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => Arc::new(PgStoreBackend { database_url: url }),
        _ => Arc::new(JsonStoreBackend),
    };
    let initial_store = backend.load().await.unwrap_or_default();
    let state = AppState {
        store: Arc::new(RwLock::new(initial_store)),
        backend,
        realtime: Arc::new(RealtimeHub::default()),
        api_key: env::var("API_KEY").ok(),
        driver_base_url: env::var("DRIVER_BASE_URL").ok(),
        integrations: IntegrationConfig {
            jira_webhook_url: env::var("JIRA_WEBHOOK_URL").ok(),
            feishu_webhook_url: env::var("FEISHU_WEBHOOK_URL").ok(),
            wecom_webhook_url: env::var("WECOM_WEBHOOK_URL").ok(),
        },
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/tasks", post(create_task).get(list_tasks))
        .route("/api/v1/tasks/:task_id", get(get_task))
        .route("/api/v1/tasks/:task_id/data", patch(update_task_data))
        .route("/api/v1/tasks/:task_id/start", post(start_task))
        .route("/api/v1/tasks/:task_id/retry", post(retry_task))
        .route("/api/v1/tasks/:task_id/pause", post(pause_task))
        .route("/api/v1/tasks/:task_id/resume", post(resume_task))
        .route("/api/v1/tasks/:task_id/terminate", post(terminate_task))
        .route("/api/v1/tasks/:task_id/progress", get(get_progress))
        .route("/api/v1/tasks/:task_id/logs", get(get_logs))
        .route("/api/v1/tasks/:task_id/tool-calls", get(get_tool_calls))
        .route("/api/v1/tasks/:task_id/snapshots", get(get_snapshots))
        .route("/api/v1/tasks/:task_id/ws", get(task_ws))
        .route("/api/v1/tasks/:task_id/report", get(get_report))
        .route("/api/v1/tasks/:task_id/bug-report", get(get_bug_report))
        .route(
            "/api/v1/tasks/:task_id/report/export",
            get(export_report_markdown),
        )
        .nest_service("/", ServeDir::new("web"))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("autotest-agent listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok", "service": "autotest-agent"}))
}

async fn create_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<CreateTaskResponse>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let params = req.params.unwrap_or_else(|| serde_json::json!({}));
    let plan = planner_plan(&req.user_goal, &params);

    let task = TestTask {
        task_id: Uuid::new_v4(),
        tenant_id: ctx.tenant_id,
        task_name: req.task_name,
        user_goal: req.user_goal,
        scenario: plan.scenario.clone(),
        status: if plan.missing_data.is_empty() {
            TaskStatus::Pending
        } else {
            TaskStatus::Blocked
        },
        created_at: Utc::now(),
        updated_at: Utc::now(),
        params: mask_sensitive_json(&params),
        required_data: plan.required_data.clone(),
        missing_data: plan.missing_data.clone(),
        planned_steps: plan.steps.clone(),
        step_logs: vec![],
        retries: 0,
        max_retries: 2,
        max_step_retries: 2,
        step_timeout_ms: 4_000,
        global_timeout_ms: 45_000,
    };

    let task_id = task.task_id;
    let status = task.status.clone();

    let mut store = state.store.write().await;
    store.tasks.insert(task_id, task);
    store.tool_calls.insert(task_id, vec![]);
    store.snapshots.insert(task_id, vec![]);
    persist_store(&state, &store).await;

    Ok(Json(CreateTaskResponse {
        task_id,
        scenario: plan.scenario,
        status,
        required_data: plan.required_data,
        missing_data: plan.missing_data,
        planned_steps: plan.steps,
    }))
}

async fn update_task_data(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<UpdateTaskDataRequest>,
) -> Result<Json<TestTask>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let mut store = state.store.write().await;
    let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }

    let merged = merge_json(task.params.clone(), req.params.clone());
    task.params = mask_sensitive_json(&merged);

    task.missing_data = task
        .required_data
        .iter()
        .filter(|key| merged.get(key.as_str()).is_none())
        .cloned()
        .collect();

    task.status = if task.missing_data.is_empty() {
        TaskStatus::Pending
    } else {
        TaskStatus::Blocked
    };
    task.updated_at = Utc::now();

    let out = task.clone();
    persist_store(&state, &store).await;
    Ok(Json(out))
}

async fn list_tasks(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<TestTask>>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    Ok(Json(
        state
            .store
            .read()
            .await
            .tasks
            .values()
            .filter(|t| t.tenant_id == ctx.tenant_id)
            .cloned()
            .collect(),
    ))
}

async fn get_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TestTask>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).cloned().ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    Ok(Json(task))
}

async fn start_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    {
        let mut store = state.store.write().await;
        let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        })?;
        if task.tenant_id != ctx.tenant_id {
            return Err(ApiError {
                code: "task_not_found",
                message: format!("task {} not found", task_id),
            });
        }

        if !task.missing_data.is_empty() {
            return Err(ApiError {
                code: "missing_required_data",
                message: format!("缺少参数: {}", task.missing_data.join(", ")),
            });
        }

        task.status = TaskStatus::Running;
        task.updated_at = Utc::now();
        task.step_logs.clear();
        store.reports.remove(&task_id);
        store.tool_calls.entry(task_id).or_default().clear();
        store.snapshots.entry(task_id).or_default().clear();
        persist_store(&state, &store).await;
    }

    let cloned = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_task_pipeline(task_id, cloned).await {
            error!("task {} pipeline failed: {}", task_id, err);
        }
    });
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(TaskStatus::Running),
            message: "task started".to_string(),
            timestamp: Utc::now(),
        })
        .await;

    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "running"}),
    ))
}

async fn retry_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    {
        let mut store = state.store.write().await;
        let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        })?;
        if task.tenant_id != ctx.tenant_id {
            return Err(ApiError {
                code: "task_not_found",
                message: format!("task {} not found", task_id),
            });
        }

        if task.retries >= task.max_retries {
            return Err(ApiError {
                code: "retry_exhausted",
                message: "max retries exceeded".to_string(),
            });
        }

        task.retries += 1;
        task.status = TaskStatus::Running;
        task.step_logs.clear();
        task.updated_at = Utc::now();
        store.reports.remove(&task_id);
        persist_store(&state, &store).await;
    }

    let cloned = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_task_pipeline(task_id, cloned).await {
            error!("retry task {} pipeline failed: {}", task_id, err);
        }
    });
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(TaskStatus::Running),
            message: "task retried".to_string(),
            timestamp: Utc::now(),
        })
        .await;

    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "running"}),
    ))
}

async fn pause_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let mut store = state.store.write().await;
    let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    task.status = TaskStatus::Paused;
    task.updated_at = Utc::now();
    persist_store(&state, &store).await;
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(TaskStatus::Paused),
            message: "task paused".to_string(),
            timestamp: Utc::now(),
        })
        .await;
    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "paused"}),
    ))
}

async fn resume_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    {
        let mut store = state.store.write().await;
        let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        })?;
        if task.tenant_id != ctx.tenant_id {
            return Err(ApiError {
                code: "task_not_found",
                message: format!("task {} not found", task_id),
            });
        }
        if task.status != TaskStatus::Paused {
            return Err(ApiError {
                code: "not_paused",
                message: "task is not paused".to_string(),
            });
        }
        task.status = TaskStatus::Running;
        task.updated_at = Utc::now();
        persist_store(&state, &store).await;
    }

    let cloned = state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_task_pipeline(task_id, cloned).await {
            error!("resume task {} pipeline failed: {}", task_id, err);
        }
    });
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(TaskStatus::Running),
            message: "task resumed".to_string(),
            timestamp: Utc::now(),
        })
        .await;

    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "running"}),
    ))
}

async fn terminate_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let mut store = state.store.write().await;
    let task = store.tasks.get_mut(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }

    task.status = TaskStatus::Terminated;
    task.updated_at = Utc::now();
    persist_store(&state, &store).await;
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(TaskStatus::Terminated),
            message: "task terminated".to_string(),
            timestamp: Utc::now(),
        })
        .await;
    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "terminated"}),
    ))
}

async fn get_progress(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TaskProgress>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }

    let done = task.step_logs.len();
    let total = task.planned_steps.len();
    let success = task
        .step_logs
        .iter()
        .filter(|s| s.status == "success")
        .count();
    let failed = task
        .step_logs
        .iter()
        .filter(|s| s.status == "failed")
        .count();

    Ok(Json(TaskProgress {
        task_id,
        status: task.status.clone(),
        total_steps: total,
        done_steps: done,
        success_steps: success,
        failed_steps: failed,
        progress_percent: if total == 0 {
            0
        } else {
            ((done as f32 / total as f32) * 100.0).round() as u8
        },
    }))
}

async fn get_logs(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<StepLog>>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    Ok(Json(task.step_logs.clone()))
}

async fn get_tool_calls(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ToolCallLog>>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    Ok(Json(
        store.tool_calls.get(&task_id).cloned().unwrap_or_default(),
    ))
}

async fn get_snapshots(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<PageSnapshot>>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    Ok(Json(
        store.snapshots.get(&task_id).cloned().unwrap_or_default(),
    ))
}

async fn get_report(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TestReport>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    let report = store.reports.get(&task_id).cloned().ok_or(ApiError {
        code: "report_not_ready",
        message: "report not generated".to_string(),
    })?;
    Ok(Json(report))
}

async fn get_bug_report(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    let report = store.reports.get(&task_id).ok_or(ApiError {
        code: "report_not_ready",
        message: "report not generated".to_string(),
    })?;
    Ok(Json(
        report
            .bug_report
            .as_ref()
            .map(|x| serde_json::to_value(x).unwrap_or(serde_json::json!({})))
            .unwrap_or(serde_json::json!({"message": "no bug generated"})),
    ))
}

async fn export_report_markdown(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<String, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;
    if task.tenant_id != ctx.tenant_id {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }
    let report = store.reports.get(&task_id).ok_or(ApiError {
        code: "report_not_ready",
        message: "report not generated".to_string(),
    })?;

    let mut md = format!(
        "# 测试报告\n\n- task_id: {}\n- result: {:?}\n- summary: {}\n- issue_summary: {}\n\n## 执行步骤\n",
        report.task_id, report.result, report.summary, report.issue_summary
    );

    for step in &report.steps {
        md.push_str(&format!(
            "- [{}] {} | expected: {} | actual: {}\n",
            step.status, step.step_name, step.expected_result, step.actual_result
        ));
    }

    if let Some(bug) = &report.bug_report {
        md.push_str("\n## Bug 报告\n");
        md.push_str(&format!(
            "- title: {}\n- severity: {}\n- reason: {}\n",
            bug.bug_title, bug.severity, bug.possible_reason
        ));
    }

    Ok(md)
}

async fn task_ws(
    ws: WebSocketUpgrade,
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let ctx = request_context(&headers, &state)?;
    {
        let store = state.store.read().await;
        let task = store.tasks.get(&task_id).ok_or(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        })?;
        if task.tenant_id != ctx.tenant_id {
            return Err(ApiError {
                code: "task_not_found",
                message: format!("task {} not found", task_id),
            });
        }
    }
    Ok(ws.on_upgrade(move |socket| ws_stream(socket, task_id, state)))
}

async fn ws_stream(socket: WebSocket, task_id: Uuid, state: AppState) {
    let mut rx = state.realtime.subscribe(task_id).await;
    let (mut sender, mut receiver) = socket.split();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Close(_) = msg {
                break;
            }
        }
    });

    while let Ok(event) = rx.recv().await {
        let payload = match serde_json::to_string(&event) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if sender.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
    }
}

async fn run_task_pipeline(task_id: Uuid, state: AppState) -> Result<(), String> {
    let task_snapshot = {
        let store = state.store.read().await;
        store
            .tasks
            .get(&task_id)
            .cloned()
            .ok_or_else(|| "task not found".to_string())?
    };

    let started = Instant::now();
    for step in task_snapshot.planned_steps.clone() {
        if is_task_stopped_or_paused(task_id, &state).await {
            return Ok(());
        }

        if started.elapsed().as_millis() > task_snapshot.global_timeout_ms as u128 {
            return fail_task(task_id, "任务超时", "超过全局超时", state).await;
        }

        let mut attempt = 0;
        let mut success = false;
        while attempt <= task_snapshot.max_step_retries {
            if is_task_stopped_or_paused(task_id, &state).await {
                return Ok(());
            }

            let step_started = Instant::now();
            let observer = observer_observe(
                task_id,
                step.step_order,
                &task_snapshot.scenario,
                state.clone(),
            )
            .await;
            let action = action_decide(&step, &observer);
            let action_result =
                execute_action(task_id, step.step_order, &action, state.clone()).await;
            let verify = verifier_verify(&step, &observer, &action_result);

            append_step_log(
                task_id,
                &step,
                attempt,
                &observer,
                &action,
                &verify,
                state.clone(),
            )
            .await;

            if verify.success {
                success = true;
                break;
            }

            if verify.reason.contains("元素缺失") || verify.reason.contains("无响应") {
                let fallback = ActionDecision {
                    action_type: ActionType::Swipe,
                    action_params: serde_json::json!({"direction": "down", "distance": 0.5}),
                };
                let _ = execute_action(task_id, step.step_order, &fallback, state.clone()).await;
            }

            if step_started.elapsed().as_millis() > task_snapshot.step_timeout_ms as u128 {
                attempt += 1;
                continue;
            }

            attempt += 1;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        if !success {
            return fail_task(
                task_id,
                &format!("步骤失败: {}", step.description),
                "达到重试上限",
                state,
            )
            .await;
        }
    }

    finalize_task(task_id, TaskStatus::Passed, None, state).await;
    Ok(())
}

async fn is_task_stopped_or_paused(task_id: Uuid, state: &AppState) -> bool {
    let store = state.store.read().await;
    match store.tasks.get(&task_id) {
        Some(task) => task.status == TaskStatus::Paused || task.status == TaskStatus::Terminated,
        None => true,
    }
}

async fn fail_task(
    task_id: Uuid,
    title: &str,
    reason: &str,
    state: AppState,
) -> Result<(), String> {
    let (steps, severity) = {
        let store = state.store.read().await;
        let task = store
            .tasks
            .get(&task_id)
            .ok_or_else(|| "task not found".to_string())?;

        let sev = if task.scenario == "login" || task.scenario == "error_prompt" {
            "P1"
        } else {
            "P2"
        };

        (
            task.step_logs
                .iter()
                .map(|x| x.step_name.clone())
                .collect::<Vec<_>>(),
            sev.to_string(),
        )
    };

    finalize_task(
        task_id,
        TaskStatus::Failed,
        Some(BugReport {
            bug_title: title.to_string(),
            severity,
            reproduction_steps: steps,
            actual_result: "页面未达预期".to_string(),
            expected_result: "步骤应成功执行并进入下一状态".to_string(),
            evidence: vec![
                "latest_screenshot.jpg".to_string(),
                "latest_tree.json".to_string(),
            ],
            possible_reason: reason.to_string(),
        }),
        state,
    )
    .await;

    Ok(())
}

async fn append_step_log(
    task_id: Uuid,
    step: &PlannedStep,
    retry_count: u8,
    observer: &ObserveResult,
    action: &ActionDecision,
    verify: &VerifyResult,
    state: AppState,
) {
    let log = StepLog {
        step_order: step.step_order,
        step_name: step.description.clone(),
        action_type: action.action_type.clone(),
        action_params: action.action_params.clone(),
        expected_result: step.expected_result.clone(),
        actual_result: verify.actual_result.clone(),
        status: if verify.success {
            "success".to_string()
        } else {
            "failed".to_string()
        },
        retry_count,
        screenshot_url: Some(observer.screenshot_url.clone()),
        page_tree: observer.page_tree.clone(),
        created_at: Utc::now(),
    };

    let mut store = state.store.write().await;
    if let Some(task) = store.tasks.get_mut(&task_id) {
        task.step_logs.push(log);
        task.updated_at = Utc::now();
    }
    persist_store(&state, &store).await;
    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "step_log".to_string(),
            task_id,
            status: None,
            message: format!("step {} updated", step.step_order),
            timestamp: Utc::now(),
        })
        .await;
}

async fn finalize_task(
    task_id: Uuid,
    status: TaskStatus,
    bug_report: Option<BugReport>,
    state: AppState,
) {
    let mut report_to_push: Option<TestReport> = None;
    let mut store = state.store.write().await;
    if let Some(task) = store.tasks.get_mut(&task_id) {
        if task.status == TaskStatus::Terminated {
            return;
        }

        task.status = status.clone();
        task.updated_at = Utc::now();

        let screenshots = task
            .step_logs
            .iter()
            .filter_map(|x| x.screenshot_url.clone())
            .collect::<Vec<_>>();

        let report = TestReport {
            report_id: Uuid::new_v4(),
            task_id,
            result: status.clone(),
            summary: if status == TaskStatus::Passed {
                "测试通过，核心链路执行完成".to_string()
            } else {
                "测试失败，请查看 Bug 报告".to_string()
            },
            issue_summary: if status == TaskStatus::Passed {
                "无异常".to_string()
            } else {
                "存在失败步骤或工具调用异常".to_string()
            },
            execution_steps: task.step_logs.iter().map(|s| s.step_name.clone()).collect(),
            actual_result: if status == TaskStatus::Passed {
                "流程完成".to_string()
            } else {
                "流程中断".to_string()
            },
            expected_result: "按计划完成所有测试步骤".to_string(),
            steps: task.step_logs.clone(),
            bug_report,
            screenshots,
            created_at: Utc::now(),
        };

        store.reports.insert(task_id, report.clone());
        report_to_push = Some(report);
    }
    persist_store(&state, &store).await;
    drop(store);

    state
        .realtime
        .publish(RealtimeEvent {
            event_type: "task_status".to_string(),
            task_id,
            status: Some(status.clone()),
            message: "task finalized".to_string(),
            timestamp: Utc::now(),
        })
        .await;
    if status == TaskStatus::Failed {
        if let Some(report) = report_to_push.as_ref() {
            dispatch_failure_integrations(&state, report).await;
        }
    }
}

async fn dispatch_failure_integrations(state: &AppState, report: &TestReport) {
    let mut targets = Vec::new();
    if let Some(url) = &state.integrations.jira_webhook_url {
        targets.push(("jira", url.clone()));
    }
    if let Some(url) = &state.integrations.feishu_webhook_url {
        targets.push(("feishu", url.clone()));
    }
    if let Some(url) = &state.integrations.wecom_webhook_url {
        targets.push(("wecom", url.clone()));
    }
    if targets.is_empty() {
        return;
    }

    for (name, url) in targets {
        let payload = serde_json::json!({
            "integration": name,
            "task_id": report.task_id,
            "report_id": report.report_id,
            "summary": report.summary,
            "issue_summary": report.issue_summary,
            "bug_report": report.bug_report
        });
        if let Err(err) = reqwest::Client::new()
            .post(&url)
            .json(&payload)
            .send()
            .await
        {
            error!("push {} integration failed: {}", name, err);
        } else {
            info!(
                "push {} integration success for task {}",
                name, report.task_id
            );
        }
    }
}

#[derive(Debug, Clone)]
struct PlanResult {
    scenario: String,
    required_data: Vec<String>,
    missing_data: Vec<String>,
    steps: Vec<PlannedStep>,
}

#[derive(Debug, Clone)]
struct ObserveResult {
    current_page: String,
    page_tree: serde_json::Value,
    screenshot_url: String,
}

#[derive(Debug, Clone)]
struct ActionDecision {
    action_type: ActionType,
    action_params: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ActionResult {
    success: bool,
    output: serde_json::Value,
}

#[derive(Debug, Clone)]
struct VerifyResult {
    success: bool,
    reason: String,
    actual_result: String,
}

fn planner_plan(goal: &str, params: &serde_json::Value) -> PlanResult {
    let normalized = goal.to_lowercase();

    if normalized.contains("登录") || normalized.contains("login") {
        return scenario_plan(
            "login",
            vec!["username", "password"],
            params,
            vec![
                step(
                    1,
                    "识别当前是否在登录页",
                    ActionType::Observe,
                    serde_json::json!({}),
                    "识别到登录页",
                ),
                step(
                    2,
                    "输入账号密码",
                    ActionType::Input,
                    serde_json::json!({"fields":["username","password"]}),
                    "账号密码填充成功",
                ),
                step(
                    3,
                    "点击登录按钮",
                    ActionType::Tap,
                    serde_json::json!({"target":"登录"}),
                    "进入首页或出现明确错误提示",
                ),
            ],
        );
    }

    if normalized.contains("搜索") || normalized.contains("search") {
        return scenario_plan(
            "search",
            vec!["keyword"],
            params,
            vec![
                step(
                    1,
                    "定位搜索框",
                    ActionType::Observe,
                    serde_json::json!({}),
                    "搜索框可用",
                ),
                step(
                    2,
                    "输入关键词",
                    ActionType::Input,
                    serde_json::json!({"field":"keyword"}),
                    "关键词输入成功",
                ),
                step(
                    3,
                    "点击搜索按钮",
                    ActionType::Tap,
                    serde_json::json!({"target":"搜索"}),
                    "展示搜索结果或空状态",
                ),
            ],
        );
    }

    if normalized.contains("表单") || normalized.contains("form") {
        return scenario_plan(
            "form_submit",
            vec!["form_data"],
            params,
            vec![
                step(
                    1,
                    "进入表单页并定位必填项",
                    ActionType::Observe,
                    serde_json::json!({}),
                    "识别到必填项",
                ),
                step(
                    2,
                    "填写并提交表单",
                    ActionType::Input,
                    serde_json::json!({"field":"form_data"}),
                    "表单提交成功",
                ),
                step(
                    3,
                    "验证成功提示",
                    ActionType::Verify,
                    serde_json::json!({"contains":"提交成功"}),
                    "出现提交成功提示",
                ),
            ],
        );
    }

    if normalized.contains("筛选") || normalized.contains("filter") {
        return scenario_plan(
            "list_filter",
            vec!["filter_condition"],
            params,
            vec![
                step(
                    1,
                    "进入列表页",
                    ActionType::Observe,
                    serde_json::json!({}),
                    "列表页可见",
                ),
                step(
                    2,
                    "打开筛选并选择条件",
                    ActionType::Tap,
                    serde_json::json!({"target":"筛选"}),
                    "筛选条件已选择",
                ),
                step(
                    3,
                    "点击确认并验证列表刷新",
                    ActionType::Verify,
                    serde_json::json!({"contains":"filtered"}),
                    "结果符合筛选条件",
                ),
            ],
        );
    }

    if normalized.contains("异常提示") || normalized.contains("错误提示") {
        return scenario_plan(
            "error_prompt",
            vec!["username", "password"],
            params,
            vec![
                step(
                    1,
                    "输入错误账号密码",
                    ActionType::Input,
                    serde_json::json!({"invalid":true}),
                    "错误数据输入成功",
                ),
                step(
                    2,
                    "点击登录并检查错误提示",
                    ActionType::Verify,
                    serde_json::json!({"contains":"错误"}),
                    "出现明确错误提示",
                ),
            ],
        );
    }

    scenario_plan(
        "generic",
        vec![],
        params,
        vec![step(
            1,
            "执行通用页面可交互性检查",
            ActionType::Observe,
            serde_json::json!({}),
            "页面可正常交互",
        )],
    )
}

fn step(
    order: u32,
    description: &str,
    action_type: ActionType,
    action_params: serde_json::Value,
    expected: &str,
) -> PlannedStep {
    PlannedStep {
        step_order: order,
        description: description.to_string(),
        action_type,
        action_params,
        expected_result: expected.to_string(),
    }
}

fn scenario_plan(
    scenario: &str,
    required: Vec<&str>,
    params: &serde_json::Value,
    steps: Vec<PlannedStep>,
) -> PlanResult {
    let required_data = required.into_iter().map(String::from).collect::<Vec<_>>();
    let missing_data = required_data
        .iter()
        .filter(|k| params.get(k.as_str()).is_none())
        .cloned()
        .collect::<Vec<_>>();

    PlanResult {
        scenario: scenario.to_string(),
        required_data,
        missing_data,
        steps,
    }
}

async fn observer_observe(
    task_id: Uuid,
    step_order: u32,
    scenario: &str,
    state: AppState,
) -> ObserveResult {
    if let Some(base_url) = &state.driver_base_url {
        let url = format!("{}/observe", base_url.trim_end_matches('/'));
        let payload = serde_json::json!({
            "task_id": task_id,
            "step_order": step_order,
            "scenario": scenario
        });
        if let Ok(resp) = reqwest::Client::new().post(url).json(&payload).send().await {
            if let Ok(value) = resp.json::<serde_json::Value>().await {
                let current_page = value
                    .get("current_page")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown_page")
                    .to_string();
                let screenshot_url = value
                    .get("screenshot_url")
                    .and_then(|x| x.as_str())
                    .unwrap_or("driver://missing_screenshot")
                    .to_string();
                let page_tree = value
                    .get("page_tree")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));
                let snapshot = PageSnapshot {
                    id: Uuid::new_v4(),
                    task_id,
                    step_order,
                    screenshot_url: screenshot_url.clone(),
                    page_tree: page_tree.clone(),
                    current_page: current_page.clone(),
                    created_at: Utc::now(),
                };
                let mut store = state.store.write().await;
                store.snapshots.entry(task_id).or_default().push(snapshot);
                persist_store(&state, &store).await;
                return ObserveResult {
                    current_page,
                    page_tree,
                    screenshot_url,
                };
            }
        }
    }

    let current_page = match scenario {
        "login" => "login_page",
        "search" => "search_page",
        "form_submit" => "form_page",
        "list_filter" => "list_page",
        "error_prompt" => "login_page",
        _ => "generic_page",
    }
    .to_string();

    let screenshot_url = format!("s3://mock/{}/step_{}.jpg", task_id, step_order);
    let page_tree = serde_json::json!({
        "current_page": current_page,
        "elements": [
            {"type":"input","name":"账号输入框","clickable":true},
            {"type":"input","name":"密码输入框","clickable":true},
            {"type":"input","name":"搜索框","clickable":true},
            {"type":"button","name":"登录","clickable":true},
            {"type":"button","name":"搜索","clickable":true},
            {"type":"button","name":"筛选","clickable":true},
            {"type":"button","name":"提交","clickable":true}
        ],
        "status":"ready"
    });

    let snapshot = PageSnapshot {
        id: Uuid::new_v4(),
        task_id,
        step_order,
        screenshot_url: screenshot_url.clone(),
        page_tree: page_tree.clone(),
        current_page: current_page.clone(),
        created_at: Utc::now(),
    };

    let mut store = state.store.write().await;
    store.snapshots.entry(task_id).or_default().push(snapshot);
    persist_store(&state, &store).await;

    ObserveResult {
        current_page,
        page_tree,
        screenshot_url,
    }
}

fn action_decide(step: &PlannedStep, observe: &ObserveResult) -> ActionDecision {
    if step.action_type == ActionType::Observe {
        return ActionDecision {
            action_type: ActionType::Observe,
            action_params: serde_json::json!({"current_page": observe.current_page}),
        };
    }

    if step.action_type == ActionType::Verify {
        return ActionDecision {
            action_type: ActionType::AgentAct,
            action_params: serde_json::json!({"instruction": step.description}),
        };
    }

    ActionDecision {
        action_type: step.action_type.clone(),
        action_params: step.action_params.clone(),
    }
}

async fn execute_action(
    task_id: Uuid,
    step_order: u32,
    action: &ActionDecision,
    state: AppState,
) -> ActionResult {
    let start = Instant::now();
    let (tool_name, mut output) = match action.action_type {
        ActionType::Observe => ("tree", serde_json::json!({"status":"ok"})),
        ActionType::Tap => ("tap", serde_json::json!({"status":"ok"})),
        ActionType::Input => ("input", serde_json::json!({"status":"ok"})),
        ActionType::Swipe => ("swipe", serde_json::json!({"status":"ok"})),
        ActionType::Verify => ("verify", serde_json::json!({"status":"ok"})),
        ActionType::AgentAct => ("agent_act", serde_json::json!({"status":"ok"})),
    };
    let mut success = true;
    if let Some(base_url) = &state.driver_base_url {
        let url = format!("{}/action", base_url.trim_end_matches('/'));
        let payload = serde_json::json!({
            "task_id": task_id,
            "step_order": step_order,
            "tool": tool_name,
            "params": action.action_params
        });
        if let Ok(resp) = reqwest::Client::new().post(url).json(&payload).send().await {
            if let Ok(driver_output) = resp.json::<serde_json::Value>().await {
                success = driver_output
                    .get("success")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(true);
                output = driver_output;
            }
        }
    }

    let log = ToolCallLog {
        id: Uuid::new_v4(),
        task_id,
        step_order,
        tool_name: tool_name.to_string(),
        request_payload: action.action_params.clone(),
        response_payload: output.clone(),
        success,
        latency_ms: start.elapsed().as_millis(),
        created_at: Utc::now(),
    };

    let mut store = state.store.write().await;
    store.tool_calls.entry(task_id).or_default().push(log);
    persist_store(&state, &store).await;

    ActionResult { success, output }
}

fn verifier_verify(
    step: &PlannedStep,
    observe: &ObserveResult,
    action_result: &ActionResult,
) -> VerifyResult {
    if !action_result.success {
        return VerifyResult {
            success: false,
            reason: "动作执行失败".to_string(),
            actual_result: "工具调用失败".to_string(),
        };
    }

    let elements = observe
        .page_tree
        .get("elements")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    if step.description.contains("登录") {
        let has_login = elements.iter().any(|e| {
            e.get("name")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .contains("登录")
        });
        if !has_login {
            return VerifyResult {
                success: false,
                reason: "元素缺失: 未找到登录按钮".to_string(),
                actual_result: "页面结构异常".to_string(),
            };
        }
    }

    VerifyResult {
        success: true,
        reason: "验证通过".to_string(),
        actual_result: format!("{}: 执行成功", step.description),
    }
}

fn mask_sensitive_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if k.contains("password") || k.contains("token") || k.contains("secret") {
                    out.insert(k.clone(), serde_json::json!("***"));
                } else {
                    out.insert(k.clone(), mask_sensitive_json(v));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(mask_sensitive_json).collect())
        }
        _ => value.clone(),
    }
}

fn merge_json(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Object(mut a_map), serde_json::Value::Object(b_map)) => {
            for (k, v) in b_map {
                let old = a_map.remove(&k).unwrap_or(serde_json::Value::Null);
                a_map.insert(k, merge_json(old, v));
            }
            serde_json::Value::Object(a_map)
        }
        (_, b_other) => b_other,
    }
}

async fn persist_store(state: &AppState, store: &Store) {
    if let Err(err) = state.backend.save(store.clone()).await {
        error!("persist store failed: {}", err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_should_cover_list_filter() {
        let p = planner_plan("测试列表筛选功能", &serde_json::json!({}));
        assert_eq!(p.scenario, "list_filter");
        assert_eq!(p.missing_data, vec!["filter_condition"]);
    }

    #[test]
    fn planner_should_cover_error_prompt() {
        let p = planner_plan(
            "测试错误提示",
            &serde_json::json!({"username":"a","password":"b"}),
        );
        assert_eq!(p.scenario, "error_prompt");
        assert!(p.missing_data.is_empty());
    }

    #[test]
    fn mask_sensitive_should_work() {
        let raw = serde_json::json!({"username":"u","password":"abc"});
        let masked = mask_sensitive_json(&raw);
        assert_eq!(masked.get("password").unwrap(), "***");
    }
}
