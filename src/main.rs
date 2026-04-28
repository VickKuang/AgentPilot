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
    extract::{Path as AxumPath, State},
    http::{HeaderMap, Method, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, patch, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_postgres::NoTls;
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    types::Json,
    Row,
};
use tokio::sync::RwLock;
use tower_http::services::ServeDir;
use tracing::{error, info};
use uuid::Uuid;

const STORE_FILE: &str = "data/store.json";
const DEFAULT_API_KEY: &str = "dev-admin-key";
const DEFAULT_TENANT: &str = "public";
const DEFAULT_RATE_LIMIT_PER_MINUTE: u32 = 120;

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<Store>>,
    backend: Arc<dyn StoreBackend>,
    realtime: Arc<RealtimeHub>,
    api_key: Option<String>,
    driver_base_url: Option<String>,
    integrations: IntegrationConfig,
    security: Arc<SecurityState>,
    rate_limiter: Arc<RwLock<HashMap<String, RateLimitWindow>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum UserRole {
    Admin,
    Operator,
    Viewer,
}

#[derive(Debug, Clone)]
struct ApiCredential {
    key_id: String,
    api_key: String,
    role: UserRole,
    tenant_scope: Option<String>,
    user_id: String,
    kms_key_ref: Option<String>,
}

#[derive(Debug, Clone)]
struct SecurityState {
    credentials: Vec<ApiCredential>,
    rate_limit_per_minute: u32,
    kms_provider: String,
}

#[derive(Debug, Clone)]
struct RateLimitWindow {
    count: u32,
    window_started_at: Instant,
}

#[derive(Debug, Clone)]
struct AuthContext {
    key_id: String,
    user_id: String,
    tenant_id: String,
    role: UserRole,
    kms_key_ref: Option<String>,
    persistence: PersistenceBackend,
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct Store {
    tasks: HashMap<Uuid, TestTask>,
    reports: HashMap<Uuid, TestReport>,
    tool_calls: HashMap<Uuid, Vec<ToolCallLog>>,
    snapshots: HashMap<Uuid, Vec<PageSnapshot>>,
    #[serde(default)]
    audit_logs: Vec<AuditLog>,
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
#[derive(Clone)]
enum PersistenceBackend {
    Json,
    Postgres(PostgresPersistence),
}

#[derive(Clone)]
struct PostgresPersistence {
    pool: PgPool,
    version: Arc<RwLock<i64>>,
}

impl PersistenceBackend {
    async fn from_env() -> Result<(Self, Store), String> {
        let database_url = std::env::var("DATABASE_URL").ok();
        if let Some(url) = database_url {
            let (pg, store) = PostgresPersistence::connect(&url).await?;
            info!("using PostgreSQL persistence");
            return Ok((Self::Postgres(pg), store));
        }

        info!("using local JSON persistence");
        Ok((Self::Json, Store::load()))
    }

    async fn save_store(&self, store: &Store) -> Result<(), String> {
        match self {
            Self::Json => store.save(),
            Self::Postgres(pg) => pg.save(store).await,
        }
    }
}

impl PostgresPersistence {
    async fn connect(database_url: &str) -> Result<(Self, Store), String> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| format!("connect postgres failed: {}", e))?;

        sqlx::query(
            r#"
            create table if not exists app_store_state (
              id smallint primary key check (id = 1),
              data jsonb not null,
              version bigint not null default 0,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| format!("create app_store_state failed: {}", e))?;

        let row = sqlx::query("select data, version from app_store_state where id = 1")
            .fetch_optional(&pool)
            .await
            .map_err(|e| format!("query app_store_state failed: {}", e))?;

        if let Some(row) = row {
            let Json(data): Json<serde_json::Value> = row.get("data");
            let version: i64 = row.get("version");
            let store = serde_json::from_value::<Store>(data).unwrap_or_default();
            Ok((
                Self {
                    pool,
                    version: Arc::new(RwLock::new(version)),
                },
                store,
            ))
        } else {
            let store = Store::default();
            let data = serde_json::to_value(&store)
                .map_err(|e| format!("serialize default store failed: {}", e))?;
            sqlx::query("insert into app_store_state(id, data, version) values (1, $1, 0)")
                .bind(Json(data))
                .execute(&pool)
                .await
                .map_err(|e| format!("init app_store_state failed: {}", e))?;

            Ok((
                Self {
                    pool,
                    version: Arc::new(RwLock::new(0)),
                },
                store,
            ))
        }
    }

    async fn save(&self, store: &Store) -> Result<(), String> {
        let data =
            serde_json::to_value(store).map_err(|e| format!("serialize store failed: {}", e))?;

        for _ in 0..3 {
            let expected = *self.version.read().await;
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| format!("begin tx failed: {}", e))?;

            let affected = sqlx::query(
                "update app_store_state set data = $1, version = version + 1, updated_at = now() where id = 1 and version = $2",
            )
            .bind(Json(data.clone()))
            .bind(expected)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("update app_store_state failed: {}", e))?
            .rows_affected();

            if affected == 1 {
                tx.commit()
                    .await
                    .map_err(|e| format!("commit tx failed: {}", e))?;
                *self.version.write().await = expected + 1;
                return Ok(());
            }

            tx.rollback()
                .await
                .map_err(|e| format!("rollback tx failed: {}", e))?;

            let latest = sqlx::query("select version from app_store_state where id = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("refresh version failed: {}", e))?;
            let latest_version: i64 = latest.get("version");
            *self.version.write().await = latest_version;
        }

        Err("save store failed after conflict retries".to_string())
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
    #[serde(default = "default_tenant")]
    tenant_id: String,
    #[serde(default)]
    created_by: String,
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
    #[serde(default = "default_next_step_order")]
    next_step_order: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlannedStep {
    step_order: u32,
    description: String,
    action_type: ActionType,
    action_params: serde_json::Value,
    expected_result: String,
    #[serde(default)]
    verify_rules: Vec<VerifyRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum VerifyRule {
    ElementExists { name: String },
    TextContains { value: String },
    CurrentPageIs { value: String },
}

fn default_next_step_order() -> u32 {
    1
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
    #[serde(default = "default_tenant")]
    tenant_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuditLog {
    id: Uuid,
    tenant_id: String,
    user_id: String,
    role: UserRole,
    action: String,
    resource: String,
    success: bool,
    detail: serde_json::Value,
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

#[derive(Debug, Serialize, Deserialize)]
struct CreateTaskResponse {
    task_id: Uuid,
    scenario: String,
    status: TaskStatus,
    required_data: Vec<String>,
    missing_data: Vec<String>,
    planned_steps: Vec<PlannedStep>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TaskProgress {
    task_id: Uuid,
    status: TaskStatus,
    total_steps: usize,
    done_steps: usize,
    success_steps: usize,
    failed_steps: usize,
    progress_percent: u8,
}

#[derive(Debug, Deserialize)]
struct ListTasksQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    status: Option<String>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListTasksResponse {
    items: Vec<TestTask>,
    page: usize,
    page_size: usize,
    total: usize,
    total_pages: usize,
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: &'static str,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ExportReportQuery {
    format: Option<String>,
    template: Option<String>,
struct StepLogQuery {
    step_order: Option<u32>,
    status: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ToolCallQuery {
    step_order: Option<u32>,
    success: Option<bool>,
    tool_name: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    step_order: Option<u32>,
    current_page: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct TaskFailureAggregation {
    task_id: Uuid,
    failed_steps: usize,
    failed_step_orders: Vec<u32>,
    failed_step_names: Vec<String>,
    failed_tools: Vec<String>,
    latest_failed_at: Option<DateTime<Utc>>,
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
fn default_tenant() -> String {
    DEFAULT_TENANT.to_string()
}

impl SecurityState {
    fn load() -> Self {
        #[derive(Deserialize)]
        struct RawCredential {
            key_id: String,
            api_key: String,
            role: UserRole,
            tenant_scope: Option<String>,
            user_id: Option<String>,
            kms_key_ref: Option<String>,
        }

        let credentials = env::var("AUTOTEST_API_CREDENTIALS")
            .ok()
            .and_then(|raw| serde_json::from_str::<Vec<RawCredential>>(&raw).ok())
            .map(|rows| {
                rows.into_iter()
                    .map(|row| ApiCredential {
                        key_id: row.key_id.clone(),
                        api_key: row.api_key,
                        role: row.role,
                        tenant_scope: row.tenant_scope,
                        user_id: row.user_id.unwrap_or(row.key_id),
                        kms_key_ref: row.kms_key_ref,
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|x| !x.is_empty())
            .unwrap_or_else(|| {
                vec![ApiCredential {
                    key_id: "dev-admin".to_string(),
                    api_key: DEFAULT_API_KEY.to_string(),
                    role: UserRole::Admin,
                    tenant_scope: None,
                    user_id: "local_admin".to_string(),
                    kms_key_ref: Some("local-kms/dev-main-key".to_string()),
                }]
            });

        let rate_limit_per_minute = env::var("AUTOTEST_RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|x| x.parse::<u32>().ok())
            .unwrap_or(DEFAULT_RATE_LIMIT_PER_MINUTE);
        let kms_provider =
            env::var("AUTOTEST_KMS_PROVIDER").unwrap_or_else(|_| "local-kms".to_string());

        Self {
            credentials,
            rate_limit_per_minute,
            kms_provider,
        }
    }
}

fn role_allowed(role: &UserRole, method: &Method) -> bool {
    match role {
        UserRole::Admin => true,
        UserRole::Operator => method != Method::DELETE,
        UserRole::Viewer => method == Method::GET,
    }
}

fn ensure_tenant_access(resource_tenant: &str, actor: &AuthContext) -> Result<(), ApiError> {
    if resource_tenant == actor.tenant_id {
        Ok(())
    } else {
        Err(ApiError {
            code: "tenant_forbidden",
            message: "resource does not belong to current tenant".to_string(),
        })
    }
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
    let security = Arc::new(SecurityState::load());
    let state = AppState {
        store: Arc::new(RwLock::new(Store::load())),
        security,
        rate_limiter: Arc::new(RwLock::new(HashMap::new())),
    let (persistence, initial_store) = PersistenceBackend::from_env().await.unwrap_or_else(|err| {
        error!("init persistence failed, fallback to JSON: {}", err);
        (PersistenceBackend::Json, Store::load())
    });

    let state = AppState {
        store: Arc::new(RwLock::new(initial_store)),
        persistence,
    };

    let app = build_app(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("autotest-agent listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
    let protected_api = Router::new()
        .route("/api/v1/tasks", post(create_task).get(list_tasks))
        .route("/api/v1/tasks/:task_id", get(get_task))
        .route("/api/v1/tasks/:task_id/data", patch(update_task_data))
        .route("/api/v1/tasks/:task_id/start", post(start_task))
        .route("/api/v1/tasks/:task_id/retry", post(retry_task))
        .route("/api/v1/tasks/:task_id/pause", post(pause_task))
        .route("/api/v1/tasks/:task_id/resume", post(resume_task))
        .route("/api/v1/tasks/:task_id/terminate", post(terminate_task))
        .route("/api/v1/tasks/:task_id/progress", get(get_progress))
        .route("/api/v1/tasks/:task_id/ws", get(task_ws))
        .route("/api/v1/tasks/:task_id/logs", get(get_logs))
        .route(
            "/api/v1/tasks/:task_id/logs/failures",
            get(get_failure_aggregation),
        )
        .route("/api/v1/tasks/:task_id/tool-calls", get(get_tool_calls))
        .route("/api/v1/tasks/:task_id/snapshots", get(get_snapshots))
        .route("/api/v1/tasks/:task_id/ws", get(task_ws))
        .route("/api/v1/tasks/:task_id/report", get(get_report))
        .route("/api/v1/tasks/:task_id/bug-report", get(get_bug_report))
        .route(
            "/api/v1/tasks/:task_id/report/export",
            get(export_report_markdown),
        )
        .route("/api/v1/audit-logs", get(get_audit_logs))
        .route("/api/v1/security/context", get(get_security_context))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .route("/health", get(health))
        .merge(protected_api)
        .nest_service("/", ServeDir::new("web"))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok", "service": "autotest-agent"}))
}

async fn auth_middleware(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let api_key = headers
        .get("x-api-key")
        .and_then(|x| x.to_str().ok())
        .unwrap_or_default();

    let credential = match state
        .security
        .credentials
        .iter()
        .find(|cred| cred.api_key == api_key)
        .cloned()
    {
        Some(cred) => cred,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"code":"unauthorized","message":"invalid api key"})),
            )
                .into_response();
        }
    };

    let requested_tenant = headers
        .get("x-tenant-id")
        .and_then(|x| x.to_str().ok())
        .unwrap_or(DEFAULT_TENANT)
        .to_string();

    if let Some(scope) = &credential.tenant_scope {
        if scope != &requested_tenant {
            return (
                StatusCode::FORBIDDEN,
                Json(
                    serde_json::json!({"code":"tenant_forbidden","message":"tenant scope denied"}),
                ),
            )
                .into_response();
        }
    }

    let method = request.method().clone();
    if !role_allowed(&credential.role, &method) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code":"rbac_forbidden","message":"insufficient role permission"})),
        )
            .into_response();
    }

    let rate_key = format!("{}:{}:{}", credential.key_id, requested_tenant, method);
    {
        let mut windows = state.rate_limiter.write().await;
        let now = Instant::now();
        let window = windows.entry(rate_key).or_insert(RateLimitWindow {
            count: 0,
            window_started_at: now,
        });
        if now.duration_since(window.window_started_at) >= Duration::from_secs(60) {
            window.count = 0;
            window.window_started_at = now;
        }
        if window.count >= state.security.rate_limit_per_minute {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({"code":"rate_limit_exceeded","message":"rate limit exceeded"})),
            )
                .into_response();
        }
        window.count += 1;
    }

    let actor = AuthContext {
        key_id: credential.key_id,
        user_id: headers
            .get("x-user-id")
            .and_then(|x| x.to_str().ok())
            .unwrap_or(&credential.user_id)
            .to_string(),
        tenant_id: requested_tenant,
        role: credential.role,
        kms_key_ref: credential.kms_key_ref,
    };

    request.extensions_mut().insert(actor);
    next.run(request).await
}

async fn create_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<CreateTaskResponse>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    let params = req.params.unwrap_or_else(|| serde_json::json!({}));
    let plan = planner_plan(&req.user_goal, &params);

    let task = TestTask {
        task_id: Uuid::new_v4(),
        tenant_id: ctx.tenant_id,
        tenant_id: actor.tenant_id.clone(),
        created_by: actor.user_id.clone(),
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
        next_step_order: 1,
    };

    let task_id = task.task_id;
    let status = task.status.clone();

    let mut store = state.store.write().await;
    store.tasks.insert(task_id, task);
    store.tool_calls.insert(task_id, vec![]);
    store.snapshots.insert(task_id, vec![]);
    append_audit_log(
        &mut store,
        &actor,
        "task.create",
        &format!("task/{}", task_id),
        true,
        serde_json::json!({}),
    );
    persist_store(&store);
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
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;

    let merged = merge_json(task.params.clone(), req.params.clone());
    task.params = mask_sensitive_json(&merged);

    task.missing_data = task
        .required_data
        .iter()
        .filter(|key| merged.get(key.as_str()).is_none())
        .cloned()
        .collect();

    let new_status = if task.missing_data.is_empty() {
        TaskStatus::Pending
    } else {
        TaskStatus::Blocked
    };
    transition_task_status(task, new_status, "update_task_data")?;
    task.updated_at = Utc::now();

    let out = task.clone();
    append_audit_log(
        &mut store,
        &actor,
        "task.update_data",
        &format!("task/{}", task_id),
        true,
        serde_json::json!({"missing_data": out.missing_data}),
    );
    persist_store(&store);
    persist_store(&state, &store).await;
    Ok(Json(out))
}

async fn list_tasks(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<TestTask>>, ApiError> {
    let ctx = request_context(&headers, &state)?;
    Ok(Json(
    Extension(actor): Extension<AuthContext>,
) -> Json<Vec<TestTask>> {
    Json(
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
            .filter(|t| t.tenant_id == actor.tenant_id)
            .cloned()
            .collect(),
    )
    Query(query): Query<ListTasksQuery>,
) -> Result<Json<ListTasksResponse>, ApiError> {
    let store = state.store.read().await;
    let mut tasks = store.tasks.values().cloned().collect::<Vec<_>>();

    if let Some(raw_status) = query.status {
        let status = parse_task_status(&raw_status)?;
        tasks.retain(|t| t.status == status);
    }

    let sort_by = query.sort_by.unwrap_or_else(|| "created_at".to_string());
    let sort_order = query.sort_order.unwrap_or_else(|| "desc".to_string());
    sort_tasks(&mut tasks, &sort_by, &sort_order)?;

    let page_size = query.page_size.unwrap_or(20).clamp(1, 100);
    let page = query.page.unwrap_or(1).max(1);
    let total = tasks.len();
    let total_pages = if total == 0 {
        0
    } else {
        total.div_ceil(page_size)
    };
    let start = (page - 1) * page_size;
    let items = if start >= total {
        vec![]
    } else {
        let end = (start + page_size).min(total);
        tasks[start..end].to_vec()
    };

    Ok(Json(ListTasksResponse {
        items,
        page,
        page_size,
        total,
        total_pages,
    }))
}

async fn get_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    Ok(Json(task))
}

async fn start_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
        ensure_tenant_access(&task.tenant_id, &actor)?;

        if !task.missing_data.is_empty() {
            return Err(ApiError {
                code: "missing_required_data",
                message: format!("缺少参数: {}", task.missing_data.join(", ")),
            });
        }

        transition_task_status(task, TaskStatus::Running, "start_task")?;
        task.updated_at = Utc::now();
        task.step_logs.clear();
        task.next_step_order = 1;
        store.reports.remove(&task_id);
        store.tool_calls.entry(task_id).or_default().clear();
        store.snapshots.entry(task_id).or_default().clear();
        append_audit_log(
            &mut store,
            &actor,
            "task.start",
            &format!("task/{}", task_id),
            true,
            serde_json::json!({}),
        );
        persist_store(&store);
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
    Extension(actor): Extension<AuthContext>,
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
        ensure_tenant_access(&task.tenant_id, &actor)?;

        if task.retries >= task.max_retries {
            return Err(ApiError {
                code: "retry_exhausted",
                message: "max retries exceeded".to_string(),
            });
        }

        task.retries += 1;
        transition_task_status(task, TaskStatus::Running, "retry_task")?;
        task.step_logs.clear();
        task.next_step_order = 1;
        task.updated_at = Utc::now();
        store.reports.remove(&task_id);
        append_audit_log(
            &mut store,
            &actor,
            "task.retry",
            &format!("task/{}", task_id),
            true,
            serde_json::json!({"retry_count": task.retries}),
        );
        persist_store(&store);
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
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    task.status = TaskStatus::Paused;
    task.updated_at = Utc::now();
    append_audit_log(
        &mut store,
        &actor,
        "task.pause",
        &format!("task/{}", task_id),
        true,
        serde_json::json!({}),
    );
    persist_store(&store);
    transition_task_status(task, TaskStatus::Paused, "pause_task")?;
    task.updated_at = Utc::now();
    persist_store(&state, &store).await;
    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "paused"}),
    ))
}

async fn resume_task(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
        ensure_tenant_access(&task.tenant_id, &actor)?;
        if task.status != TaskStatus::Paused {
            return Err(ApiError {
                code: "not_paused",
                message: "task is not paused".to_string(),
            });
        }
        task.status = TaskStatus::Running;
        task.updated_at = Utc::now();
        append_audit_log(
            &mut store,
            &actor,
            "task.resume",
            &format!("task/{}", task_id),
            true,
            serde_json::json!({}),
        );
        persist_store(&store);
        transition_task_status(task, TaskStatus::Running, "resume_task")?;
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
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;

    transition_task_status(task, TaskStatus::Terminated, "terminate_task")?;
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
    append_audit_log(
        &mut store,
        &actor,
        "task.terminate",
        &format!("task/{}", task_id),
        true,
        serde_json::json!({}),
    );
    persist_store(&store);
    persist_store(&state, &store).await;
    Ok(Json(
        serde_json::json!({"task_id": task_id, "status": "terminated"}),
    ))
}

async fn get_progress(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    Ok(Json(build_progress(task_id, task)))
}

async fn task_ws(
    ws: WebSocketUpgrade,
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.store.read().await.tasks.contains_key(&task_id) {
        return Err(ApiError {
            code: "task_not_found",
            message: format!("task {} not found", task_id),
        });
    }

    Ok(ws.on_upgrade(move |socket| task_ws_loop(socket, task_id, state)))
}

async fn task_ws_loop(mut socket: WebSocket, task_id: Uuid, state: AppState) {
    loop {
        let payload = {
            let store = state.store.read().await;
            let Some(task) = store.tasks.get(&task_id) else {
                break;
            };

            serde_json::json!({
                "task": task,
                "logs": task.step_logs,
                "tool_calls": store.tool_calls.get(&task_id).cloned().unwrap_or_default(),
                "progress": build_progress(task_id, task),
            })
        };

        if socket
            .send(Message::Text(payload.to_string()))
            .await
            .is_err()
        {
            break;
        }

        let status = {
            let store = state.store.read().await;
            store.tasks.get(&task_id).map(|t| t.status.clone())
        };

        if matches!(
            status,
            Some(TaskStatus::Passed | TaskStatus::Failed | TaskStatus::Terminated)
        ) {
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn get_logs(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
    Query(query): Query<StepLogQuery>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    Ok(Json(task.step_logs.clone()))

    let started_at = parse_optional_datetime(query.started_at.as_deref(), "started_at")?;
    let ended_at = parse_optional_datetime(query.ended_at.as_deref(), "ended_at")?;
    validate_time_range(started_at, ended_at)?;

    let status = query.status.as_deref();
    let logs = task
        .step_logs
        .iter()
        .filter(|x| query.step_order.is_none_or(|order| x.step_order == order))
        .filter(|x| status.is_none_or(|status| x.status.eq_ignore_ascii_case(status)))
        .filter(|x| started_at.is_none_or(|start| x.created_at >= start))
        .filter(|x| ended_at.is_none_or(|end| x.created_at <= end))
        .cloned()
        .collect();

    Ok(Json(logs))
}

async fn get_failure_aggregation(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<TaskFailureAggregation>, ApiError> {
    let store = state.store.read().await;
    let task = store.tasks.get(&task_id).ok_or(ApiError {
        code: "task_not_found",
        message: format!("task {} not found", task_id),
    })?;

    let failed_logs = task
        .step_logs
        .iter()
        .filter(|x| x.status.eq_ignore_ascii_case("failed"))
        .collect::<Vec<_>>();

    let failed_step_orders = failed_logs.iter().map(|x| x.step_order).collect::<Vec<_>>();
    let failed_step_names = failed_logs
        .iter()
        .map(|x| x.step_name.clone())
        .collect::<Vec<_>>();
    let latest_failed_at = failed_logs.iter().map(|x| x.created_at).max();

    let failed_tools = store
        .tool_calls
        .get(&task_id)
        .into_iter()
        .flatten()
        .filter(|x| !x.success)
        .map(|x| x.tool_name.clone())
        .collect::<Vec<_>>();

    Ok(Json(TaskFailureAggregation {
        task_id,
        failed_steps: failed_logs.len(),
        failed_step_orders,
        failed_step_names,
        failed_tools,
        latest_failed_at,
    }))
}

async fn get_tool_calls(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    Ok(Json(
        store.tool_calls.get(&task_id).cloned().unwrap_or_default(),
    ))
    Query(query): Query<ToolCallQuery>,
) -> Result<Json<Vec<ToolCallLog>>, ApiError> {
    let store = state.store.read().await;
    let started_at = parse_optional_datetime(query.started_at.as_deref(), "started_at")?;
    let ended_at = parse_optional_datetime(query.ended_at.as_deref(), "ended_at")?;
    validate_time_range(started_at, ended_at)?;

    let tool_name = query.tool_name.as_deref();
    let tool_calls = store
        .tool_calls
        .get(&task_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|x| query.step_order.is_none_or(|order| x.step_order == order))
        .filter(|x| query.success.is_none_or(|success| x.success == success))
        .filter(|x| tool_name.is_none_or(|name| x.tool_name.eq_ignore_ascii_case(name)))
        .filter(|x| started_at.is_none_or(|start| x.created_at >= start))
        .filter(|x| ended_at.is_none_or(|end| x.created_at <= end))
        .collect::<Vec<_>>();

    Ok(Json(tool_calls))
}

async fn get_snapshots(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&task.tenant_id, &actor)?;
    Ok(Json(
        store.snapshots.get(&task_id).cloned().unwrap_or_default(),
    ))
    Query(query): Query<SnapshotQuery>,
) -> Result<Json<Vec<PageSnapshot>>, ApiError> {
    let store = state.store.read().await;
    let started_at = parse_optional_datetime(query.started_at.as_deref(), "started_at")?;
    let ended_at = parse_optional_datetime(query.ended_at.as_deref(), "ended_at")?;
    validate_time_range(started_at, ended_at)?;

    let current_page = query.current_page.as_deref();
    let snapshots = store
        .snapshots
        .get(&task_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|x| query.step_order.is_none_or(|order| x.step_order == order))
        .filter(|x| current_page.is_none_or(|page| x.current_page.eq_ignore_ascii_case(page)))
        .filter(|x| started_at.is_none_or(|start| x.created_at >= start))
        .filter(|x| ended_at.is_none_or(|end| x.created_at <= end))
        .collect::<Vec<_>>();

    Ok(Json(snapshots))
}

async fn get_report(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&report.tenant_id, &actor)?;
    Ok(Json(report))
}

async fn get_bug_report(
    AxumPath(task_id): AxumPath<Uuid>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(actor): Extension<AuthContext>,
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
    ensure_tenant_access(&report.tenant_id, &actor)?;
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
    Extension(actor): Extension<AuthContext>,
) -> Result<String, ApiError> {
    axum::extract::Query(query): axum::extract::Query<ExportReportQuery>,
) -> Result<Response, ApiError> {
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
    ensure_tenant_access(&report.tenant_id, &actor)?;

    let format = query.format.unwrap_or_else(|| "markdown".to_string());
    let markdown = render_report_markdown(report, query.template.as_deref());

    match format.as_str() {
        "markdown" | "md" => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/markdown; charset=utf-8"),
            );
            Ok((headers, markdown).into_response())
        }
        "html" => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            Ok((headers, markdown_to_html(&markdown)).into_response())
        }
        "pdf" => {
            let pdf_bytes = render_report_pdf(report, &markdown).map_err(|e| ApiError {
                code: "pdf_export_failed",
                message: e,
            })?;
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/pdf"),
            );
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("attachment; filename=\"report.pdf\""),
            );
            Ok((headers, pdf_bytes).into_response())
        }
        _ => Err(ApiError {
            code: "unsupported_format",
            message: "format only supports: markdown|html|pdf".to_string(),
        }),
    }
}

#[derive(Debug, Serialize)]
struct SecurityContextResponse {
    user_id: String,
    tenant_id: String,
    role: UserRole,
    key_id: String,
    kms_provider: String,
    kms_key_ref: Option<String>,
    rate_limit_per_minute: u32,
}

async fn get_security_context(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthContext>,
) -> Json<SecurityContextResponse> {
    Json(SecurityContextResponse {
        user_id: actor.user_id,
        tenant_id: actor.tenant_id,
        role: actor.role,
        key_id: actor.key_id,
        kms_provider: state.security.kms_provider.clone(),
        kms_key_ref: actor.kms_key_ref,
        rate_limit_per_minute: state.security.rate_limit_per_minute,
    })
}

async fn get_audit_logs(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthContext>,
) -> Result<Json<Vec<AuditLog>>, ApiError> {
    let store = state.store.read().await;
    let logs = store
        .audit_logs
        .iter()
        .filter(|log| {
            log.tenant_id == actor.tenant_id
                || (actor.role == UserRole::Admin && actor.tenant_id == DEFAULT_TENANT)
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok(Json(logs))
}

fn append_audit_log(
    store: &mut Store,
    actor: &AuthContext,
    action: &str,
    resource: &str,
    success: bool,
    detail: serde_json::Value,
) {
    store.audit_logs.push(AuditLog {
        id: Uuid::new_v4(),
        tenant_id: actor.tenant_id.clone(),
        user_id: actor.user_id.clone(),
        role: actor.role.clone(),
        action: action.to_string(),
        resource: resource.to_string(),
        success,
        detail,
        created_at: Utc::now(),
    });
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
    let pending_steps = task_snapshot
        .planned_steps
        .clone()
        .into_iter()
        .filter(|s| s.step_order >= task_snapshot.next_step_order)
        .collect::<Vec<_>>();

    for step in pending_steps {
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
                mark_step_success(task_id, step.step_order, state.clone()).await;
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
        let sev = evaluate_bug_severity(task, reason);

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

fn evaluate_bug_severity(task: &TestTask, reason: &str) -> String {
    let failed_steps = task
        .step_logs
        .iter()
        .filter(|s| s.status == "failed")
        .count();
    let retried_steps = task.step_logs.iter().filter(|s| s.retry_count > 0).count();
    let timed_out = reason.contains("超时");
    let auth_scene = task.scenario == "login" || task.scenario == "error_prompt";

    if timed_out || failed_steps >= 2 || (auth_scene && failed_steps >= 1) {
        return "P1".to_string();
    }
    if retried_steps >= 2 || task.retries > 0 {
        return "P2".to_string();
    }
    "P3".to_string()
}

fn render_report_markdown(report: &TestReport, template: Option<&str>) -> String {
    let default_template = r#"# 测试报告

- task_id: {{task_id}}
- result: {{result}}
- summary: {{summary}}
- issue_summary: {{issue_summary}}

## 执行步骤
{{steps}}

{{bug_section}}
"#;

    let steps = report
        .steps
        .iter()
        .map(|step| {
            format!(
                "- [{}] {} | expected: {} | actual: {}",
                step.status, step.step_name, step.expected_result, step.actual_result
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let bug_section = report.bug_report.as_ref().map_or_else(
        || "## Bug 报告\n- 无".to_string(),
        |bug| {
            format!(
                "## Bug 报告\n- title: {}\n- severity: {}\n- reason: {}",
                bug.bug_title, bug.severity, bug.possible_reason
            )
        },
    );

    (template.unwrap_or(default_template))
        .replace("{{task_id}}", &report.task_id.to_string())
        .replace("{{result}}", &format!("{:?}", report.result))
        .replace("{{summary}}", &report.summary)
        .replace("{{issue_summary}}", &report.issue_summary)
        .replace("{{steps}}", &steps)
        .replace("{{bug_section}}", &bug_section)
}

fn markdown_to_html(markdown: &str) -> String {
    let escaped = markdown
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    format!(
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"UTF-8\"><title>测试报告</title></head><body><pre>{}</pre></body></html>",
        escaped
    )
}

fn render_report_pdf(report: &TestReport, markdown: &str) -> Result<Vec<u8>, String> {
    use printpdf::{BuiltinFont, Mm, PdfDocument};
    use std::io::BufWriter;

    let (doc, page, layer) = PdfDocument::new("AutoTest Report", Mm(210.0), Mm(297.0), "Layer 1");
    let current_layer = doc.get_page(page).get_layer(layer);
    let font = doc
        .add_builtin_font(BuiltinFont::Helvetica)
        .map_err(|e| format!("load pdf font failed: {}", e))?;

    let mut lines = vec![
        format!("AutoTest Report / task_id: {}", report.task_id),
        format!("result: {:?}", report.result),
        format!("summary: {}", report.summary),
        "--------------------------".to_string(),
    ];
    lines.extend(markdown.lines().take(24).map(|x| x.to_string()));

    let mut y = 285.0;
    for line in lines {
        if y < 10.0 {
            break;
        }
        current_layer.use_text(line, 10.0, Mm(10.0), Mm(y), &font);
        y -= 6.0;
    }

    let mut bytes = Vec::new();
    let mut writer = BufWriter::new(&mut bytes);
    doc.save(&mut writer)
        .map_err(|e| format!("render pdf failed: {}", e))?;
    Ok(bytes)
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

async fn mark_step_success(task_id: Uuid, step_order: u32, state: AppState) {
    let mut store = state.store.write().await;
    if let Some(task) = store.tasks.get_mut(&task_id) {
        task.next_step_order = step_order + 1;
        task.updated_at = Utc::now();
    }
    persist_store(&store);
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

        if let Err(err) = transition_task_status(task, status.clone(), "finalize_task") {
            error!(
                "invalid finalize transition for task {}: {}",
                task_id, err.message
            );
            return;
        }
        task.updated_at = Utc::now();
        if status == TaskStatus::Passed {
            task.next_step_order = task.planned_steps.len() as u32 + 1;
        }

        let screenshots = task
            .step_logs
            .iter()
            .filter_map(|x| x.screenshot_url.clone())
            .collect::<Vec<_>>();

        let report = TestReport {
            report_id: Uuid::new_v4(),
            task_id,
            tenant_id: task.tenant_id.clone(),
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
        store.reports.insert(task_id, report);
        persist_store(&state, &store).await;
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
                with_rules(
                    step(
                        1,
                        "识别当前是否在登录页",
                        ActionType::Observe,
                        serde_json::json!({}),
                        "识别到登录页",
                    ),
                    vec![VerifyRule::CurrentPageIs {
                        value: "login_page".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        2,
                        "输入账号密码",
                        ActionType::Input,
                        serde_json::json!({"fields":["username","password"]}),
                        "账号密码填充成功",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "账号输入框".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        3,
                        "点击登录按钮",
                        ActionType::Tap,
                        serde_json::json!({"target":"登录"}),
                        "进入首页或出现明确错误提示",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "登录".to_string(),
                    }],
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
                with_rules(
                    step(
                        1,
                        "定位搜索框",
                        ActionType::Observe,
                        serde_json::json!({}),
                        "搜索框可用",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "搜索框".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        2,
                        "输入关键词",
                        ActionType::Input,
                        serde_json::json!({"field":"keyword"}),
                        "关键词输入成功",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "搜索框".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        3,
                        "点击搜索按钮",
                        ActionType::Tap,
                        serde_json::json!({"target":"搜索"}),
                        "展示搜索结果或空状态",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "搜索".to_string(),
                    }],
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
                with_rules(
                    step(
                        1,
                        "进入表单页并定位必填项",
                        ActionType::Observe,
                        serde_json::json!({}),
                        "识别到必填项",
                    ),
                    vec![VerifyRule::CurrentPageIs {
                        value: "form_page".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        2,
                        "填写并提交表单",
                        ActionType::Input,
                        serde_json::json!({"field":"form_data"}),
                        "表单提交成功",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "提交".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        3,
                        "验证成功提示",
                        ActionType::Verify,
                        serde_json::json!({"contains":"提交成功"}),
                        "出现提交成功提示",
                    ),
                    vec![VerifyRule::TextContains {
                        value: "提交成功".to_string(),
                    }],
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
                with_rules(
                    step(
                        1,
                        "进入列表页",
                        ActionType::Observe,
                        serde_json::json!({}),
                        "列表页可见",
                    ),
                    vec![VerifyRule::CurrentPageIs {
                        value: "list_page".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        2,
                        "打开筛选并选择条件",
                        ActionType::Tap,
                        serde_json::json!({"target":"筛选"}),
                        "筛选条件已选择",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "筛选".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        3,
                        "点击确认并验证列表刷新",
                        ActionType::Verify,
                        serde_json::json!({"contains":"filtered"}),
                        "结果符合筛选条件",
                    ),
                    vec![VerifyRule::TextContains {
                        value: "filtered".to_string(),
                    }],
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
                with_rules(
                    step(
                        1,
                        "输入错误账号密码",
                        ActionType::Input,
                        serde_json::json!({"invalid":true}),
                        "错误数据输入成功",
                    ),
                    vec![VerifyRule::ElementExists {
                        name: "账号输入框".to_string(),
                    }],
                ),
                with_rules(
                    step(
                        2,
                        "点击登录并检查错误提示",
                        ActionType::Verify,
                        serde_json::json!({"contains":"错误"}),
                        "出现明确错误提示",
                    ),
                    vec![VerifyRule::TextContains {
                        value: "错误".to_string(),
                    }],
                ),
            ],
        );
    }

    scenario_plan(
        "generic",
        vec![],
        params,
        vec![with_rules(
            step(
                1,
                "执行通用页面可交互性检查",
                ActionType::Observe,
                serde_json::json!({}),
                "页面可正常交互",
            ),
            vec![VerifyRule::CurrentPageIs {
                value: "generic_page".to_string(),
            }],
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
        verify_rules: vec![],
    }
}

fn with_rules(mut s: PlannedStep, rules: Vec<VerifyRule>) -> PlannedStep {
    s.verify_rules = rules;
    s
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
    let elements = match current_page.as_str() {
        "login_page" => vec![
            serde_json::json!({"type":"input","name":"账号输入框","clickable":true}),
            serde_json::json!({"type":"input","name":"密码输入框","clickable":true}),
            serde_json::json!({"type":"button","name":"登录","clickable":true}),
        ],
        "search_page" => vec![
            serde_json::json!({"type":"input","name":"搜索框","clickable":true}),
            serde_json::json!({"type":"button","name":"搜索","clickable":true}),
        ],
        "form_page" => vec![
            serde_json::json!({"type":"input","name":"姓名","clickable":true}),
            serde_json::json!({"type":"button","name":"提交","clickable":true}),
        ],
        "list_page" => vec![
            serde_json::json!({"type":"button","name":"筛选","clickable":true}),
            serde_json::json!({"type":"list","name":"结果列表","clickable":false}),
        ],
        _ => vec![serde_json::json!({"type":"container","name":"页面主体","clickable":false})],
    };
    let page_tree = serde_json::json!({
        "current_page": current_page,
        "elements": elements,
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
    let (tool_name, output) = match action.action_type {
        ActionType::Observe => (
            "tree",
            serde_json::json!({"status":"ok","message":"页面结构已获取"}),
        ),
        ActionType::Tap => (
            "tap",
            serde_json::json!({"status":"ok","message":"点击成功"}),
        ),
        ActionType::Input => (
            "input",
            serde_json::json!({"status":"ok","message":"输入成功"}),
        ),
        ActionType::Swipe => (
            "swipe",
            serde_json::json!({"status":"ok","message":"滑动成功"}),
        ),
        ActionType::Verify => (
            "verify",
            serde_json::json!({"status":"ok","message":"校验动作执行"}),
        ),
        ActionType::AgentAct => {
            let instruction = action
                .action_params
                .get("instruction")
                .and_then(|x| x.as_str())
                .unwrap_or_default();
            let message = if instruction.contains("错误提示") {
                "错误提示已出现"
            } else if instruction.contains("成功提示") {
                "提交成功"
            } else if instruction.contains("筛选") {
                "filtered list ready"
            } else {
                "agent action done"
            };
            (
                "agent_act",
                serde_json::json!({"status":"ok","message":message}),
            )
        }
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

    for rule in &step.verify_rules {
        if let Some(reason) = check_verify_rule(rule, &elements, observe, action_result) {
            return VerifyResult {
                success: false,
                reason,
                actual_result: "断言失败".to_string(),
            };
        }
    }

    if step.verify_rules.is_empty() && step.description.contains("登录") {
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

fn check_verify_rule(
    rule: &VerifyRule,
    elements: &[serde_json::Value],
    observe: &ObserveResult,
    action_result: &ActionResult,
) -> Option<String> {
    match rule {
        VerifyRule::ElementExists { name } => {
            let found = elements.iter().any(|e| {
                e.get("name")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .contains(name)
            });
            if found {
                None
            } else {
                Some(format!("元素缺失: {}", name))
            }
        }
        VerifyRule::TextContains { value } => {
            let text = action_result
                .output
                .get("message")
                .and_then(|x| x.as_str())
                .unwrap_or_default();
            if text.contains(value) {
                None
            } else {
                Some(format!("文本断言失败: 缺少 {}", value))
            }
        }
        VerifyRule::CurrentPageIs { value } => {
            if &observe.current_page == value {
                None
            } else {
                Some(format!("页面断言失败: 当前为 {}", observe.current_page))
            }
        }
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
    if let Err(err) = state.persistence.save_store(store).await {
fn transition_task_status(
    task: &mut TestTask,
    target: TaskStatus,
    action: &'static str,
) -> Result<(), ApiError> {
    if can_transition(&task.status, &target) {
        task.status = target;
        return Ok(());
    }

    Err(ApiError {
        code: "invalid_status_transition",
        message: format!(
            "非法状态迁移: {:?} -> {:?} (action={})",
            task.status, target, action
        ),
    })
}

fn can_transition(from: &TaskStatus, to: &TaskStatus) -> bool {
    if from == to {
        return true;
    }

    matches!(
        (from, to),
        (TaskStatus::Blocked, TaskStatus::Pending)
            | (TaskStatus::Pending, TaskStatus::Blocked)
            | (TaskStatus::Pending, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::Paused)
            | (TaskStatus::Paused, TaskStatus::Running)
            | (TaskStatus::Running, TaskStatus::Passed)
            | (TaskStatus::Running, TaskStatus::Failed)
            | (TaskStatus::Failed, TaskStatus::Running)
            | (TaskStatus::Passed, TaskStatus::Running)
            | (TaskStatus::Pending, TaskStatus::Terminated)
            | (TaskStatus::Running, TaskStatus::Terminated)
            | (TaskStatus::Paused, TaskStatus::Terminated)
            | (TaskStatus::Blocked, TaskStatus::Terminated)
            | (TaskStatus::Failed, TaskStatus::Terminated)
    )
}

fn parse_task_status(value: &str) -> Result<TaskStatus, ApiError> {
    match value {
        "pending" => Ok(TaskStatus::Pending),
        "running" => Ok(TaskStatus::Running),
        "paused" => Ok(TaskStatus::Paused),
        "passed" => Ok(TaskStatus::Passed),
        "failed" => Ok(TaskStatus::Failed),
        "blocked" => Ok(TaskStatus::Blocked),
        "terminated" => Ok(TaskStatus::Terminated),
        _ => Err(ApiError {
            code: "invalid_status_filter",
            message: format!("unsupported status filter: {}", value),
        }),
    }
}

fn sort_tasks(tasks: &mut [TestTask], sort_by: &str, sort_order: &str) -> Result<(), ApiError> {
    match sort_by {
        "created_at" => tasks.sort_by_key(|t| t.created_at),
        "updated_at" => tasks.sort_by_key(|t| t.updated_at),
        _ => {
            return Err(ApiError {
                code: "invalid_sort_by",
                message: format!("unsupported sort_by: {}", sort_by),
            });
        }
    }

    match sort_order {
        "asc" => {}
        "desc" => tasks.reverse(),
        _ => {
            return Err(ApiError {
                code: "invalid_sort_order",
                message: format!("unsupported sort_order: {}", sort_order),
            });
        }
    }

    Ok(())
}

fn persist_store(store: &Store) {
    if let Err(err) = store.save() {
        error!("persist store failed: {}", err);
    }
}

fn build_progress(task_id: Uuid, task: &TestTask) -> TaskProgress {
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

    TaskProgress {
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
    }
fn parse_optional_datetime(
    raw: Option<&str>,
    field: &str,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    raw.map(|value| {
        DateTime::parse_from_rfc3339(value)
            .map(|x| x.with_timezone(&Utc))
            .map_err(|_| ApiError {
                code: "invalid_query_param",
                message: format!("{} must be RFC3339 datetime", field),
            })
    })
    .transpose()
}

fn validate_time_range(
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
) -> Result<(), ApiError> {
    if let (Some(start), Some(end)) = (started_at, ended_at) {
        if start > end {
            return Err(ApiError {
                code: "invalid_time_range",
                message: "started_at must be earlier than or equal to ended_at".to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use std::{
        fs,
        sync::{Mutex, OnceLock},
    };
    use tower::util::ServiceExt;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn reset_store_file() {
        let _ = fs::remove_file(STORE_FILE);
    }

    fn test_app() -> Router {
        let state = AppState {
            store: Arc::new(RwLock::new(Store::default())),
        };
        build_app(state)
    }

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

    #[tokio::test]
    async fn api_should_block_start_without_required_data() {
        let _guard = test_lock().lock().unwrap();
        reset_store_file();

        let app = test_app();
        let create_req = Request::builder()
            .method("POST")
            .uri("/api/v1/tasks")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "task_name":"login-missing-param",
                    "user_goal":"测试登录流程",
                    "params":{"username":"demo"}
                })
                .to_string(),
            ))
            .unwrap();

        let create_resp = app.clone().oneshot(create_req).await.unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);
        let create_body = to_bytes(create_resp.into_body(), usize::MAX).await.unwrap();
        let created: CreateTaskResponse = serde_json::from_slice(&create_body).unwrap();
        assert_eq!(created.status, TaskStatus::Blocked);

        let start_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/tasks/{}/start", created.task_id))
            .body(Body::empty())
            .unwrap();
        let start_resp = app.clone().oneshot(start_req).await.unwrap();
        assert_eq!(start_resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn api_should_complete_lifecycle_and_generate_report() {
        let _guard = test_lock().lock().unwrap();
        reset_store_file();

        let app = test_app();
        let create_req = Request::builder()
            .method("POST")
            .uri("/api/v1/tasks")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "task_name":"login-success",
                    "user_goal":"测试登录流程",
                    "params":{"username":"demo","password":"123456"}
                })
                .to_string(),
            ))
            .unwrap();

        let create_resp = app.clone().oneshot(create_req).await.unwrap();
        assert_eq!(create_resp.status(), StatusCode::OK);
        let create_body = to_bytes(create_resp.into_body(), usize::MAX).await.unwrap();
        let created: CreateTaskResponse = serde_json::from_slice(&create_body).unwrap();
        assert_eq!(created.status, TaskStatus::Pending);

        let start_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/tasks/{}/start", created.task_id))
            .body(Body::empty())
            .unwrap();
        let start_resp = app.clone().oneshot(start_req).await.unwrap();
        assert_eq!(start_resp.status(), StatusCode::OK);

        let mut final_progress = None;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let progress_req = Request::builder()
                .method("GET")
                .uri(format!("/api/v1/tasks/{}/progress", created.task_id))
                .body(Body::empty())
                .unwrap();
            let progress_resp = app.clone().oneshot(progress_req).await.unwrap();
            assert_eq!(progress_resp.status(), StatusCode::OK);
            let progress_body = to_bytes(progress_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let progress: TaskProgress = serde_json::from_slice(&progress_body).unwrap();

            if progress.status == TaskStatus::Passed || progress.status == TaskStatus::Failed {
                final_progress = Some(progress);
                break;
            }
        }

        let progress = final_progress.expect("task should reach terminal status");
        assert_eq!(progress.status, TaskStatus::Passed);
        assert_eq!(progress.progress_percent, 100);

        let report_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/tasks/{}/report", created.task_id))
            .body(Body::empty())
            .unwrap();
        let report_resp = app.clone().oneshot(report_req).await.unwrap();
        assert_eq!(report_resp.status(), StatusCode::OK);
        let report_body = to_bytes(report_resp.into_body(), usize::MAX).await.unwrap();
        let report: TestReport = serde_json::from_slice(&report_body).unwrap();
        assert_eq!(report.result, TaskStatus::Passed);
        assert!(!report.steps.is_empty());
    }

    #[tokio::test]
    async fn markdown_export_should_match_regression_baseline() {
        let _guard = test_lock().lock().unwrap();
        reset_store_file();

        let app = test_app();
        let create_req = Request::builder()
            .method("POST")
            .uri("/api/v1/tasks")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "task_name":"generic-smoke",
                    "user_goal":"执行一次通用检查",
                    "params":{}
                })
                .to_string(),
            ))
            .unwrap();
        let create_resp = app.clone().oneshot(create_req).await.unwrap();
        let create_body = to_bytes(create_resp.into_body(), usize::MAX).await.unwrap();
        let created: CreateTaskResponse = serde_json::from_slice(&create_body).unwrap();

        let start_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/tasks/{}/start", created.task_id))
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(start_req).await.unwrap();

        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let progress_req = Request::builder()
                .method("GET")
                .uri(format!("/api/v1/tasks/{}/progress", created.task_id))
                .body(Body::empty())
                .unwrap();
            let progress_resp = app.clone().oneshot(progress_req).await.unwrap();
            let progress_body = to_bytes(progress_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let progress: TaskProgress = serde_json::from_slice(&progress_body).unwrap();
            if progress.status == TaskStatus::Passed {
                break;
            }
        }

        let export_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/tasks/{}/report/export", created.task_id))
            .body(Body::empty())
            .unwrap();
        let export_resp = app.clone().oneshot(export_req).await.unwrap();
        assert_eq!(export_resp.status(), StatusCode::OK);
        let markdown_body = to_bytes(export_resp.into_body(), usize::MAX).await.unwrap();
        let markdown = String::from_utf8(markdown_body.to_vec()).unwrap();

        let baseline_raw =
            fs::read_to_string("tests/fixtures/report_markdown_baseline.json").unwrap();
        let baseline: serde_json::Value = serde_json::from_str(&baseline_raw).unwrap();
        let phrases = baseline
            .get("required_phrases")
            .and_then(|v| v.as_array())
            .unwrap();

        for phrase in phrases {
            let text = phrase.as_str().unwrap();
            assert!(markdown.contains(text), "markdown should include: {text}");
        }
    #[test]
    fn severity_should_be_p1_for_timeout() {
        let task = TestTask {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            user_goal: "g".to_string(),
            scenario: "search".to_string(),
            status: TaskStatus::Failed,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            params: serde_json::json!({}),
            required_data: vec![],
            missing_data: vec![],
            planned_steps: vec![],
            step_logs: vec![],
            retries: 0,
            max_retries: 2,
            max_step_retries: 2,
            step_timeout_ms: 1000,
            global_timeout_ms: 1000,
        };
        assert_eq!(evaluate_bug_severity(&task, "超过全局超时"), "P1");
    }

    #[test]
    fn markdown_template_should_apply() {
        let report = TestReport {
            report_id: Uuid::new_v4(),
            task_id: Uuid::new_v4(),
            result: TaskStatus::Passed,
            summary: "ok".to_string(),
            issue_summary: "none".to_string(),
            execution_steps: vec![],
            actual_result: "done".to_string(),
            expected_result: "done".to_string(),
            steps: vec![],
            bug_report: None,
            screenshots: vec![],
            created_at: Utc::now(),
        };
        let rendered = render_report_markdown(&report, Some("任务={{task_id}} 结果={{result}}"));
        assert!(rendered.contains("任务="));
        assert!(rendered.contains("结果=Passed"));
    fn planner_step_should_include_verify_rules() {
        let p = planner_plan("测试登录流程", &serde_json::json!({}));
        assert!(!p.steps[0].verify_rules.is_empty());
    }

    #[test]
    fn verify_rule_should_check_current_page() {
        let rule = VerifyRule::CurrentPageIs {
            value: "login_page".to_string(),
        };
        let observe = ObserveResult {
            current_page: "search_page".to_string(),
            page_tree: serde_json::json!({"elements":[]}),
            screenshot_url: "mock.jpg".to_string(),
        };
        let result = check_verify_rule(
            &rule,
            &[],
            &observe,
            &ActionResult {
                success: true,
                output: serde_json::json!({}),
            },
        );
        assert!(result.is_some());
    fn status_transition_should_be_checked() {
        assert!(can_transition(&TaskStatus::Pending, &TaskStatus::Running));
        assert!(can_transition(&TaskStatus::Running, &TaskStatus::Passed));
        assert!(!can_transition(&TaskStatus::Passed, &TaskStatus::Paused));
    }

    #[test]
    fn sort_tasks_should_support_created_at() {
        let now = Utc::now();
        let old = now - chrono::Duration::seconds(10);
        let mut tasks = vec![
            TestTask {
                task_id: Uuid::new_v4(),
                task_name: "b".to_string(),
                user_goal: "g".to_string(),
                scenario: "search".to_string(),
                status: TaskStatus::Pending,
                created_at: now,
                updated_at: now,
                params: serde_json::json!({}),
                required_data: vec![],
                missing_data: vec![],
                planned_steps: vec![],
                step_logs: vec![],
                retries: 0,
                max_retries: 2,
                max_step_retries: 2,
                step_timeout_ms: 1000,
                global_timeout_ms: 1000,
            },
            TestTask {
                task_id: Uuid::new_v4(),
                task_name: "a".to_string(),
                user_goal: "g".to_string(),
                scenario: "search".to_string(),
                status: TaskStatus::Pending,
                created_at: old,
                updated_at: old,
                params: serde_json::json!({}),
                required_data: vec![],
                missing_data: vec![],
                planned_steps: vec![],
                step_logs: vec![],
                retries: 0,
                max_retries: 2,
                max_step_retries: 2,
                step_timeout_ms: 1000,
                global_timeout_ms: 1000,
            },
        ];

        sort_tasks(&mut tasks, "created_at", "asc").unwrap();
        assert_eq!(tasks[0].task_name, "a");
        sort_tasks(&mut tasks, "created_at", "desc").unwrap();
        assert_eq!(tasks[0].task_name, "b");
    }
}
