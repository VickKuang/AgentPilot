-- PostgreSQL schema for AutoTest Agent (full MVP)

create table if not exists test_tasks (
  task_id uuid primary key,
  task_name varchar(255) not null,
  user_goal text not null,
  scenario varchar(64) not null,
  status varchar(32) not null,
  params jsonb not null default '{}'::jsonb,
  required_data jsonb not null default '[]'::jsonb,
  missing_data jsonb not null default '[]'::jsonb,
  retries int not null default 0,
  max_retries int not null default 2,
  max_step_retries int not null default 2,
  step_timeout_ms int not null default 4000,
  global_timeout_ms int not null default 45000,
  created_at timestamptz not null,
  updated_at timestamptz not null
);
create index if not exists idx_test_tasks_status on test_tasks(status);
create index if not exists idx_test_tasks_updated on test_tasks(updated_at desc);

create table if not exists test_steps (
  step_id uuid primary key,
  task_id uuid not null references test_tasks(task_id) on delete cascade,
  step_order int not null,
  step_name text not null,
  action_type varchar(32) not null,
  action_params jsonb not null default '{}'::jsonb,
  expected_result text not null,
  actual_result text,
  status varchar(32) not null,
  retry_count int not null default 0,
  screenshot_url text,
  page_tree jsonb,
  created_at timestamptz not null
);
create index if not exists idx_test_steps_task on test_steps(task_id, created_at);

create table if not exists tool_call_logs (
  id uuid primary key,
  task_id uuid not null references test_tasks(task_id) on delete cascade,
  step_order int not null,
  tool_name varchar(64) not null,
  request_payload jsonb not null default '{}'::jsonb,
  response_payload jsonb not null default '{}'::jsonb,
  success boolean not null,
  latency_ms bigint not null,
  created_at timestamptz not null
);
create index if not exists idx_tool_call_logs_task on tool_call_logs(task_id, created_at);

create table if not exists page_snapshots (
  id uuid primary key,
  task_id uuid not null references test_tasks(task_id) on delete cascade,
  step_order int not null,
  screenshot_url text not null,
  page_tree jsonb not null,
  current_page varchar(128) not null,
  created_at timestamptz not null
);
create index if not exists idx_page_snapshots_task on page_snapshots(task_id, created_at);

create table if not exists test_reports (
  report_id uuid primary key,
  task_id uuid not null unique references test_tasks(task_id) on delete cascade,
  result varchar(32) not null,
  summary text not null,
  issue_summary text,
  execution_steps jsonb not null default '[]'::jsonb,
  actual_result text,
  expected_result text,
  screenshots jsonb not null default '[]'::jsonb,
  created_at timestamptz not null
);

create table if not exists bug_reports (
  bug_id uuid primary key,
  task_id uuid not null unique references test_tasks(task_id) on delete cascade,
  bug_title text not null,
  severity varchar(16) not null,
  reproduction_steps jsonb not null default '[]'::jsonb,
  actual_result text not null,
  expected_result text not null,
  evidence jsonb not null default '[]'::jsonb,
  possible_reason text,
  created_at timestamptz not null
);
