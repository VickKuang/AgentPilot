# AutoTest Agent MVP 执行指令（可直接复制给开发/编程/产品 Agent）

你是一名**资深产品经理 + AI Agent 架构师 + 全栈工程师**，请基于以下 PRD，设计并实现一个可落地的 MVP 版本。

你的输出目标不是泛泛而谈，而是要产出**可执行、可拆解、可开发、可验收**的方案与任务清单。

---

## 一、你的工作目标（必须完成）

请基于 PRD 输出以下 9 项内容：

1. **系统技术架构拆解**（前端、后端、Agent Orchestrator、工具层、数据层、存储层）
2. **前端页面结构设计**（信息架构 + 页面模块 + 关键交互流程）
3. **后端 API 设计**（REST/GraphQL 均可，需给出核心接口定义）
4. **Agent 调度流程设计**（Planner/Observer/Action/Verifier/Reporter）
5. **工具调用协议设计**（screenshot/tree/tap/swipe/input 等）
6. **数据库表结构设计**（核心表、字段、索引、状态流转）
7. **MVP 开发任务清单**（按优先级拆分，可直接进 Jira/禅道）
8. **可执行开发计划**（按周里程碑、角色分工、交付物）
9. **必要的原型代码或伪代码**（至少覆盖调度主流程 + 一条场景链路）

---

## 二、业务背景与产品目标（PRD 摘要）

- 当前 UI 回归测试高度依赖人工，重复、耗时、报告质量不稳定。
- 传统脚本式 UI 自动化在页面改版后维护成本高。
- 目标是打造一个可通过自然语言驱动的 UI 测试 Agent，实现：
  - 自动测试计划生成
  - 页面观察与理解
  - 动作执行
  - 结果验证
  - 异常自恢复
  - 测试报告/Bug 报告自动生成

### MVP 一期范围

优先支持以下核心链路：

1. 登录流程
2. 搜索流程
3. 表单提交流程

（列表筛选、异常提示可作为扩展场景预留接口）

---

## 三、功能与能力边界（必须覆盖）

### 1）自然语言任务输入

输入示例：
- 测试登录流程是否正常
- 测试搜索功能是否正常，关键词为手机
- 测试反馈表单是否可提交

系统应识别：
- 测试目标
- 测试对象
- 预期结果
- 必要参数（账号/密码/关键词等）
- 参数缺失时触发补充询问

### 2）多 Agent 协同

- **Planner Agent**：任务理解、步骤拆解、测试数据需求识别
- **Observer Agent**：读取 screenshot + tree，识别页面状态与可操作元素
- **Action Agent**：调用 UI 原子动作工具并记录执行日志
- **Verifier Agent**：逐步校验预期结果，判断继续/重试/终止
- **Reporter Agent**：生成结构化测试报告与失败 Bug 报告

### 3）工具能力（MVP）

必须支持：
- screenshot（JPEG）
- tree（JSON）
- tap
- input
- swipe

可预留扩展：
- long-press / pinch / rotate / agent act

### 4）异常处理与自恢复（MVP 必做）

至少实现以下策略：
- 找不到元素：重读 tree → 判断需滚动 → swipe 后重试
- 点击无响应：等待 + 再观察（screenshot/tree）+ 状态判定
- 登录失败：区分凭证错误、接口异常、无响应

### 5）报告输出（结构化）

- 测试报告：步骤、预期、实际、截图、日志、结论
- Bug 报告：标题、严重级别、复现步骤、证据、可能原因

---

## 四、非功能要求（必须体现到方案）

1. **可追踪性**：保留关键决策、工具调用、页面快照、步骤状态
2. **稳定性**：失败要有原因，不允许静默失败
3. **安全性**：账号密码等敏感字段加密/脱敏
4. **可扩展性**：后续可扩展到支付、下单、权限、多端兼容等

---

## 五、你输出内容的格式要求（严格遵守）

请按以下章节顺序输出：

1. `System Architecture`
2. `Frontend Design`
3. `Backend API Design`
4. `Agent Orchestration`
5. `Tool Protocol Spec`
6. `Database Schema`
7. `MVP Task Breakdown`
8. `Development Plan (Weekly Milestones)`
9. `Pseudocode / Prototype`
10. `Risks & Mitigations`
11. `MVP Acceptance Criteria`

每个章节必须包含：
- 设计说明
- 为什么这样设计
- 与 PRD 对应关系

---

## 六、交付粒度要求（避免空泛）

### API 需至少包含

- 创建测试任务
- 启动执行
- 查询执行状态
- 获取步骤日志
- 获取测试报告
- 获取 Bug 报告
- 终止/重试任务

### 数据库至少包含

- test_tasks
- test_steps
- test_reports
- bug_reports
- tool_call_logs
- page_snapshots

并给出：
- 主键/外键
- 关键索引
- 状态字段枚举

### 调度流程需明确

- 状态机（pending/running/passed/failed/blocked）
- 重试次数与退出条件
- 单步超时与全局超时
- 并发策略（串行/有限并发）

---

## 七、验收标准（MVP Definition of Done）

满足以下条件才算 MVP 完成：

1. 能通过自然语言发起 3 条核心测试链路（登录/搜索/表单）
2. 每条链路具备 Planner→Observer→Action→Verifier→Reporter 闭环
3. 执行过程有完整步骤日志与截图证据
4. 失败时自动生成结构化 Bug 报告
5. 可在页面查看任务状态、执行过程、最终报告
6. 具备基础重试与异常自恢复能力
7. 数据可持久化并支持任务历史查询

---

## 八、建议技术栈（可按实际调整）

- 前端：React + TypeScript + Ant Design
- 后端：Node.js（NestJS）或 Python（FastAPI）
- Agent 编排：LangGraph / 自研状态机
- 消息队列：Redis + BullMQ（异步任务）
- 数据库：PostgreSQL
- 对象存储：S3/MinIO（截图与报告附件）
- 可观测性：OpenTelemetry + Loki/ELK

> 若你选择其他技术栈，需要说明替代原因和成本影响。

---

## 九、输出风格要求

- 用中文输出；
- 不要泛泛描述，尽量给出结构化表格、JSON 示例、伪代码；
- 尽量让“开发同学拿到即开工”；
- 若你认为 PRD 有缺口，请先列出“关键待确认问题”，并给默认假设继续推进。
