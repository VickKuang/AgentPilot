# 安全与平台能力模块

## 已实现
- API Key 鉴权中间件（`x-api-key`），支持凭证从环境变量加载。
- RBAC（`admin/operator/viewer`）按 HTTP 方法进行权限控制。
- 多租户隔离（`x-tenant-id`）：任务、报告、日志查询均按租户约束。
- API 限流（按 key + tenant + method 的分钟窗口计数）。
- 审计日志（任务创建、启动、重试、暂停、恢复、终止、补参）与审计查询 API。
- 密钥管理集成：凭证元信息包含 `kms_key_ref`，并提供安全上下文 API 返回 `kms_provider` / `key_id`。

## 待完善（本轮明确缺口）
- 增加 JWT/OIDC 双栈鉴权与短期令牌轮换。
- 将限流从单节点内存计数升级为 Redis 或网关级全局限流。
- 审计日志接入不可篡改归档与告警联动。

## 拆分建议
- `security/auth`：鉴权与角色校验。
- `security/tenant`：租户隔离策略。
- `security/rate_limit`：限流策略。
- `security/audit`：审计模型与查询。
