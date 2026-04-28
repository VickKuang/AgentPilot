# Tool Protocol (Full MVP)

## 通用请求

```json
{
  "request_id": "uuid",
  "task_id": "uuid",
  "step_order": 1,
  "tool_name": "screenshot | tree | tap | input | swipe | agent_act",
  "payload": {}
}
```

## 通用响应

```json
{
  "request_id": "uuid",
  "success": true,
  "latency_ms": 16,
  "output": {},
  "error": null
}
```

## screenshot

```json
{"format":"jpeg","quality":80}
```

## tree

```json
{"depth":8,"include_hidden":false}
```

## tap

```json
{"target":"登录"}
```

## input

```json
{"field":"username","value":"demo"}
```

## swipe

```json
{"direction":"down","distance":0.5}
```

## agent_act

```json
{"instruction":"检查是否出现错误提示"}
```

## 错误码建议

- `element_not_found`
- `action_no_response`
- `page_timeout`
- `driver_internal_error`
