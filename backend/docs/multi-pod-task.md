# DeerFlow Gateway 模式多 Pod 改造计划

## 背景

Gateway 模式（`make dev-pro`）将 agent runtime 内嵌于 FastAPI，不依赖 `langgraph dev`。
当前单进程可用，多 Pod 部署需解决以下四个组件的跨进程共享问题。

NFS 挂载（memory.json / uploads / outputs）已就绪，不在本计划范围内。

---

## Task 1：Checkpointer → PostgreSQL

**目标**：对话状态（LangGraph checkpoint）持久化到 Postgres，多 Pod 共享。

**现状**：
- 支持 `memory` / `sqlite` / `postgres` 三种类型，配置在 `config.yaml`
- 实现文件：`packages/harness/deerflow/agents/checkpointer/async_provider.py`
- 官方库 `langgraph-checkpoint-postgres` 已在依赖列表，但未安装

**改造步骤**：

1. 安装依赖：
   ```bash
   uv add langgraph-checkpoint-postgres psycopg[binary] psycopg-pool
   ```

2. 修改 `config.yaml`：
   ```yaml
   checkpointer:
     type: postgres
     connection_string: postgresql://user:password@host:5432/deerflow
   ```

3. 验证：启动 Gateway，检查 Postgres 中是否自动创建 `checkpoints` 相关表（`AsyncPostgresSaver.setup()` 自动建表）。

**涉及文件**：
- `config.yaml`（配置变更）
- `packages/harness/deerflow/agents/checkpointer/async_provider.py`（无需改动，已支持）

---

## Task 2：Thread Store → PostgreSQL

**目标**：thread 元数据（列表、标题、状态）持久化到 Postgres，多 Pod 共享。

**现状**：
- Store 复用 `checkpointer` 配置，无独立配置项
- 实现文件：`packages/harness/deerflow/runtime/store/async_provider.py`
- Task 1 完成后，Store 自动切换到 `AsyncPostgresStore`

**改造步骤**：

1. Task 1 完成后无需额外配置，Store 自动跟随 checkpointer 类型。

2. 安装额外依赖（Store 需要独立的 psycopg pool）：
   ```bash
   uv add langgraph-checkpoint-postgres  # Task 1 已安装，此处确认
   ```

3. 验证：调用 `POST /api/threads` 创建 thread，重启 Gateway 后调用 `GET /api/threads` 确认数据持久化。

**涉及文件**：
- `packages/harness/deerflow/runtime/store/async_provider.py`（无需改动，已支持）

---

## Task 3：StreamBridge → Redis Pub/Sub 实现

**目标**：agent 执行事件流可跨 Pod 订阅，任意 Pod 均可消费任意 run 的 SSE 事件。

**现状**：
- 抽象接口已定义：`packages/harness/deerflow/runtime/stream_bridge/base.py`
- 内存实现：`packages/harness/deerflow/runtime/stream_bridge/memory.py`
- 工厂方法已预留 redis 分支但抛 `NotImplementedError`：`async_provider.py` 第 49 行
- 配置类已定义 `redis_url` 字段：`packages/harness/deerflow/config/stream_bridge_config.py`

**改造步骤**：

1. 安装依赖：
   ```bash
   uv add redis[asyncio]
   ```

2. 新建 `packages/harness/deerflow/runtime/stream_bridge/redis_bridge.py`，实现 `RedisStreamBridge`：

   - `publish(run_id, event, data)`：
     - `XADD run:events:{run_id} * event ... data ...`（Redis Stream，支持回放）
     - `PUBLISH run:notify:{run_id} ""`（Pub/Sub，唤醒等待中的订阅者）

   - `subscribe(run_id, *, last_event_id=None)`：
     - 若有 `last_event_id`，先 `XRANGE run:events:{run_id} last_event_id+1 +` 回放历史
     - 再 `SUBSCRIBE run:notify:{run_id}` 实时监听，收到通知后 `XREAD` 拉取新事件
     - 超时无事件时 yield `HEARTBEAT_SENTINEL`
     - 收到 `__end__` 事件时 yield `END_SENTINEL` 并退出

   - `publish_end(run_id)`：写入特殊 `__end__` 事件到 Stream，并 PUBLISH 通知

   - `cleanup(run_id, *, delay=0)`：`DEL run:events:{run_id}`，可选延迟

3. 修改 `async_provider.py`，填充 redis 分支：
   ```python
   if config.type == "redis":
       from deerflow.runtime.stream_bridge.redis_bridge import RedisStreamBridge
       bridge = RedisStreamBridge(redis_url=config.redis_url)
       try:
           yield bridge
       finally:
           await bridge.close()
       return
   ```

4. 修改 `config.yaml`，新增 `stream_bridge` 配置节（需同步更新 `app_config.py` 解析逻辑）：
   ```yaml
   stream_bridge:
     type: redis
     redis_url: redis://host:6379/0
   ```

5. 验证：两个 Gateway 实例，Pod A 发起 run，Pod B 订阅 SSE，确认事件正常推送。

**涉及文件**：
- `packages/harness/deerflow/runtime/stream_bridge/redis_bridge.py`（新建）
- `packages/harness/deerflow/runtime/stream_bridge/async_provider.py`（填充 redis 分支）
- `packages/harness/deerflow/config/app_config.py`（解析 stream_bridge 配置节）
- `config.yaml` / `config.example.yaml`（新增配置示例）

---

## Task 4：RunManager → Redis 持久化

**目标**：run 状态（pending/running/success/error）跨 Pod 可见；取消信号跨 Pod 广播。

**现状**：
- 纯内存实现：`packages/harness/deerflow/runtime/runs/manager.py`
- `RunRecord` 包含 `asyncio.Task` 和 `asyncio.Event`，这两个字段天然无法跨进程

**改造步骤**：

1. 新建 `packages/harness/deerflow/runtime/runs/redis_manager.py`，实现 `RedisRunManager`，与 `RunManager` 接口兼容：

   - run 状态存 Redis Hash：`HSET run:{run_id} status running thread_id xxx ...`
   - `list_by_thread(thread_id)`：`SCAN` 或维护 `thread:runs:{thread_id}` Set 索引
   - `cancel(run_id)`：`PUBLISH run:cancel:{run_id} interrupt`，本地 Pod 订阅后设置 `abort_event`
   - `has_inflight(thread_id)`：查 Redis Hash 中 status 为 pending/running 的记录

2. 每个 Pod 启动时订阅 `run:cancel:*` 频道，收到取消信号后查找本地 `asyncio.Task` 并 cancel。

3. `RunRecord` 中的 `asyncio.Task` 和 `abort_event` 仍保留在本地内存（只有执行该 run 的 Pod 才有），跨 Pod 取消通过 Redis Pub/Sub 传递信号。

4. 修改 `deps.py`，根据配置选择 `RunManager` 或 `RedisRunManager`：
   ```python
   # 若 stream_bridge.type == "redis"，使用 RedisRunManager
   app.state.run_manager = RedisRunManager(redis_url=...) if use_redis else RunManager()
   ```

5. run 记录 TTL：成功/失败的 run 在 Redis 中保留 5 分钟后自动过期（`EXPIRE run:{run_id} 300`）。

**涉及文件**：
- `packages/harness/deerflow/runtime/runs/redis_manager.py`（新建）
- `app/gateway/deps.py`（按配置选择 RunManager 实现）

---

## 改造优先级与依赖关系

```
Task 1 (Checkpointer/Postgres)
  └─→ Task 2 (Store/Postgres) [自动完成，无额外工作]

Task 3 (StreamBridge/Redis)
  └─→ Task 4 (RunManager/Redis) [共用同一 Redis 实例]
```

Task 1+2 可独立上线，解决状态持久化问题。
Task 3+4 需同步上线，解决跨 Pod 事件流和 run 管理问题。

---

## 基础设施要求

| 组件 | 规格建议 |
|------|---------|
| PostgreSQL | >= 14，建议独立实例，开启连接池（pgBouncer） |
| Redis | >= 6.2（需要 Redis Streams），单实例或 Sentinel |

---

## 验收标准

1. 启动 2 个 Gateway Pod，共享同一 Postgres + Redis
2. Pod A 创建 thread 并发起对话
3. Pod B 可查询到该 thread（Store 共享验证）
4. Pod B 订阅该 run 的 SSE，可收到完整事件流（StreamBridge 跨 Pod 验证）
5. 重启所有 Pod 后，历史 thread 和对话状态仍可恢复（Checkpointer 持久化验证）
6. 通过 Pod A 取消一个正在 Pod B 执行的 run，run 正常中断（RunManager 跨 Pod 取消验证）
