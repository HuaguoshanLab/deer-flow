"""基于 Redis 的跨 Pod run 状态管理器。

架构：
- run 状态存储在 Redis Hash `run:{run_id}`，成功/失败后 TTL 5 分钟自动过期。
- 维护 Set 索引 `thread:runs:{thread_id}` 加速按 thread 查询。
- 取消信号通过 `PUBLISH run:cancel:{run_id} interrupt/rollback` 广播。
- 每个 Pod 启动时订阅 `run:cancel:*`，收到信号后取消本地 asyncio.Task。
- asyncio.Task 和 abort_event 仍保留在本地内存（只有执行该 run 的 Pod 才有）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

import redis.asyncio as aioredis

from .manager import ConflictError, UnsupportedStrategyError
from .schemas import DisconnectMode, RunStatus

logger = logging.getLogger(__name__)

# 成功/失败 run 在 Redis 中的保留时间（秒）
_TERMINAL_TTL = 300
# 进行中 run 的最大保留时间，防止 Pod 崩溃后 key 永久残留（秒）
_INFLIGHT_TTL = 3600


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _run_key(run_id: str) -> str:
    return f"run:{run_id}"


def _thread_runs_key(thread_id: str) -> str:
    return f"thread:runs:{thread_id}"


def _cancel_channel(run_id: str) -> str:
    return f"run:cancel:{run_id}"


@dataclass
class RunRecord:
    """单个 run 的可变记录（本地内存部分）。"""

    run_id: str
    thread_id: str
    assistant_id: str | None
    status: RunStatus
    on_disconnect: DisconnectMode
    multitask_strategy: str = "reject"
    metadata: dict = field(default_factory=dict)
    kwargs: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""
    task: asyncio.Task | None = field(default=None, repr=False)
    abort_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    abort_action: str = "interrupt"
    error: str | None = None


class RedisRunManager:
    """基于 Redis 的跨 Pod run 状态管理器，与 RunManager 接口兼容。

    本地内存中保存 asyncio.Task 和 abort_event，Redis 中保存可序列化的状态字段。
    """

    def __init__(self, *, redis_url: str = "redis://localhost:6379/0") -> None:
        self._redis_url = redis_url
        self._client: aioredis.Redis | None = None
        self._lock = asyncio.Lock()
        # 本地 Pod 持有的 run 记录（含 Task / abort_event）
        self._local: dict[str, RunRecord] = {}
        # 取消信号监听任务
        self._cancel_listener_task: asyncio.Task | None = None

    async def start(self) -> None:
        """启动取消信号监听器，应在 Pod 启动时调用。"""
        self._cancel_listener_task = asyncio.create_task(
            self._cancel_listener_loop(), name="redis-run-cancel-listener"
        )

    async def stop(self) -> None:
        """停止监听器并关闭 Redis 连接。"""
        if self._cancel_listener_task is not None:
            self._cancel_listener_task.cancel()
            try:
                await self._cancel_listener_task
            except asyncio.CancelledError:
                pass
            self._cancel_listener_task = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def _get_client(self) -> aioredis.Redis:
        """懒初始化共享 Redis 客户端。"""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = aioredis.from_url(
                        self._redis_url,
                        decode_responses=True,
                        socket_connect_timeout=5,
                    )
        return self._client

    # ------------------------------------------------------------------
    # 取消信号监听
    # ------------------------------------------------------------------

    async def _cancel_listener_loop(self) -> None:
        """订阅 run:cancel:* 频道，收到信号后取消本地 Task。"""
        while True:
            try:
                client = await self._get_client()
                pubsub = client.pubsub()
                await pubsub.psubscribe("run:cancel:*")
                logger.info("RedisRunManager: 已订阅 run:cancel:* 取消信号")
                async for message in pubsub.listen():
                    if message["type"] != "pmessage":
                        continue
                    channel: str = message.get("channel", "")
                    # channel 格式：run:cancel:{run_id}
                    run_id = channel.removeprefix("run:cancel:")
                    action = message.get("data", "interrupt")
                    await self._handle_cancel_signal(run_id, action)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("RedisRunManager 取消监听器异常，5 秒后重连: %s", exc)
                await asyncio.sleep(5)

    async def _handle_cancel_signal(self, run_id: str, action: str) -> None:
        """处理来自 Redis 的取消信号，取消本地 Task。"""
        record = self._local.get(run_id)
        if record is None:
            return
        record.abort_action = action
        record.abort_event.set()
        if record.task is not None and not record.task.done():
            record.task.cancel()
        logger.info("Run %s 收到跨 Pod 取消信号 (action=%s)", run_id, action)

    # ------------------------------------------------------------------
    # RunManager 兼容接口
    # ------------------------------------------------------------------

    async def create(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
    ) -> RunRecord:
        """创建新的 pending run 并注册到 Redis。"""
        run_id = str(uuid.uuid4())
        now = _now_iso()
        record = RunRecord(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_id,
            status=RunStatus.pending,
            on_disconnect=on_disconnect,
            multitask_strategy=multitask_strategy,
            metadata=metadata or {},
            kwargs=kwargs or {},
            created_at=now,
            updated_at=now,
        )
        self._local[run_id] = record
        await self._save_to_redis(record)
        logger.info("Run created: run_id=%s thread_id=%s", run_id, thread_id)
        return record

    def get(self, run_id: str) -> RunRecord | None:
        """优先从本地内存查找，再从 Redis 重建（不含 Task/abort_event）。"""
        return self._local.get(run_id)

    async def get_remote(self, run_id: str) -> RunRecord | None:
        """从 Redis 重建 RunRecord（不含 Task/abort_event，用于跨 Pod 查询）。"""
        client = await self._get_client()
        data = await client.hgetall(_run_key(run_id))
        if not data:
            return None
        return _record_from_redis(run_id, data)

    async def list_by_thread(self, thread_id: str) -> list[RunRecord]:
        """返回该 thread 的所有 run，优先本地，补充 Redis 中其他 Pod 的记录。"""
        client = await self._get_client()
        run_ids = await client.smembers(_thread_runs_key(thread_id))
        records: list[RunRecord] = []
        for run_id in run_ids:
            local = self._local.get(run_id)
            if local is not None:
                records.append(local)
            else:
                remote = await self.get_remote(run_id)
                if remote is not None:
                    records.append(remote)
        # 按创建时间倒序
        records.sort(key=lambda r: r.created_at, reverse=True)
        return records

    async def set_status(self, run_id: str, status: RunStatus, *, error: str | None = None) -> None:
        """更新 run 状态，同步到 Redis。"""
        record = self._local.get(run_id)
        if record is None:
            logger.warning("set_status called for unknown run %s", run_id)
            return
        record.status = status
        record.updated_at = _now_iso()
        if error is not None:
            record.error = error
        await self._save_to_redis(record)
        # 终态 run 设置 TTL
        if status in (RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted):
            client = await self._get_client()
            await client.expire(_run_key(run_id), _TERMINAL_TTL)
        logger.info("Run %s -> %s", run_id, status.value)

    async def cancel(self, run_id: str, *, action: str = "interrupt") -> bool:
        """取消 run：本地直接取消，跨 Pod 通过 Redis Pub/Sub 广播。"""
        # 先查本地
        record = self._local.get(run_id)
        if record is not None:
            if record.status not in (RunStatus.pending, RunStatus.running):
                return False
            record.abort_action = action
            record.abort_event.set()
            if record.task is not None and not record.task.done():
                record.task.cancel()
            record.status = RunStatus.interrupted
            record.updated_at = _now_iso()
            await self._save_to_redis(record)
            logger.info("Run %s 本地取消 (action=%s)", run_id, action)
            return True

        # 查 Redis（其他 Pod 的 run）
        client = await self._get_client()
        data = await client.hgetall(_run_key(run_id))
        if not data:
            return False
        status_val = data.get("status", "")
        if status_val not in (RunStatus.pending, RunStatus.running):
            return False

        # 广播取消信号
        await client.publish(_cancel_channel(run_id), action)
        # 更新 Redis 状态
        now = _now_iso()
        await client.hset(_run_key(run_id), mapping={"status": RunStatus.interrupted, "updated_at": now})
        await client.expire(_run_key(run_id), _TERMINAL_TTL)
        logger.info("Run %s 跨 Pod 取消广播 (action=%s)", run_id, action)
        return True

    async def create_or_reject(
        self,
        thread_id: str,
        assistant_id: str | None = None,
        *,
        on_disconnect: DisconnectMode = DisconnectMode.cancel,
        metadata: dict | None = None,
        kwargs: dict | None = None,
        multitask_strategy: str = "reject",
    ) -> RunRecord:
        """原子检查 inflight run 并创建新 run。"""
        _supported_strategies = ("reject", "interrupt", "rollback")
        if multitask_strategy not in _supported_strategies:
            raise UnsupportedStrategyError(
                f"Multitask strategy '{multitask_strategy}' is not yet supported. "
                f"Supported strategies: {', '.join(_supported_strategies)}"
            )

        async with self._lock:
            inflight = await self._get_inflight_records(thread_id)

            if multitask_strategy == "reject" and inflight:
                raise ConflictError(f"Thread {thread_id} already has an active run")

            if multitask_strategy in ("interrupt", "rollback") and inflight:
                for r in inflight:
                    await self.cancel(r.run_id, action=multitask_strategy)
                logger.info(
                    "已取消 %d 个 inflight run (thread=%s, strategy=%s)",
                    len(inflight),
                    thread_id,
                    multitask_strategy,
                )

            return await self.create(
                thread_id,
                assistant_id,
                on_disconnect=on_disconnect,
                metadata=metadata,
                kwargs=kwargs,
                multitask_strategy=multitask_strategy,
            )

    async def has_inflight(self, thread_id: str) -> bool:
        """检查 thread 是否有 pending/running 的 run。"""
        inflight = await self._get_inflight_records(thread_id)
        return len(inflight) > 0

    async def cleanup(self, run_id: str, *, delay: float = 300) -> None:
        """清理本地记录，Redis 依赖 TTL 自动过期。"""
        if delay > 0:
            await asyncio.sleep(delay)
        self._local.pop(run_id, None)
        logger.debug("Run record %s 本地清理完成", run_id)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    async def _save_to_redis(self, record: RunRecord) -> None:
        """将 RunRecord 的可序列化字段写入 Redis Hash，并维护 Set 索引。"""
        client = await self._get_client()
        mapping = {
            "thread_id": record.thread_id,
            "assistant_id": record.assistant_id or "",
            "status": record.status.value,
            "on_disconnect": record.on_disconnect.value,
            "multitask_strategy": record.multitask_strategy,
            "metadata": json.dumps(record.metadata, ensure_ascii=False),
            "kwargs": json.dumps(record.kwargs, ensure_ascii=False),
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "abort_action": record.abort_action,
            "error": record.error or "",
        }
        key = _run_key(record.run_id)
        await client.hset(key, mapping=mapping)
        # 进行中的 run 设置较长 TTL 防止 Pod 崩溃后 key 永久残留
        if record.status in (RunStatus.pending, RunStatus.running):
            await client.expire(key, _INFLIGHT_TTL)
        # 维护 thread → run_ids 索引
        await client.sadd(_thread_runs_key(record.thread_id), record.run_id)
        # Set 索引 TTL 与 run key 保持一致
        await client.expire(_thread_runs_key(record.thread_id), _INFLIGHT_TTL)

    async def _get_inflight_records(self, thread_id: str) -> list[RunRecord]:
        """获取 thread 下所有 pending/running 的 run 记录。"""
        client = await self._get_client()
        run_ids = await client.smembers(_thread_runs_key(thread_id))
        inflight: list[RunRecord] = []
        for run_id in run_ids:
            local = self._local.get(run_id)
            if local is not None:
                if local.status in (RunStatus.pending, RunStatus.running):
                    inflight.append(local)
            else:
                data = await client.hgetall(_run_key(run_id))
                if data and data.get("status") in (RunStatus.pending, RunStatus.running):
                    inflight.append(_record_from_redis(run_id, data))
        return inflight


def _record_from_redis(run_id: str, data: dict) -> RunRecord:
    """从 Redis Hash 数据重建 RunRecord（不含 Task/abort_event）。"""
    return RunRecord(
        run_id=run_id,
        thread_id=data.get("thread_id", ""),
        assistant_id=data.get("assistant_id") or None,
        status=RunStatus(data.get("status", RunStatus.pending)),
        on_disconnect=DisconnectMode(data.get("on_disconnect", DisconnectMode.cancel)),
        multitask_strategy=data.get("multitask_strategy", "reject"),
        metadata=_safe_json(data.get("metadata", "{}")),
        kwargs=_safe_json(data.get("kwargs", "{}")),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
        abort_action=data.get("abort_action", "interrupt"),
        error=data.get("error") or None,
    )


def _safe_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
