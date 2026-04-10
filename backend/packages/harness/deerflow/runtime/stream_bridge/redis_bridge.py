"""基于 Redis Streams + Pub/Sub 的跨 Pod 事件流实现。

架构：
- 每个 run 的事件写入 Redis Stream `run:events:{run_id}`，支持断线重连回放。
- 同时向 `run:notify:{run_id}` 发布 Pub/Sub 通知，唤醒等待中的订阅者。
- 结束信号写入特殊 `__end__` 事件，订阅者收到后退出。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.client import PubSub

from .base import END_SENTINEL, HEARTBEAT_SENTINEL, StreamBridge, StreamEvent

logger = logging.getLogger(__name__)

# Redis Stream 中标记流结束的特殊事件名
_END_EVENT = "__end__"
# Stream key 前缀
_STREAM_KEY = "run:events:{run_id}"
# Pub/Sub 通知 channel 前缀
_NOTIFY_KEY = "run:notify:{run_id}"


def _stream_key(run_id: str) -> str:
    return f"run:events:{run_id}"


def _notify_key(run_id: str) -> str:
    return f"run:notify:{run_id}"


class RedisStreamBridge(StreamBridge):
    """Redis Streams + Pub/Sub 实现的跨 Pod 事件流桥接器。

    - publish / publish_end：写入 Redis Stream，并通过 Pub/Sub 唤醒订阅者。
    - subscribe：先回放历史事件，再实时监听新事件。
    - cleanup：删除 Stream key，可选延迟。
    """

    def __init__(self, *, redis_url: str = "redis://localhost:6379/0") -> None:
        self._redis_url = redis_url
        # 共享连接池，用于 publish / cleanup 等写操作
        self._client: aioredis.Redis | None = None
        self._lock = asyncio.Lock()

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
    # StreamBridge API
    # ------------------------------------------------------------------

    async def publish(self, run_id: str, event: str, data: Any) -> None:
        """将事件写入 Redis Stream，并通过 Pub/Sub 通知订阅者。"""
        client = await self._get_client()
        payload = json.dumps(data, ensure_ascii=False)
        # XADD 自动生成 ID（毫秒时间戳-序号）
        await client.xadd(_stream_key(run_id), {"event": event, "data": payload})
        # 唤醒等待中的订阅者
        await client.publish(_notify_key(run_id), "")

    async def publish_end(self, run_id: str) -> None:
        """写入结束标记事件，并通知订阅者。"""
        client = await self._get_client()
        await client.xadd(_stream_key(run_id), {"event": _END_EVENT, "data": ""})
        await client.publish(_notify_key(run_id), "")

    async def subscribe(
        self,
        run_id: str,
        *,
        last_event_id: str | None = None,
        heartbeat_interval: float = 15.0,
    ) -> AsyncIterator[StreamEvent]:
        """异步迭代器，按序 yield 该 run 的所有事件。

        - 若提供 last_event_id，先从该 ID 之后回放历史事件。
        - 之后通过 Pub/Sub 实时监听新事件。
        - 超时无事件时 yield HEARTBEAT_SENTINEL。
        - 收到 __end__ 事件时 yield END_SENTINEL 并退出。
        """
        client = await self._get_client()
        # 订阅 Pub/Sub 通知（独立连接，不能复用共享客户端）
        pubsub: PubSub = client.pubsub()
        await pubsub.subscribe(_notify_key(run_id))

        try:
            # 确定回放起始 ID
            # Redis XRANGE 的起始 ID："-" 表示最早，"(id" 表示不含该 id
            start_id = "-" if last_event_id is None else f"({last_event_id}"

            # 先回放历史
            async for event in self._replay(client, run_id, start_id):
                if event is END_SENTINEL:
                    yield END_SENTINEL
                    return
                yield event
                start_id = f"({event.id}"

            # 实时监听
            async for event in self._listen(client, pubsub, run_id, start_id, heartbeat_interval):
                if event is END_SENTINEL:
                    yield END_SENTINEL
                    return
                if event is HEARTBEAT_SENTINEL:
                    yield HEARTBEAT_SENTINEL
                else:
                    yield event
                    start_id = f"({event.id}"
        finally:
            try:
                await pubsub.unsubscribe(_notify_key(run_id))
                await pubsub.aclose()
            except Exception:
                pass

    async def _replay(
        self,
        client: aioredis.Redis,
        run_id: str,
        start_id: str,
    ) -> AsyncIterator[StreamEvent]:
        """从 Redis Stream 回放 start_id 之后的所有历史事件。"""
        entries = await client.xrange(_stream_key(run_id), min=start_id, max="+")
        for entry_id, fields in entries:
            event_name = fields.get("event", "")
            if event_name == _END_EVENT:
                yield END_SENTINEL
                return
            data = _parse_data(fields.get("data", ""))
            yield StreamEvent(id=entry_id, event=event_name, data=data)

    async def _listen(
        self,
        client: aioredis.Redis,
        pubsub: PubSub,
        run_id: str,
        start_id: str,
        heartbeat_interval: float,
    ) -> AsyncIterator[StreamEvent]:
        """通过 Pub/Sub 实时监听新事件，收到通知后用 XREAD 拉取。

        注意：XREAD 的 ID 语义与 XRANGE 不同：
        - 不支持 "-"，从头读用 "0-0"
        - 本身就是排他的（返回 ID > 给定值），不需要 "(id" 前缀
        """
        # 将 XRANGE 风格的 start_id 转换为 XREAD 兼容格式
        if start_id == "-":
            current_start = "0-0"
        elif start_id.startswith("("):
            current_start = start_id[1:]
        else:
            current_start = start_id
        while True:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=heartbeat_interval),
                    timeout=heartbeat_interval + 1,
                )
            except TimeoutError:
                yield HEARTBEAT_SENTINEL
                continue

            if message is None:
                # 超时但没有消息，发送心跳
                yield HEARTBEAT_SENTINEL
                continue

            # 收到通知，用 XREAD 拉取新事件
            results = await client.xread({_stream_key(run_id): current_start}, count=100)
            if not results:
                continue

            for _key, entries in results:
                for entry_id, fields in entries:
                    event_name = fields.get("event", "")
                    if event_name == _END_EVENT:
                        yield END_SENTINEL
                        return
                    data = _parse_data(fields.get("data", ""))
                    yield StreamEvent(id=entry_id, event=event_name, data=data)
                    current_start = entry_id  # XREAD 用原始 ID，不加 "(" 前缀

    async def cleanup(self, run_id: str, *, delay: float = 0) -> None:
        """删除该 run 的 Redis Stream key，可选延迟。"""
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            client = await self._get_client()
            await client.delete(_stream_key(run_id))
        except Exception as exc:
            logger.warning("cleanup run %s 失败: %s", run_id, exc)

    async def close(self) -> None:
        """关闭共享 Redis 连接。"""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None


def _parse_data(raw: str) -> Any:
    """将 JSON 字符串解析为 Python 对象，解析失败时原样返回。"""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
