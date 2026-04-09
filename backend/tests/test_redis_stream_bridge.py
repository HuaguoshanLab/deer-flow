"""RedisStreamBridge 单元测试（使用 fakeredis 模拟 Redis）。"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis as fake_aioredis
import pytest

from deerflow.runtime.stream_bridge.base import END_SENTINEL, HEARTBEAT_SENTINEL, StreamEvent
from deerflow.runtime.stream_bridge.redis_bridge import RedisStreamBridge


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis():
    """返回一个 fakeredis 异步客户端实例。"""
    return fake_aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def bridge(fake_redis):
    """返回注入了 fakeredis 的 RedisStreamBridge。"""
    b = RedisStreamBridge(redis_url="redis://localhost:6379/0")
    b._client = fake_redis
    return b


# ---------------------------------------------------------------------------
# publish / publish_end
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_publish_writes_to_stream(bridge, fake_redis):
    """publish 应将事件写入 Redis Stream。"""
    await bridge.publish("run-1", "metadata", {"run_id": "run-1"})

    entries = await fake_redis.xrange("run:events:run-1", min="-", max="+")
    assert len(entries) == 1
    _id, fields = entries[0]
    assert fields["event"] == "metadata"


@pytest.mark.anyio
async def test_publish_end_writes_end_event(bridge, fake_redis):
    """publish_end 应写入 __end__ 事件。"""
    await bridge.publish_end("run-1")

    entries = await fake_redis.xrange("run:events:run-1", min="-", max="+")
    assert len(entries) == 1
    _id, fields = entries[0]
    assert fields["event"] == "__end__"


# ---------------------------------------------------------------------------
# subscribe（回放历史）
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_subscribe_replays_history(bridge):
    """subscribe 应先回放历史事件，再收到 END_SENTINEL。"""
    run_id = "run-replay"
    await bridge.publish(run_id, "metadata", {"run_id": run_id})
    await bridge.publish(run_id, "values", {"step": 1})
    await bridge.publish_end(run_id)

    received: list[StreamEvent] = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(received) == 3
    assert received[0].event == "metadata"
    assert received[1].event == "values"
    assert received[2] is END_SENTINEL


@pytest.mark.anyio
async def test_subscribe_with_last_event_id(bridge):
    """提供 last_event_id 时，应从该 ID 之后回放。"""
    run_id = "run-last-id"
    await bridge.publish(run_id, "e1", {"n": 1})
    await bridge.publish(run_id, "e2", {"n": 2})
    await bridge.publish(run_id, "e3", {"n": 3})
    await bridge.publish_end(run_id)

    # 先获取第一个事件的 ID
    first_pass: list[StreamEvent] = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        first_pass.append(entry)
        if entry is END_SENTINEL:
            break

    first_id = first_pass[0].id

    # 从第一个事件之后重新订阅
    received: list[StreamEvent] = []
    async for entry in bridge.subscribe(run_id, last_event_id=first_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert [e.event for e in received[:-1]] == ["e2", "e3"]
    assert received[-1] is END_SENTINEL


@pytest.mark.anyio
async def test_subscribe_empty_run_yields_end(bridge):
    """没有事件直接 publish_end，订阅者应立即收到 END_SENTINEL。"""
    run_id = "run-empty"
    await bridge.publish_end(run_id)

    received: list[StreamEvent] = []
    async for entry in bridge.subscribe(run_id, heartbeat_interval=1.0):
        received.append(entry)
        if entry is END_SENTINEL:
            break

    assert len(received) == 1
    assert received[0] is END_SENTINEL


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cleanup_deletes_stream_key(bridge, fake_redis):
    """cleanup 应删除 Redis Stream key。"""
    run_id = "run-cleanup"
    await bridge.publish(run_id, "test", {})
    assert await fake_redis.exists("run:events:run-1") >= 0

    await bridge.cleanup(run_id, delay=0)
    assert await fake_redis.exists(f"run:events:{run_id}") == 0


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_close_clears_client(bridge):
    """close 应关闭 Redis 连接并清空 _client。"""
    assert bridge._client is not None
    await bridge.close()
    assert bridge._client is None


# ---------------------------------------------------------------------------
# make_stream_bridge factory
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_make_stream_bridge_redis():
    """make_stream_bridge 在 redis 配置下应返回 RedisStreamBridge。"""
    from deerflow.config.stream_bridge_config import StreamBridgeConfig
    from deerflow.runtime.stream_bridge.async_provider import make_stream_bridge
    from deerflow.runtime.stream_bridge.redis_bridge import RedisStreamBridge

    config = StreamBridgeConfig(type="redis", redis_url="redis://localhost:6379/0")

    # 用 fakeredis 替换真实 Redis 连接
    fake = fake_aioredis.FakeRedis(decode_responses=True)
    with patch("redis.asyncio.from_url", return_value=fake):
        async with make_stream_bridge(config) as bridge:
            assert isinstance(bridge, RedisStreamBridge)
