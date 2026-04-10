"""RedisRunManager 单元测试（使用 fakeredis 模拟 Redis）。"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis as fake_aioredis
import pytest

from deerflow.runtime.runs.manager import ConflictError
from deerflow.runtime.runs.redis_manager import RedisRunManager
from deerflow.runtime.runs.schemas import RunStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis():
    return fake_aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def manager(fake_redis):
    m = RedisRunManager(redis_url="redis://localhost:6379/0")
    m._client = fake_redis
    return m


# ---------------------------------------------------------------------------
# create / get
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_and_get(manager):
    """创建的 run 应可通过 get 取回。"""
    record = await manager.create("thread-1", "lead_agent", metadata={"k": "v"})
    assert record.status == RunStatus.pending
    assert record.thread_id == "thread-1"
    assert record.assistant_id == "lead_agent"
    assert record.metadata == {"k": "v"}

    fetched = manager.get(record.run_id)
    assert fetched is record


@pytest.mark.anyio
async def test_create_persists_to_redis(manager, fake_redis):
    """create 应将 run 状态写入 Redis Hash。"""
    record = await manager.create("thread-1")
    data = await fake_redis.hgetall(f"run:{record.run_id}")
    assert data["status"] == "pending"
    assert data["thread_id"] == "thread-1"


# ---------------------------------------------------------------------------
# set_status
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_set_status_transitions(manager):
    """状态应能从 pending 转换到 running 再到 success。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    assert record.status == RunStatus.running

    await manager.set_status(record.run_id, RunStatus.success)
    assert record.status == RunStatus.success


@pytest.mark.anyio
async def test_set_status_with_error(manager):
    """error 状态应保存错误信息。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.error, error="boom")
    assert record.error == "boom"


@pytest.mark.anyio
async def test_terminal_status_sets_ttl(manager, fake_redis):
    """终态 run 应在 Redis 中设置 TTL。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.success)
    ttl = await fake_redis.ttl(f"run:{record.run_id}")
    assert ttl > 0


@pytest.mark.anyio
async def test_custom_ttl_from_config_applies_to_expire(fake_redis):
    """自定义 TTL 参数应生效到 run key 过期时间。"""
    manager = RedisRunManager(
        redis_url="redis://localhost:6379/0",
        terminal_ttl_seconds=123,
        inflight_ttl_seconds=234,
    )
    manager._client = fake_redis

    record = await manager.create("thread-1")
    run_ttl = await fake_redis.ttl(f"run:{record.run_id}")
    thread_ttl = await fake_redis.ttl("thread:runs:thread-1")
    assert 0 < run_ttl <= 234
    assert 0 < thread_ttl <= 234

    await manager.set_status(record.run_id, RunStatus.success)
    terminal_ttl = await fake_redis.ttl(f"run:{record.run_id}")
    assert 0 < terminal_ttl <= 123


# ---------------------------------------------------------------------------
# cancel（本地）
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_local_run(manager):
    """取消本地 run 应设置 abort_event 并转换到 interrupted 状态。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    result = await manager.cancel(record.run_id)
    assert result is True
    assert record.abort_event.is_set()
    assert record.status == RunStatus.interrupted


@pytest.mark.anyio
async def test_cancel_completed_run_returns_false(manager):
    """取消已完成的 run 应返回 False。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.success)

    result = await manager.cancel(record.run_id)
    assert result is False


# ---------------------------------------------------------------------------
# list_by_thread / has_inflight
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_by_thread(manager):
    """list_by_thread 应返回该 thread 的所有 run，按创建时间倒序。"""
    r1 = await manager.create("thread-1")
    r2 = await manager.create("thread-1")
    await manager.create("thread-2")

    runs = await manager.list_by_thread("thread-1")
    assert len(runs) == 2
    run_ids = [r.run_id for r in runs]
    assert r2.run_id in run_ids
    assert r1.run_id in run_ids


@pytest.mark.anyio
async def test_has_inflight_true_when_pending(manager):
    """pending run 存在时 has_inflight 应返回 True。"""
    await manager.create("thread-1")
    assert await manager.has_inflight("thread-1") is True


@pytest.mark.anyio
async def test_has_inflight_false_after_success(manager):
    """run 完成后 has_inflight 应返回 False。"""
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.success)
    assert await manager.has_inflight("thread-1") is False


# ---------------------------------------------------------------------------
# create_or_reject
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_or_reject_raises_on_conflict(manager):
    """reject 策略下，thread 有 inflight run 时应抛出 ConflictError。"""
    await manager.create("thread-1")
    with pytest.raises(ConflictError):
        await manager.create_or_reject("thread-1", multitask_strategy="reject")


@pytest.mark.anyio
async def test_create_or_reject_interrupt_cancels_inflight(manager):
    """interrupt 策略下，应取消 inflight run 并创建新 run。"""
    r1 = await manager.create("thread-1")
    await manager.set_status(r1.run_id, RunStatus.running)

    r2 = await manager.create_or_reject("thread-1", multitask_strategy="interrupt")
    assert r2.run_id != r1.run_id
    assert r1.status == RunStatus.interrupted


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cleanup_removes_local_record(manager):
    """cleanup 应从本地内存中移除 run 记录。"""
    record = await manager.create("thread-1")
    run_id = record.run_id

    await manager.cleanup(run_id, delay=0)
    assert manager.get(run_id) is None
