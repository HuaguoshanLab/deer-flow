"""Store async factory 单元测试。"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deerflow.config.checkpointer_config import CheckpointerConfig


@pytest.mark.anyio
async def test_make_store_memory():
    """checkpointer type=memory 时应返回 InMemoryStore。"""
    from langgraph.store.memory import InMemoryStore

    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = CheckpointerConfig(type="memory")

    with patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config):
        async with make_store() as store:
            assert isinstance(store, InMemoryStore)


@pytest.mark.anyio
async def test_make_store_no_checkpointer_config():
    """未配置 checkpointer 时应返回 InMemoryStore 并打印 WARNING。"""
    from langgraph.store.memory import InMemoryStore

    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = None

    with patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config):
        async with make_store() as store:
            assert isinstance(store, InMemoryStore)


@pytest.mark.anyio
async def test_make_store_sqlite():
    """checkpointer type=sqlite 时应调用 AsyncSqliteStore.from_conn_string 并执行 setup。"""
    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = CheckpointerConfig(type="sqlite", connection_string="/tmp/test_store.db")

    mock_store = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_store
    mock_cm.__aexit__.return_value = False

    mock_store_cls = MagicMock()
    mock_store_cls.from_conn_string.return_value = mock_cm

    mock_module = MagicMock()
    mock_module.AsyncSqliteStore = mock_store_cls

    with (
        patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config),
        patch.dict(sys.modules, {"langgraph.store.sqlite.aio": mock_module}),
        patch("deerflow.runtime.store.async_provider.resolve_sqlite_conn_str", return_value="/tmp/test_store.db"),
        patch("deerflow.runtime.store.async_provider.ensure_sqlite_parent_dir"),
    ):
        async with make_store() as store:
            assert store is mock_store

    mock_store_cls.from_conn_string.assert_called_once_with("/tmp/test_store.db")
    mock_store.setup.assert_awaited_once()


@pytest.mark.anyio
async def test_make_store_postgres():
    """checkpointer type=postgres 时应调用 AsyncPostgresStore.from_conn_string 并执行 setup。"""
    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = CheckpointerConfig(type="postgres", connection_string="postgresql://localhost/db")

    mock_store = AsyncMock()
    mock_cm = AsyncMock()
    mock_cm.__aenter__.return_value = mock_store
    mock_cm.__aexit__.return_value = False

    mock_store_cls = MagicMock()
    mock_store_cls.from_conn_string.return_value = mock_cm

    mock_module = MagicMock()
    mock_module.AsyncPostgresStore = mock_store_cls

    with (
        patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config),
        patch.dict(sys.modules, {"langgraph.store.postgres.aio": mock_module}),
    ):
        async with make_store() as store:
            assert store is mock_store

    mock_store_cls.from_conn_string.assert_called_once_with("postgresql://localhost/db")
    mock_store.setup.assert_awaited_once()


@pytest.mark.anyio
async def test_make_store_postgres_raises_when_connection_string_missing():
    """postgres 类型缺少 connection_string 时应抛出 ValueError。"""
    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = CheckpointerConfig(type="postgres")

    mock_module = MagicMock()
    mock_module.AsyncPostgresStore = MagicMock()

    with (
        patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config),
        patch.dict(sys.modules, {"langgraph.store.postgres.aio": mock_module}),
    ):
        with pytest.raises(ValueError, match="connection_string is required"):
            async with make_store():
                pass


@pytest.mark.anyio
async def test_make_store_postgres_raises_when_package_missing():
    """postgres 包未安装时应抛出 ImportError 并提示安装命令。"""
    from deerflow.runtime.store.async_provider import make_store

    mock_config = MagicMock()
    mock_config.checkpointer = CheckpointerConfig(type="postgres", connection_string="postgresql://localhost/db")

    with (
        patch("deerflow.runtime.store.async_provider.get_app_config", return_value=mock_config),
        patch.dict(sys.modules, {"langgraph.store.postgres.aio": None}),
    ):
        with pytest.raises(ImportError, match="langgraph-checkpoint-postgres"):
            async with make_store():
                pass
