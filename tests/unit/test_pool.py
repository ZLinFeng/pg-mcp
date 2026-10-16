"""Unit tests for connection pool helpers."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config.settings import DatabaseConfig
from pg_mcp.db.pool import close_pools, create_pool, create_pools


class TestCreatePool:
    """Tests for create_pool."""

    @pytest.mark.asyncio
    async def test_passes_config_to_asyncpg(self) -> None:
        """Connection and pool settings are forwarded to asyncpg."""
        pool = MagicMock()
        with patch(
            "pg_mcp.db.pool.asyncpg.create_pool", new=AsyncMock(return_value=pool)
        ) as create:
            config = DatabaseConfig(
                host="db.host",
                port=5433,
                name="db1",
                user="alice",
                password="secret",
                min_pool_size=2,
                max_pool_size=9,
            )
            result = await create_pool(config)

        assert result is pool
        kwargs = create.await_args.kwargs
        assert kwargs["host"] == "db.host"
        assert kwargs["port"] == 5433
        assert kwargs["database"] == "db1"
        assert kwargs["user"] == "alice"
        assert kwargs["password"] == "secret"
        assert kwargs["min_size"] == 2
        assert kwargs["max_size"] == 9

    @pytest.mark.asyncio
    async def test_none_result_raises(self) -> None:
        """A None pool from asyncpg is converted to a RuntimeError."""
        with (
            patch("pg_mcp.db.pool.asyncpg.create_pool", new=AsyncMock(return_value=None)),
            pytest.raises(RuntimeError, match="Failed to create connection pool"),
        ):
            await create_pool(DatabaseConfig(name="db1"))


class TestCreatePools:
    """Tests for create_pools."""

    @pytest.mark.asyncio
    async def test_creates_one_pool_per_config(self) -> None:
        """Pools are keyed by database name."""
        configs = [
            DatabaseConfig(name="db1", host="h1"),
            DatabaseConfig(name="db2", host="h2"),
        ]
        fake = {"db1": MagicMock(), "db2": MagicMock()}

        async def fake_create(config: DatabaseConfig) -> MagicMock:
            return fake[config.name]

        with patch("pg_mcp.db.pool.create_pool", new=AsyncMock(side_effect=fake_create)):
            pools = await create_pools(configs)

        assert set(pools) == {"db1", "db2"}
        assert pools["db1"] is fake["db1"]
        assert pools["db2"] is fake["db2"]

    @pytest.mark.asyncio
    async def test_empty_config_returns_empty_dict(self) -> None:
        """No configurations means no pools."""
        assert await create_pools([]) == {}


class TestClosePools:
    """Tests for close_pools."""

    @pytest.mark.asyncio
    async def test_graceful_close(self) -> None:
        """Healthy pools are closed without forced termination."""
        pool = MagicMock()
        pool.close = AsyncMock()

        await close_pools({"db1": pool}, timeout=1.0)

        pool.close.assert_awaited_once()
        pool.terminate.assert_not_called()

    @pytest.mark.asyncio
    async def test_timeout_forces_termination(self) -> None:
        """Pools that take too long to close are terminated."""
        pool = MagicMock()

        async def slow_close() -> None:
            await asyncio.sleep(5)

        pool.close = slow_close

        await close_pools({"db1": pool}, timeout=0.01)

        pool.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_error_forces_termination(self) -> None:
        """Errors during close force termination but do not propagate."""
        pool = MagicMock()
        pool.close = AsyncMock(side_effect=RuntimeError("boom"))

        await close_pools({"db1": pool}, timeout=1.0)

        pool.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_one_pool_failure_does_not_stop_others(self) -> None:
        """A failing pool does not prevent closing the remaining pools."""
        bad = MagicMock()
        bad.close = AsyncMock(side_effect=RuntimeError("boom"))
        good = MagicMock()
        good.close = AsyncMock()

        await close_pools({"bad": bad, "good": good}, timeout=1.0)

        bad.terminate.assert_called_once()
        good.close.assert_awaited_once()
        good.terminate.assert_not_called()
