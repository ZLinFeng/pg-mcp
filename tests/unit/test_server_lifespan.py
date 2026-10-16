"""Unit tests for the server lifespan and MCP query tool (with mocked I/O)."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pg_mcp.server as server_module
from pg_mcp.config.settings import DatabaseConfig, OpenAIConfig, Settings
from pg_mcp.models.query import QueryResponse, ReturnType
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.server import lifespan, mcp, query


@pytest.fixture
def restore_globals() -> Any:
    """Snapshot and restore the server module globals."""
    saved = (
        server_module._settings,
        server_module._pools,
        server_module._schema_cache,
        server_module._orchestrator,
        server_module._metrics,
        server_module._rate_limiter,
    )
    yield
    (
        server_module._settings,
        server_module._pools,
        server_module._schema_cache,
        server_module._orchestrator,
        server_module._metrics,
        server_module._rate_limiter,
    ) = saved


def make_settings(databases: list[DatabaseConfig] | None = None) -> Settings:
    """Build a Settings instance that does not read ambient environment."""
    kwargs: dict[str, Any] = {"openai": OpenAIConfig(api_key="sk-test")}
    if databases is not None:
        kwargs["databases"] = databases
    return Settings(**kwargs)


def single_db_settings(name: str = "test_db") -> Settings:
    """Settings whose primary database is named *name*."""
    return Settings(
        openai=OpenAIConfig(api_key="sk-test"),
        database=DatabaseConfig(name=name, host="localhost"),
    )


def make_cache_mock() -> MagicMock:
    """Create a SchemaCache mock whose load() returns an empty schema."""
    cache = MagicMock()
    cache.load = AsyncMock(
        return_value=DatabaseSchema(database_name="test_db", version="16")
    )
    cache.stop_auto_refresh = AsyncMock()
    return cache


class TestLifespan:
    """Tests for lifespan startup and shutdown wiring."""

    @pytest.mark.asyncio
    async def test_startup_builds_per_database_components(
        self, restore_globals: None
    ) -> None:
        """Lifespan builds pools, validators and executors for each database."""
        settings = single_db_settings()
        pool = MagicMock()
        cache = make_cache_mock()

        with (
            patch("pg_mcp.server.Settings", return_value=settings),
            patch("pg_mcp.server.configure_logging"),
            patch(
                "pg_mcp.server.create_pools", new=AsyncMock(return_value={"test_db": pool})
            ),
            patch("pg_mcp.server.SchemaCache", return_value=cache),
            patch("pg_mcp.server.MetricsCollector", return_value=MagicMock()),
            patch("pg_mcp.server.close_pools", new=AsyncMock()) as close_pools,
        ):
            async with lifespan(mcp):
                orchestrator = server_module._orchestrator
                assert orchestrator is not None
                assert set(orchestrator.sql_executors) == {"test_db"}
                assert set(orchestrator.sql_validators) == {"test_db"}
                assert orchestrator.pools == {"test_db": pool}
                assert server_module._rate_limiter is not None

            # Shutdown closes pools and stops the schema cache
            close_pools.assert_awaited_once()
            cache.stop_auto_refresh.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_startup_multi_database_wiring(
        self, restore_globals: None
    ) -> None:
        """Multiple databases each get their own executor and validator."""
        settings = make_settings(
            databases=[
                DatabaseConfig(name="db1", host="h1"),
                DatabaseConfig(name="db2", host="h2"),
            ]
        )
        pools = {"db1": MagicMock(), "db2": MagicMock()}
        cache = make_cache_mock()

        with (
            patch("pg_mcp.server.Settings", return_value=settings),
            patch("pg_mcp.server.configure_logging"),
            patch("pg_mcp.server.create_pools", new=AsyncMock(return_value=pools)),
            patch("pg_mcp.server.SchemaCache", return_value=cache),
            patch("pg_mcp.server.MetricsCollector", return_value=MagicMock()),
            patch("pg_mcp.server.close_pools", new=AsyncMock()),
        ):
            async with lifespan(mcp):
                orchestrator = server_module._orchestrator
                assert orchestrator is not None
                assert set(orchestrator.sql_executors) == {"db1", "db2"}
                assert set(orchestrator.sql_validators) == {"db1", "db2"}
                assert orchestrator.sql_executors["db1"].pool is pools["db1"]
                assert orchestrator.sql_executors["db2"].pool is pools["db2"]
                # Schemas were loaded for both databases
                assert cache.load.await_count == 2

    @pytest.mark.asyncio
    async def test_shutdown_tolerates_cache_errors(
        self, restore_globals: None
    ) -> None:
        """Errors during shutdown are logged, not raised."""
        settings = single_db_settings()
        cache = make_cache_mock()
        cache.stop_auto_refresh = AsyncMock(side_effect=RuntimeError("refresh stuck"))

        with (
            patch("pg_mcp.server.Settings", return_value=settings),
            patch("pg_mcp.server.configure_logging"),
            patch(
                "pg_mcp.server.create_pools", new=AsyncMock(return_value={"test_db": MagicMock()})
            ),
            patch("pg_mcp.server.SchemaCache", return_value=cache),
            patch("pg_mcp.server.MetricsCollector", return_value=MagicMock()),
            patch(
                "pg_mcp.server.close_pools", new=AsyncMock(side_effect=RuntimeError("closed?"))
            ),
        ):
            # Must not raise despite both shutdown steps failing
            async with lifespan(mcp):
                pass


class TestQueryTool:
    """Tests for the MCP query tool entry point."""

    @pytest.mark.asyncio
    async def test_server_not_initialized(self, restore_globals: None) -> None:
        """A missing orchestrator produces a structured error."""
        server_module._orchestrator = None

        result = await query(question="Count users")

        assert result["success"] is False
        assert result["error"]["code"] == "SERVER_NOT_INITIALIZED"

    @pytest.mark.asyncio
    async def test_invalid_return_type(self, restore_globals: None) -> None:
        """Unknown return_type values are rejected."""
        server_module._orchestrator = MagicMock()

        result = await query(question="Count users", return_type="csv")

        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_PARAMETER"

    @pytest.mark.asyncio
    async def test_invalid_request_parameters(self, restore_globals: None) -> None:
        """Invalid question input is rejected before reaching the orchestrator."""
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock()
        server_module._orchestrator = orchestrator

        result = await query(question="   ")

        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_REQUEST"
        orchestrator.execute_query.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_delegates_to_orchestrator(self, restore_globals: None) -> None:
        """The tool forwards the request and serializes the response."""
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock(
            return_value=QueryResponse(
                success=True,
                generated_sql="SELECT 1;",
                tokens_used=12,
                request_id="req-1",
            )
        )
        server_module._orchestrator = orchestrator

        result = await query(question="Count users", database="db2", return_type="sql")

        assert result["success"] is True
        assert result["generated_sql"] == "SELECT 1;"
        assert result["tokens_used"] == 12
        assert result["request_id"] == "req-1"

        request = orchestrator.execute_query.await_args.args[0]
        assert request.question == "Count users"
        assert request.database == "db2"
        assert request.return_type is ReturnType.SQL

    @pytest.mark.asyncio
    async def test_orchestrator_exception_is_captured(self, restore_globals: None) -> None:
        """Unexpected orchestrator failures become INTERNAL_ERROR responses."""
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock(side_effect=RuntimeError("boom"))
        server_module._orchestrator = orchestrator

        result = await query(question="Count users")

        assert result["success"] is False
        assert result["error"]["code"] == "INTERNAL_ERROR"
        assert result["tokens_used"] == 0
