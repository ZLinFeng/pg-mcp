"""Unit tests for observability: tracing context and Prometheus metrics."""

import logging
import uuid

import pytest

from pg_mcp.observability.metrics import MetricsCollector
from pg_mcp.observability.tracing import (
    TraceContext,
    clear_request_id,
    generate_request_id,
    get_request_id,
    request_context,
    set_request_id,
    trace_async,
    trace_sync,
)


class TestTracingContext:
    """Tests for request ID generation and context propagation."""

    def test_generate_request_id_is_uuid(self) -> None:
        """Generated request IDs are valid UUIDs."""
        request_id = generate_request_id()
        uuid.UUID(request_id)  # Should not raise

    def test_generate_request_id_unique(self) -> None:
        """Two generated IDs differ."""
        assert generate_request_id() != generate_request_id()

    def test_set_and_get_request_id(self) -> None:
        """set/get roundtrip; clear resets the context."""
        try:
            set_request_id("req-123")
            assert get_request_id() == "req-123"
        finally:
            clear_request_id()
        assert get_request_id() is None

    @pytest.mark.asyncio
    async def test_request_context_sets_and_cleans_up(self) -> None:
        """request_context sets the ID during the block and clears it after."""
        assert get_request_id() is None

        async with request_context("ctx-42") as request_id:
            assert request_id == "ctx-42"
            assert get_request_id() == "ctx-42"

        assert get_request_id() is None

    @pytest.mark.asyncio
    async def test_request_context_generates_id_when_missing(self) -> None:
        """request_context without an explicit ID generates one."""
        async with request_context() as request_id:
            assert request_id
            assert get_request_id() == request_id

    @pytest.mark.asyncio
    async def test_nested_context_restores_previous_id(self) -> None:
        """Nested contexts restore the enclosing request ID on exit."""
        async with request_context("outer"):
            async with request_context("inner"):
                assert get_request_id() == "inner"
            assert get_request_id() == "outer"
        assert get_request_id() is None

    def test_trace_context_model(self) -> None:
        """TraceContext serializes request metadata."""
        ctx = TraceContext(request_id="r1", operation="generate_sql", metadata={"database": "db1"})
        dumped = ctx.model_dump()
        assert dumped["request_id"] == "r1"
        assert dumped["operation"] == "generate_sql"
        assert dumped["metadata"]["database"] == "db1"


class TestMetricsCollector:
    """Tests for the Prometheus metrics collector."""

    @staticmethod
    def counter_value(counter, **labels) -> float:
        """Read the current value of a labeled counter child."""
        return counter.labels(**labels)._value.get()

    def test_singleton_instance(self) -> None:
        """MetricsCollector behaves as a singleton."""
        assert MetricsCollector() is MetricsCollector()

    def test_increment_query_request(self) -> None:
        """Query request counter increases per status/database."""
        collector = MetricsCollector()
        labels = {"status": "success", "database": "metrics_test_db"}
        before = self.counter_value(collector.query_requests, **labels)

        collector.increment_query_request(status="success", database="metrics_test_db")

        assert self.counter_value(collector.query_requests, **labels) == before + 1

    def test_increment_llm_call(self) -> None:
        """LLM call counter increases per operation."""
        collector = MetricsCollector()
        labels = {"operation": "metrics_test_op"}
        before = self.counter_value(collector.llm_calls, **labels)

        collector.increment_llm_call(operation="metrics_test_op")

        assert self.counter_value(collector.llm_calls, **labels) == before + 1

    def test_observe_llm_latency(self) -> None:
        """LLM latency observations accumulate."""
        collector = MetricsCollector()
        labels = {"operation": "metrics_test_op"}
        before = collector.llm_latency.labels(**labels)._sum.get()

        collector.observe_llm_latency(operation="metrics_test_op", duration=1.5)

        assert collector.llm_latency.labels(**labels)._sum.get() == before + 1.5

    def test_increment_llm_tokens(self) -> None:
        """Token counter accumulates usage."""
        collector = MetricsCollector()
        labels = {"operation": "metrics_test_op"}
        before = self.counter_value(collector.llm_tokens_used, **labels)

        collector.increment_llm_tokens(operation="metrics_test_op", tokens=100)

        assert self.counter_value(collector.llm_tokens_used, **labels) == before + 100

    def test_increment_sql_rejected(self) -> None:
        """Rejected SQL counter increases per reason."""
        collector = MetricsCollector()
        labels = {"reason": "metrics_test_reason"}
        before = self.counter_value(collector.sql_rejected, **labels)

        collector.increment_sql_rejected(reason="metrics_test_reason")

        assert self.counter_value(collector.sql_rejected, **labels) == before + 1

    def test_observe_db_query_duration(self) -> None:
        """Database query durations accumulate in the histogram."""
        collector = MetricsCollector()
        before_sum = collector.db_query_duration._sum.get()

        collector.observe_db_query_duration(duration=0.25)

        assert collector.db_query_duration._sum.get() >= before_sum + 0.25

    def test_set_schema_cache_age(self) -> None:
        """Schema cache gauge is set per database."""
        collector = MetricsCollector()
        collector.set_schema_cache_age(database="metrics_test_db", age_seconds=12.5)

        value = collector.schema_cache_age.labels(database="metrics_test_db")._value.get()
        assert value == 12.5

    def test_set_db_connections_active(self) -> None:
        """Active connection gauge is set per database."""
        collector = MetricsCollector()
        collector.set_db_connections_active(database="metrics_test_db", count=7)

        value = collector.db_connections_active.labels(database="metrics_test_db")._value.get()
        assert value == 7


class TestTracingLogger:
    """Tests for the tracing-aware logger wrapper."""

    @pytest.mark.asyncio
    async def test_request_id_attached_to_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """Logs emitted inside a request context carry the request_id."""
        from pg_mcp.observability.tracing import get_tracing_logger

        logger = get_tracing_logger("test.tracing")

        with caplog.at_level(logging.INFO, logger="test.tracing"):
            async with request_context("log-req-1"):
                logger.info("hello from request")

        assert caplog.records
        assert caplog.records[-1].request_id == "log-req-1"


class TestTraceDecorators:
    """Tests for the trace_async/trace_sync decorators."""

    @pytest.mark.asyncio
    async def test_trace_async_injects_request_context(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Records logged inside a traced async call carry id and operation."""

        @trace_async(operation="generate_sql")
        async def work(question: str) -> str:
            logging.getLogger("test.trace_async").info("working")
            return question

        old_factory = logging.getLogRecordFactory()

        with caplog.at_level(logging.INFO, logger="test.trace_async"):
            async with request_context("tr-1"):
                result = await work("count users")

        assert result == "count users"
        # Original record factory is restored after the call
        assert logging.getLogRecordFactory() is old_factory
        assert caplog.records[-1].request_id == "tr-1"
        assert caplog.records[-1].operation == "generate_sql"

    @pytest.mark.asyncio
    async def test_trace_async_without_context_plain_execution(self) -> None:
        """Without a request context the function just runs normally."""

        @trace_async()
        async def work() -> str:
            return "done"

        old_factory = logging.getLogRecordFactory()

        assert await work() == "done"
        assert logging.getLogRecordFactory() is old_factory

    @pytest.mark.asyncio
    async def test_trace_sync_injects_request_context(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Records logged inside a traced sync call carry id and operation."""

        @trace_sync(operation="validate_sql")
        def work(sql: str) -> bool:
            logging.getLogger("test.trace_sync").info("validating")
            return bool(sql)

        old_factory = logging.getLogRecordFactory()

        with caplog.at_level(logging.INFO, logger="test.trace_sync"):
            async with request_context("tr-2"):
                assert work("SELECT 1") is True

        assert logging.getLogRecordFactory() is old_factory
        assert caplog.records[-1].request_id == "tr-2"
        assert caplog.records[-1].operation == "validate_sql"

    def test_trace_sync_without_context_plain_execution(self) -> None:
        """Sync tracing without context leaves the factory untouched."""

        @trace_sync()
        def work() -> int:
            return 7

        old_factory = logging.getLogRecordFactory()

        assert work() == 7
        assert logging.getLogRecordFactory() is old_factory
