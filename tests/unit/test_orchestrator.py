"""Unit tests for QueryOrchestrator.

This module tests the orchestrator's coordination of the query pipeline,
including retry logic, error handling, and integration with all components.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    LLMError,
    LLMUnavailableError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    QueryRequest,
    ResultValidationResult,
    ReturnType,
)
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.observability.tracing import get_request_id
from pg_mcp.resilience.circuit_breaker import CircuitState
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.services.orchestrator import QueryOrchestrator


@pytest.fixture
def users_schema() -> DatabaseSchema:
    """Shared schema fixture with a single users table."""
    return DatabaseSchema(
        database_name="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnInfo(
                        name="id",
                        data_type="integer",
                        is_nullable=False,
                        is_primary_key=True,
                    ),
                ],
            )
        ],
        version="15.0",
    )


class TestDatabaseResolution:
    """Test database name resolution logic."""

    @pytest.fixture
    def mock_pools(self) -> dict[str, MagicMock]:
        """Create mock connection pools."""
        return {
            "db1": MagicMock(),
            "db2": MagicMock(),
        }

    @pytest.fixture
    def orchestrator(self, mock_pools: dict[str, MagicMock]) -> QueryOrchestrator:
        """Create orchestrator with mocked components."""
        return QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"db1": MagicMock(), "db2": MagicMock()},
            sql_executors={"db1": MagicMock(), "db2": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools=mock_pools,
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

    def test_resolve_database_specified_valid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified valid database."""
        result = orchestrator._resolve_database("db1")
        assert result == "db1"

    def test_resolve_database_specified_invalid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified but invalid database."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database("nonexistent")

        assert "not found" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]
        assert "db2" in exc_info.value.details["available_databases"]

    def test_resolve_database_auto_select_single(self) -> None:
        """Test auto-selecting when only one database available."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"only_db": MagicMock()},
            sql_executors={"only_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"only_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        result = orchestrator._resolve_database(None)
        assert result == "only_db"

    def test_resolve_database_auto_select_multiple_fails(
        self, orchestrator: QueryOrchestrator
    ) -> None:
        """Test that auto-select fails when multiple databases available."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "multiple databases" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]

    def test_resolve_database_no_databases(self) -> None:
        """Test error when no databases configured."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={},
            sql_executors={},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "no databases configured" in str(exc_info.value).lower()


class TestSQLGenerationWithRetry:
    """Test SQL generation with retry logic."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_generate_sql_success_first_attempt(self, mock_schema: DatabaseSchema) -> None:
        """Test successful SQL generation on first attempt."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None  # No exception = valid

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, validation_result, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
            validator=orchestrator.sql_validators["test_db"],
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert validation_result.is_valid is True
        assert validation_result.is_select is True
        mock_generator.generate.assert_called_once()
        mock_validator.validate_or_raise.assert_called_once_with("SELECT * FROM users;")

    @pytest.mark.asyncio
    async def test_generate_sql_retry_on_validation_failure(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Test retry logic when validation fails."""
        # Setup mocks - first attempt fails validation, second succeeds
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = [
            "SELECT * FROM user;",  # First attempt (wrong table name)
            "SELECT * FROM users;",  # Second attempt (correct)
        ]

        mock_validator = MagicMock()
        # First call raises error, second call succeeds
        mock_validator.validate_or_raise.side_effect = [
            SQLParseError('relation "user" does not exist'),
            None,  # Success on second attempt
        ]

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, validation_result, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
            validator=orchestrator.sql_validators["test_db"],
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert validation_result.is_valid is True
        assert mock_generator.generate.call_count == 2
        assert mock_validator.validate_or_raise.call_count == 2

        # Verify retry included error feedback
        second_call = mock_generator.generate.call_args_list[1]
        assert second_call.kwargs["previous_attempt"] == "SELECT * FROM user;"
        assert 'relation "user" does not exist' in second_call.kwargs["error_feedback"]

    @pytest.mark.asyncio
    async def test_generate_sql_fails_after_max_retries(self, mock_schema: DatabaseSchema) -> None:
        """Test failure after exhausting all retries."""
        # Setup mocks - all attempts fail validation
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "DELETE FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.side_effect = SecurityViolationError(
            "DELETE statements are not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2),
            validation_config=ValidationConfig(),
        )

        # Execute and verify exception
        with pytest.raises(SecurityViolationError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Delete all users",
                schema=mock_schema,
                request_id="test-123",
                validator=orchestrator.sql_validators["test_db"],
            )

        assert "DELETE statements are not allowed" in str(exc_info.value)
        # Should attempt max_retries + 1 times (initial + retries)
        assert mock_generator.generate.call_count == 3
        assert orchestrator.circuit_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_generate_sql_circuit_breaker_open(self, mock_schema: DatabaseSchema) -> None:
        """Test that open circuit breaker prevents SQL generation."""
        orchestrator = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(circuit_breaker_threshold=1),
            validation_config=ValidationConfig(),
        )

        # Manually open the circuit breaker
        orchestrator.circuit_breaker._state = CircuitState.OPEN
        orchestrator.circuit_breaker._failure_count = 5

        # Attempt should fail immediately
        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
                validator=orchestrator.sql_validators["test_db"],
            )

        assert "temporarily unavailable" in str(exc_info.value).lower()
        assert "circuit breaker" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_generate_sql_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors during generation."""
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
                validator=orchestrator.sql_validators["test_db"],
            )

        assert "unexpectedly" in str(exc_info.value).lower()
        assert orchestrator.circuit_breaker.failure_count == 1


class TestResultValidation:
    """Test result validation logic."""

    @pytest.mark.asyncio
    async def test_validate_results_success(self) -> None:
        """Test successful result validation."""
        mock_validator = AsyncMock()
        mock_validator.validate.return_value = ResultValidationResult(
            confidence=85,
            explanation="Results match the question well",
            suggestion=None,
            is_acceptable=True,
        )

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 85
        mock_validator.validate.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_results_disabled(self) -> None:
        """Test that validation is skipped when disabled."""
        mock_validator = AsyncMock()

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 100
        mock_validator.validate.assert_not_called()

    @pytest.mark.asyncio
    async def test_validate_results_failure_does_not_raise(self) -> None:
        """Test that validation failures don't raise exceptions."""
        mock_validator = AsyncMock()
        mock_validator.validate.side_effect = Exception("Validation failed")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Should not raise, returns default confidence
        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 100


class TestExecuteQueryFlow:
    """Test complete query execution flow."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_execute_query_sql_only(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=SQL."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT * FROM users;"
        assert response.validation is not None
        assert response.validation.is_valid is True
        assert response.data is None  # No execution for SQL-only
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_with_results(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=RESULT."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT id, name FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None

        mock_executor = AsyncMock()
        mock_executor.execute.return_value = (
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            2,  # total count
        )

        mock_result_validator = AsyncMock()
        mock_result_validator.validate.return_value = ResultValidationResult(
            confidence=90,
            explanation="Good results",
            suggestion=None,
            is_acceptable=True,
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": mock_executor},
            result_validator=mock_result_validator,
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT id, name FROM users;"
        assert response.data is not None
        assert response.data.row_count == 2
        assert len(response.data.rows) == 2
        assert response.data.columns == ["id", "name"]
        assert response.confidence == 90
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_schema_not_cached(self) -> None:
        """Test loading schema when not in cache."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None  # Not in cache
        mock_cache.load = AsyncMock(return_value=mock_schema)

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None

        mock_pool = MagicMock()

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": mock_pool},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify schema was loaded
        mock_cache.load.assert_called_once_with("test_db", mock_pool)
        assert response.success is True

    @pytest.mark.asyncio
    async def test_execute_query_schema_load_fails(self) -> None:
        """Test handling of schema load failure."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.load = AsyncMock(side_effect=Exception("DB connection failed"))

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "schema" in response.error.message.lower()
        assert response.generated_sql is None

    @pytest.mark.asyncio
    async def test_execute_query_validation_error(self) -> None:
        """Test handling of SQL validation errors."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "DELETE FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.side_effect = SecurityViolationError("DELETE not allowed")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Delete all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "DELETE not allowed" in response.error.message
        assert response.error.code == "security_violation"

    @pytest.mark.asyncio
    async def test_execute_query_execution_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of SQL execution errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT * FROM users;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None

        mock_executor = AsyncMock()
        mock_executor.execute.side_effect = DatabaseError("Query execution failed")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"test_db": mock_validator},
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "execution failed" in response.error.message.lower()
        assert response.error.code == "database_error"

    @pytest.mark.asyncio
    async def test_execute_query_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert response.error.code == "internal_error"
        assert "internal server error" in response.error.message.lower()

    @pytest.mark.asyncio
    async def test_execute_query_auto_select_database(self, mock_schema: DatabaseSchema) -> None:
        """Test auto-selecting database when only one available."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = "SELECT 1;"

        mock_validator = MagicMock()
        mock_validator.validate_or_raise.return_value = None

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validators={"only_db": mock_validator},
            sql_executors={"only_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"only_db": MagicMock()},  # Only one database
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute without specifying database
        request = QueryRequest(
            question="Test query",
            database=None,  # No database specified
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        # Verify schema was fetched for auto-selected database
        mock_cache.get.assert_called_once_with("only_db")


class TestComponentRegistryValidation:
    """Pools, validators and executors must stay aligned per database."""

    @staticmethod
    def _make(
        pools: dict[str, MagicMock],
        validators: dict[str, MagicMock],
        executors: dict[str, MagicMock],
    ) -> QueryOrchestrator:
        return QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validators=validators,
            sql_executors=executors,
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools=pools,
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

    def test_rejects_missing_executor(self) -> None:
        """Constructor rejects pools without a matching executor."""
        with pytest.raises(ValueError, match="sql_executors"):
            self._make({"db1": MagicMock()}, {"db1": MagicMock()}, {})

    def test_rejects_missing_validator(self) -> None:
        """Constructor rejects pools without a matching validator."""
        with pytest.raises(ValueError, match="sql_validators"):
            self._make({"db1": MagicMock()}, {}, {"db1": MagicMock()})

    def test_rejects_executor_for_unknown_database(self) -> None:
        """Constructor rejects executors not backed by a pool."""
        executors = {"db1": MagicMock(), "orphan": MagicMock()}
        with pytest.raises(ValueError, match="sql_executors"):
            self._make({"db1": MagicMock()}, {"db1": MagicMock()}, executors)

    def test_get_components_for_unknown_database(self) -> None:
        """Lookup helpers raise a structured error for unknown databases."""
        orchestrator = self._make(
            {"db1": MagicMock()},
            {"db1": MagicMock()},
            {"db1": MagicMock()},
        )
        with pytest.raises(DatabaseError):
            orchestrator._get_executor("other")
        with pytest.raises(DatabaseError):
            orchestrator._get_validator("other")


class TestMultiDatabaseRouting:
    """Requests must use the executor/validator of the resolved database."""

    @pytest.fixture
    def components(
        self, users_schema: DatabaseSchema
    ) -> tuple[QueryOrchestrator, AsyncMock, dict[str, MagicMock], dict[str, AsyncMock], MagicMock]:
        """Build an orchestrator with distinct components per database."""
        validators = {"db1": MagicMock(), "db2": MagicMock()}
        for validator in validators.values():
            validator.validate_or_raise.return_value = None

        executors = {"db1": AsyncMock(), "db2": AsyncMock()}
        executors["db1"].execute.return_value = ([{"origin": "db1"}], 1)
        executors["db2"].execute.return_value = ([{"origin": "db2"}], 1)

        cache = MagicMock()
        cache.get.return_value = users_schema

        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators=validators,
            sql_executors=executors,
            result_validator=MagicMock(),
            schema_cache=cache,
            pools={"db1": MagicMock(), "db2": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
        )
        return orchestrator, generator, validators, executors, cache

    @pytest.mark.asyncio
    async def test_routes_request_to_requested_database(
        self,
        components: tuple[
            QueryOrchestrator, AsyncMock, dict[str, MagicMock], dict[str, AsyncMock], MagicMock
        ],
    ) -> None:
        """A request for db2 never touches db1's executor or validator."""
        orchestrator, _generator, validators, executors, cache = components

        response = await orchestrator.execute_query(
            QueryRequest(question="Get users", database="db2", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        assert response.data is not None
        assert response.data.rows == [{"origin": "db2"}]
        executors["db2"].execute.assert_awaited_once()
        executors["db1"].execute.assert_not_awaited()
        validators["db2"].validate_or_raise.assert_called_once()
        validators["db1"].validate_or_raise.assert_not_called()
        cache.get.assert_called_with("db2")

    @pytest.mark.asyncio
    async def test_routes_request_to_other_database(
        self,
        components: tuple[
            QueryOrchestrator, AsyncMock, dict[str, MagicMock], dict[str, AsyncMock], MagicMock
        ],
    ) -> None:
        """A request for db1 uses db1's components only."""
        orchestrator, _generator, validators, executors, _cache = components

        response = await orchestrator.execute_query(
            QueryRequest(question="Get users", database="db1", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        executors["db1"].execute.assert_awaited_once()
        executors["db2"].execute.assert_not_awaited()
        validators["db1"].validate_or_raise.assert_called_once()
        validators["db2"].validate_or_raise.assert_not_called()


class TestRateLimiting:
    """Rate limiter is enforced in the request path."""

    @staticmethod
    def _orchestrator(
        users_schema: DatabaseSchema,
        limiter: MultiRateLimiter,
        generator: AsyncMock,
    ) -> QueryOrchestrator:
        cache = MagicMock()
        cache.get.return_value = users_schema
        return QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": MagicMock()},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(rate_limit_timeout=0.1),
            validation_config=ValidationConfig(),
            rate_limiter=limiter,
        )

    @pytest.mark.asyncio
    async def test_query_rate_limit_rejection(self, users_schema: DatabaseSchema) -> None:
        """When no query slot frees up, the request is rejected as rate limited."""
        limiter = MultiRateLimiter(query_limit=1, llm_limit=1)
        generator = AsyncMock()
        orchestrator = self._orchestrator(users_schema, limiter, generator)

        await limiter.query_limiter.acquire()
        try:
            response = await orchestrator.execute_query(
                QueryRequest(question="Count users", return_type=ReturnType.SQL)
            )
        finally:
            limiter.query_limiter.release()

        assert response.success is False
        assert response.error is not None
        assert response.error.code == "rate_limit_exceeded"
        assert response.error.details is not None
        assert response.error.details["resource"] == "query"
        generator.generate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_llm_rate_limit_rejection(self, users_schema: DatabaseSchema) -> None:
        """When no LLM slot frees up, generation is rejected as rate limited."""
        limiter = MultiRateLimiter(query_limit=2, llm_limit=1)
        generator = AsyncMock()
        orchestrator = self._orchestrator(users_schema, limiter, generator)

        await limiter.llm_limiter.acquire()
        try:
            response = await orchestrator.execute_query(
                QueryRequest(question="Count users", return_type=ReturnType.SQL)
            )
        finally:
            limiter.llm_limiter.release()

        assert response.success is False
        assert response.error is not None
        assert response.error.code == "rate_limit_exceeded"
        assert response.error.details is not None
        assert response.error.details["resource"] == "llm"
        generator.generate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_concurrent_limit_defaults_from_config(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Orchestrator builds a limiter from ResilienceConfig when none is passed."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT 1;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        cache = MagicMock()
        cache.get.return_value = users_schema

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(
                max_concurrent_queries=3, max_concurrent_llm_calls=2
            ),
            validation_config=ValidationConfig(),
        )

        assert orchestrator.rate_limiter.query_limiter.max_concurrent == 3
        assert orchestrator.rate_limiter.llm_limiter.max_concurrent == 2


class TestRetryBackoff:
    """Retry loop applies the configured exponential backoff."""

    @staticmethod
    def _orchestrator(
        generator: AsyncMock,
        validator: MagicMock,
        resilience: ResilienceConfig,
    ) -> QueryOrchestrator:
        return QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=resilience,
            validation_config=ValidationConfig(),
        )

    @pytest.mark.asyncio
    async def test_exponential_backoff_between_retries(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Each retry waits retry_delay * backoff_factor ** attempt seconds."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.side_effect = [
            SQLParseError("attempt 1 failed"),
            SQLParseError("attempt 2 failed"),
            None,
        ]
        orchestrator = self._orchestrator(
            generator,
            validator,
            ResilienceConfig(max_retries=2, retry_delay=1.0, backoff_factor=2.0),
        )
        orchestrator._sleep = AsyncMock()

        sql, validation, _tokens = await orchestrator._generate_sql_with_retry(
            question="Count users",
            schema=users_schema,
            request_id="backoff-1",
            validator=orchestrator.sql_validators["test_db"],
        )

        assert sql == "SELECT id FROM users;"
        assert validation.is_valid is True
        delays = [call.args[0] for call in orchestrator._sleep.call_args_list]
        assert delays == [1.0, 2.0]
        assert generator.generate.call_count == 3

    @pytest.mark.asyncio
    async def test_no_backoff_on_first_attempt_success(
        self, users_schema: DatabaseSchema
    ) -> None:
        """A successful first attempt never sleeps."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        orchestrator = self._orchestrator(
            generator,
            validator,
            ResilienceConfig(max_retries=2, retry_delay=1.0, backoff_factor=2.0),
        )
        orchestrator._sleep = AsyncMock()

        await orchestrator._generate_sql_with_retry(
            question="Count users",
            schema=users_schema,
            request_id="backoff-2",
            validator=orchestrator.sql_validators["test_db"],
        )

        orchestrator._sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_transient_llm_error_retried_with_backoff(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Transient LLM failures are retried after a backoff delay."""
        generator = AsyncMock()
        generator.generate.side_effect = [
            LLMUnavailableError("OpenAI API rate limit exceeded"),
            "SELECT 1;",
        ]
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        orchestrator = self._orchestrator(
            generator,
            validator,
            ResilienceConfig(max_retries=2, retry_delay=1.0, backoff_factor=2.0),
        )
        orchestrator._sleep = AsyncMock()

        sql, _validation, _tokens = await orchestrator._generate_sql_with_retry(
            question="Count users",
            schema=users_schema,
            request_id="backoff-3",
            validator=orchestrator.sql_validators["test_db"],
        )

        assert sql == "SELECT 1;"
        assert generator.generate.call_count == 2
        delays = [call.args[0] for call in orchestrator._sleep.call_args_list]
        assert delays == [1.0]

    @pytest.mark.asyncio
    async def test_transient_llm_error_exhausts_retries(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Persisting transient LLM failures opens the way to the circuit breaker."""
        generator = AsyncMock()
        generator.generate.side_effect = LLMUnavailableError("OpenAI API unavailable")
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        orchestrator = self._orchestrator(
            generator,
            validator,
            ResilienceConfig(max_retries=1, retry_delay=0.1, backoff_factor=1.0),
        )
        orchestrator._sleep = AsyncMock()

        with pytest.raises(LLMUnavailableError):
            await orchestrator._generate_sql_with_retry(
                question="Count users",
                schema=users_schema,
                request_id="backoff-4",
                validator=orchestrator.sql_validators["test_db"],
            )

        assert generator.generate.call_count == 2
        assert orchestrator.circuit_breaker.failure_count == 1


class TestObservabilityIntegration:
    """Metrics and tracing are recorded in the real request path."""

    @staticmethod
    def _orchestrator(
        users_schema: DatabaseSchema,
        metrics: MagicMock,
        generator: AsyncMock,
        validator: MagicMock,
        validation: ValidationConfig | None = None,
        result_validator: AsyncMock | None = None,
        executor: AsyncMock | None = None,
        max_retries: int = 0,
    ) -> QueryOrchestrator:
        cache = MagicMock()
        cache.get.return_value = users_schema
        return QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": executor or AsyncMock()},
            result_validator=result_validator or MagicMock(),
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=max_retries),
            validation_config=validation or ValidationConfig(enabled=False),
            metrics=metrics,
        )

    @pytest.mark.asyncio
    async def test_success_records_metrics_and_request_id(
        self, users_schema: DatabaseSchema
    ) -> None:
        """A successful request increments request/LLM metrics and carries a request_id."""
        metrics = MagicMock()
        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        orchestrator = self._orchestrator(users_schema, metrics, generator, validator)

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", return_type=ReturnType.SQL)
        )

        assert response.success is True
        assert response.request_id is not None
        metrics.increment_query_request.assert_called_once_with(
            status="success", database="test_db"
        )
        metrics.query_duration.observe.assert_called_once()
        metrics.increment_llm_call.assert_called_once_with("generate_sql")
        metrics.observe_llm_latency.assert_called_once()
        metrics.increment_sql_rejected.assert_not_called()

    @pytest.mark.asyncio
    async def test_security_violation_status_metrics(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Failed requests record the error status and rejection reason."""
        metrics = MagicMock()
        generator = AsyncMock()
        generator.generate.return_value = "DELETE FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.side_effect = SecurityViolationError("DELETE not allowed")
        orchestrator = self._orchestrator(
            users_schema, metrics, generator, validator, max_retries=0
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Delete users", return_type=ReturnType.SQL)
        )

        assert response.success is False
        assert response.request_id is not None
        assert response.error is not None
        assert response.error.code == "security_violation"
        metrics.increment_query_request.assert_called_once_with(
            status="security_violation", database="test_db"
        )
        metrics.increment_sql_rejected.assert_called_once_with(reason="security_violation")

    @pytest.mark.asyncio
    async def test_tracing_context_propagates_request_id(
        self, users_schema: DatabaseSchema
    ) -> None:
        """The response request_id equals the tracing context seen mid-pipeline."""
        metrics = MagicMock()
        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None

        seen: list[str | None] = []

        def record_context(_name: str) -> DatabaseSchema:
            seen.append(get_request_id())
            return users_schema

        cache = MagicMock()
        cache.get.side_effect = record_context

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": AsyncMock()},
            result_validator=MagicMock(),
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
            metrics=metrics,
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", return_type=ReturnType.SQL)
        )

        assert response.request_id is not None
        assert seen == [response.request_id]
        # Context must be cleaned up after the request
        assert get_request_id() is None

    @pytest.mark.asyncio
    async def test_result_validation_metrics_recorded(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Result validation LLM calls are counted and timed."""
        metrics = MagicMock()
        generator = AsyncMock()
        generator.generate.return_value = "SELECT id FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None

        result_validator = AsyncMock()
        result_validator.validate.return_value = ResultValidationResult(
            confidence=90,
            explanation="Good results",
            suggestion=None,
            is_acceptable=True,
        )
        executor = AsyncMock()
        executor.execute.return_value = ([{"id": 1}], 1)

        orchestrator = self._orchestrator(
            users_schema,
            metrics,
            generator,
            validator,
            validation=ValidationConfig(enabled=True, min_confidence_score=70),
            result_validator=result_validator,
            executor=executor,
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Get users", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        assert metrics.increment_llm_call.call_args_list[0].args == ("generate_sql",)
        assert metrics.increment_llm_call.call_args_list[1].args == ("validate_result",)
        assert metrics.observe_db_query_duration.called
        assert response.warnings == []


class TestValidationThresholds:
    """Configured validation thresholds are enforced in the pipeline."""

    @pytest.mark.asyncio
    async def test_question_length_limit_enforced(self, users_schema: DatabaseSchema) -> None:
        """Questions longer than ValidationConfig.max_question_length are rejected."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT 1;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None
        cache = MagicMock()
        cache.get.return_value = users_schema

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(max_question_length=5),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="this question is far too long", return_type=ReturnType.SQL)
        )

        assert response.success is False
        assert response.error is not None
        assert response.error.code == "question_too_long"
        assert response.error.details is not None
        assert response.error.details["max_question_length"] == 5
        generator.generate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_low_confidence_result_flagged_with_warning(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Results below min_confidence_score are returned with a warning."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT COUNT(*) FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None

        result_validator = AsyncMock()
        result_validator.validate.return_value = ResultValidationResult(
            confidence=50,
            explanation="Weak match",
            suggestion=None,
            is_acceptable=False,
        )
        executor = AsyncMock()
        executor.execute.return_value = ([{"count": 1}], 1)
        cache = MagicMock()
        cache.get.return_value = users_schema

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": executor},
            result_validator=result_validator,
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True, min_confidence_score=70),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        assert response.confidence == 50
        assert len(response.warnings) == 1
        assert "below the configured minimum" in response.warnings[0]

    @pytest.mark.asyncio
    async def test_confident_result_has_no_warnings(
        self, users_schema: DatabaseSchema
    ) -> None:
        """Results at or above the threshold carry no warnings."""
        generator = AsyncMock()
        generator.generate.return_value = "SELECT COUNT(*) FROM users;"
        validator = MagicMock()
        validator.validate_or_raise.return_value = None

        result_validator = AsyncMock()
        result_validator.validate.return_value = ResultValidationResult(
            confidence=95,
            explanation="Great match",
            suggestion=None,
            is_acceptable=True,
        )
        executor = AsyncMock()
        executor.execute.return_value = ([{"count": 1}], 1)
        cache = MagicMock()
        cache.get.return_value = users_schema

        orchestrator = QueryOrchestrator(
            sql_generator=generator,
            sql_validators={"test_db": validator},
            sql_executors={"test_db": executor},
            result_validator=result_validator,
            schema_cache=cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True, min_confidence_score=70),
        )

        response = await orchestrator.execute_query(
            QueryRequest(question="Count users", return_type=ReturnType.RESULT)
        )

        assert response.success is True
        assert response.confidence == 95
        assert response.warnings == []
