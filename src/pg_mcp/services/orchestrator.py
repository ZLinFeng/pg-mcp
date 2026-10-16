"""Query orchestrator for coordinating the complete query flow.

This module provides the QueryOrchestrator class that coordinates all components
of the query processing pipeline: SQL generation, validation, execution, and result
validation. It routes every request to the executor/validator of the resolved
database, enforces rate limits, implements retry with exponential backoff and
circuit breaking, and records metrics and trace IDs for observability.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from asyncpg import Pool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

from pg_mcp.cache.schema_cache import SchemaCache
from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    ErrorCode,
    LLMError,
    LLMTimeoutError,
    LLMUnavailableError,
    PgMcpError,
    QuestionTooLongError,
    RateLimitExceededError,
    SchemaLoadError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    ErrorDetail,
    QueryRequest,
    QueryResponse,
    QueryResult,
    ReturnType,
    ValidationResult,
)
from pg_mcp.observability.metrics import MetricsCollector
from pg_mcp.observability.metrics import metrics as default_metrics
from pg_mcp.observability.tracing import generate_request_id, request_context
from pg_mcp.resilience.circuit_breaker import CircuitBreaker
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.services.result_validator import ResultValidator
from pg_mcp.services.sql_executor import SQLExecutor
from pg_mcp.services.sql_generator import SQLGenerator
from pg_mcp.services.sql_validator import SQLValidator

logger = logging.getLogger(__name__)


class QueryOrchestrator:
    """Orchestrates the complete query processing pipeline.

    This class coordinates SQL generation, validation, execution, and result
    validation. It routes each request to the executor and security validator
    registered for the resolved database (never a "primary" executor), applies
    rate limits to queries and LLM calls, retries with exponential backoff,
    and implements the circuit breaker pattern for fault tolerance.

    Example:
        >>> orchestrator = QueryOrchestrator(
        ...     sql_generator=generator,
        ...     sql_validators={"mydb": validator},
        ...     sql_executors={"mydb": executor},
        ...     result_validator=result_validator,
        ...     schema_cache=cache,
        ...     pools={"mydb": pool},
        ...     resilience_config=resilience_config,
        ...     validation_config=validation_config,
        ... )
        >>> response = await orchestrator.execute_query(QueryRequest(
        ...     question="How many users?",
        ...     database="mydb"
        ... ))
    """

    def __init__(
        self,
        sql_generator: SQLGenerator,
        sql_validators: dict[str, SQLValidator],
        sql_executors: dict[str, SQLExecutor],
        result_validator: ResultValidator,
        schema_cache: SchemaCache,
        pools: dict[str, Pool],
        resilience_config: ResilienceConfig,
        validation_config: ValidationConfig,
        rate_limiter: MultiRateLimiter | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        """Initialize query orchestrator.

        Args:
            sql_generator: SQL generation service.
            sql_validators: Mapping of database name to its security validator.
            sql_executors: Mapping of database name to its SQL executor.
            result_validator: Result validation service.
            schema_cache: Schema cache instance.
            pools: Dictionary mapping database names to connection pools.
            resilience_config: Resilience configuration for retries, circuit
                breaker and rate limits.
            validation_config: Validation configuration including thresholds.
            rate_limiter: Optional shared rate limiter; one is created from
                ``resilience_config`` when not provided.
            metrics: Optional metrics collector; the global collector is used
                when not provided.

        Raises:
            ValueError: If pools, validators and executors are not aligned by
                database name, which would allow a request to hit the wrong
                database or bypass its security policy.
        """
        pool_names = set(pools)
        if set(sql_executors) != pool_names:
            raise ValueError(
                "sql_executors must be configured for exactly the same databases as pools: "
                f"pools={sorted(pool_names)}, executors={sorted(sql_executors)}"
            )
        if set(sql_validators) != pool_names:
            raise ValueError(
                "sql_validators must be configured for exactly the same databases as pools: "
                f"pools={sorted(pool_names)}, validators={sorted(sql_validators)}"
            )

        self.sql_generator = sql_generator
        self.sql_validators = sql_validators
        self.sql_executors = sql_executors
        self.result_validator = result_validator
        self.schema_cache = schema_cache
        self.pools = pools
        self.resilience_config = resilience_config
        self.validation_config = validation_config

        # Rate limiting for queries and LLM calls
        self.rate_limiter = rate_limiter or MultiRateLimiter(
            query_limit=resilience_config.max_concurrent_queries,
            llm_limit=resilience_config.max_concurrent_llm_calls,
        )

        # Metrics (global singleton unless overridden for testing)
        self.metrics = default_metrics if metrics is None else metrics

        # Create circuit breaker for LLM calls
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=resilience_config.circuit_breaker_threshold,
            recovery_timeout=resilience_config.circuit_breaker_timeout,
        )

        # Injectable sleep used for retry backoff (patched in tests)
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    async def execute_query(self, request: QueryRequest) -> QueryResponse:
        """Execute complete query flow from question to results.

        This method orchestrates the entire pipeline:
        1. Generate request_id and enter the tracing context
        2. Enforce the configured question length limit
        3. Resolve and validate database name
        4. Acquire a query rate limit slot
        5. Load schema, generate/validate SQL with retry + backoff
        6. Execute SQL through the executor of the resolved database
        7. Validate results (optional) and record metrics
        8. Return structured response

        Args:
            request: Query request containing question and parameters.

        Returns:
            QueryResponse: Complete response with SQL, results, or error information.

        Example:
            >>> response = await orchestrator.execute_query(
            ...     QueryRequest(question="Count all users", return_type="result")
            ... )
            >>> if response.success:
            ...     print(f"Found {response.data.row_count} rows")
        """
        # Generate request_id for full-chain tracing
        request_id = generate_request_id()
        start_time = self._get_current_time_ms()
        database = request.database or "unknown"
        status = ErrorCode.INTERNAL_ERROR.value

        async with request_context(request_id):
            logger.info(
                "Starting query execution",
                extra={"request_id": request_id, "question": request.question[:100]},
            )

            try:
                # Step 1: Enforce configured question length limit
                max_length = self.validation_config.max_question_length
                if len(request.question) > max_length:
                    raise QuestionTooLongError(
                        message=(
                            f"Question length {len(request.question)} exceeds configured "
                            f"maximum of {max_length} characters"
                        ),
                        details={
                            "max_question_length": max_length,
                            "actual_length": len(request.question),
                        },
                    )

                # Step 2: Resolve database name
                database = self._resolve_database(request.database)
                logger.debug(
                    "Resolved database",
                    extra={"request_id": request_id, "database": database},
                )

                # Step 3: Run the pipeline under the query rate limiter
                try:
                    async with self.rate_limiter.for_queries(
                        timeout=self.resilience_config.rate_limit_timeout
                    ):
                        response = await self._run_pipeline(request, database, request_id)
                except TimeoutError as e:
                    raise RateLimitExceededError(
                        message=(
                            "Rate limit exceeded: timed out waiting for an execution slot "
                            f"({self.resilience_config.rate_limit_timeout}s)"
                        ),
                        details={
                            "resource": "query",
                            "timeout_seconds": self.resilience_config.rate_limit_timeout,
                        },
                    ) from e

                status = "success" if response.success else "error"

            except PgMcpError as e:
                # Handle known application errors
                logger.warning(
                    "Query execution failed with known error",
                    extra={
                        "request_id": request_id,
                        "error_code": e.code,
                        "error_message": str(e),
                    },
                )
                response = QueryResponse(
                    success=False,
                    generated_sql=None,
                    validation=None,
                    data=None,
                    error=ErrorDetail(
                        code=e.code.value,
                        message=e.message,
                        details=e.details,
                    ),
                    confidence=0,
                    tokens_used=None,
                    request_id=request_id,
                )
                status = e.code.value
            except Exception as e:
                # Handle unexpected errors
                logger.exception(
                    "Query execution failed with unexpected error",
                    extra={"request_id": request_id},
                )
                response = QueryResponse(
                    success=False,
                    generated_sql=None,
                    validation=None,
                    data=None,
                    error=ErrorDetail(
                        code=ErrorCode.INTERNAL_ERROR.value,
                        message=f"Internal server error: {e!s}",
                        details={"error_type": type(e).__name__},
                    ),
                    confidence=0,
                    tokens_used=None,
                    request_id=request_id,
                )
                status = ErrorCode.INTERNAL_ERROR.value
            finally:
                # Observability: record request outcome and duration
                duration_seconds = (self._get_current_time_ms() - start_time) / 1000
                self.metrics.increment_query_request(status=status, database=database)
                self.metrics.query_duration.observe(duration_seconds)
                logger.debug(
                    "Query execution finished",
                    extra={
                        "request_id": request_id,
                        "database": database,
                        "status": status,
                        "duration_seconds": duration_seconds,
                    },
                )

        return response

    async def _run_pipeline(
        self,
        request: QueryRequest,
        database_name: str,
        request_id: str,
    ) -> QueryResponse:
        """Run generation/execution for an already resolved database.

        The executor and security validator are always looked up from
        ``database_name`` so a request can never run against another database
        or with another database's security policy.

        Args:
            request: Original query request.
            database_name: Resolved database name.
            request_id: Request ID for tracing.

        Returns:
            QueryResponse: Successful response (errors are raised as PgMcpError).

        Raises:
            DatabaseError: If no executor/validator exists for the database.
            SchemaLoadError: If the schema cannot be loaded.
            LLMError: If SQL generation fails (circuit breaker open, etc.).
            SecurityViolationError: If generated SQL fails validation.
        """
        executor = self._get_executor(database_name)
        validator = self._get_validator(database_name)
        logger.debug(
            "Routed request to database components",
            extra={"request_id": request_id, "database": database_name},
        )

        # Load schema from cache
        schema = await self._load_schema(database_name, request_id)
        age = self.schema_cache.get_cache_age(database_name)
        if age is not None:
            self.metrics.set_schema_cache_age(database_name, age)

        # Generate and validate SQL with retry logic
        generated_sql, validation_result, tokens_used = await self._generate_sql_with_retry(
            question=request.question,
            schema=schema,
            request_id=request_id,
            validator=validator,
        )

        # If return_type is SQL, return early
        if request.return_type == ReturnType.SQL:
            logger.info(
                "Returning SQL only",
                extra={"request_id": request_id, "sql_length": len(generated_sql)},
            )
            return QueryResponse(
                success=True,
                generated_sql=generated_sql,
                validation=validation_result,
                data=None,
                error=None,
                confidence=100,
                tokens_used=tokens_used,
                request_id=request_id,
            )

        # Execute SQL through the executor of the resolved database
        logger.debug("Executing SQL", extra={"request_id": request_id})
        start_time = self._get_current_time_ms()

        results, total_count = await executor.execute(generated_sql)

        execution_time_ms = self._get_current_time_ms() - start_time
        self.metrics.observe_db_query_duration(execution_time_ms / 1000)
        logger.info(
            "SQL executed successfully",
            extra={
                "request_id": request_id,
                "database": database_name,
                "row_count": total_count,
                "execution_time_ms": execution_time_ms,
            },
        )

        # Validate results (non-blocking, failures don't fail the request)
        result_confidence = await self._validate_results_safely(
            question=request.question,
            sql=generated_sql,
            results=results,
            row_count=total_count,
            request_id=request_id,
        )

        # Flag low-confidence results without failing the request
        warnings: list[str] = []
        min_confidence = self.validation_config.min_confidence_score
        if result_confidence < min_confidence:
            warnings.append(
                f"Result confidence {result_confidence} is below the configured "
                f"minimum of {min_confidence}"
            )
            logger.warning(
                "Result confidence below configured minimum",
                extra={
                    "request_id": request_id,
                    "confidence": result_confidence,
                    "min_confidence_score": min_confidence,
                },
            )

        query_result = QueryResult(
            columns=list(results[0].keys()) if results else [],
            rows=results,
            row_count=len(results),  # Limited row count (after max_rows applied)
            execution_time_ms=execution_time_ms,
        )

        return QueryResponse(
            success=True,
            generated_sql=generated_sql,
            validation=validation_result,
            data=query_result,
            error=None,
            confidence=result_confidence,
            tokens_used=tokens_used,
            request_id=request_id,
            warnings=warnings,
        )

    def _get_executor(self, database_name: str) -> SQLExecutor:
        """Get the SQL executor registered for a database.

        Args:
            database_name: Resolved database name.

        Returns:
            SQLExecutor: Executor bound to that database's connection pool.

        Raises:
            DatabaseError: If no executor is registered for the database.
        """
        executor = self.sql_executors.get(database_name)
        if executor is None:
            raise DatabaseError(
                message=f"No SQL executor configured for database '{database_name}'",
                details={
                    "database": database_name,
                    "available_databases": sorted(self.sql_executors),
                },
            )
        return executor

    def _get_validator(self, database_name: str) -> SQLValidator:
        """Get the security validator registered for a database.

        Args:
            database_name: Resolved database name.

        Returns:
            SQLValidator: Validator enforcing that database's security policy.

        Raises:
            DatabaseError: If no validator is registered for the database.
        """
        validator = self.sql_validators.get(database_name)
        if validator is None:
            raise DatabaseError(
                message=f"No SQL validator configured for database '{database_name}'",
                details={
                    "database": database_name,
                    "available_databases": sorted(self.sql_validators),
                },
            )
        return validator

    async def _load_schema(self, database_name: str, request_id: str) -> Any:
        """Load the schema for a database from cache or the database itself.

        Args:
            database_name: Resolved database name.
            request_id: Request ID for tracing.

        Returns:
            DatabaseSchema: Schema for the requested database.

        Raises:
            DatabaseError: If no connection pool exists for the database.
            SchemaLoadError: If introspection fails.
        """
        schema = self.schema_cache.get(database_name)
        if schema is not None:
            logger.debug(
                "Schema loaded from cache",
                extra={
                    "request_id": request_id,
                    "database": database_name,
                    "tables": len(schema.tables),
                },
            )
            return schema

        pool = self.pools.get(database_name)
        if pool is None:
            raise DatabaseError(
                message=f"No connection pool available for database '{database_name}'",
                details={"database": database_name},
            )
        try:
            schema = await self.schema_cache.load(database_name, pool)
        except Exception as e:
            raise SchemaLoadError(
                message=f"Failed to load schema for database '{database_name}': {e!s}",
                details={"database": database_name, "error": str(e)},
            ) from e

        logger.debug(
            "Schema loaded",
            extra={
                "request_id": request_id,
                "database": database_name,
                "tables": len(schema.tables),
            },
        )
        return schema

    async def _generate_sql(
        self,
        question: str,
        schema: Any,
        previous_attempt: str | None,
        error_feedback: str | None,
        request_id: str,
    ) -> str:
        """Call the LLM under the LLM rate limiter and record metrics.

        Args:
            question: User's natural language question.
            schema: Database schema for context.
            previous_attempt: Previously generated SQL that failed (for retry).
            error_feedback: Error message from previous attempt (for retry).
            request_id: Request ID for tracing.

        Returns:
            str: Generated SQL query.

        Raises:
            RateLimitExceededError: If no LLM slot becomes available in time.
            LLMError: If generation fails.
        """
        self.metrics.increment_llm_call("generate_sql")
        start_time = time.time()
        try:
            async with self.rate_limiter.for_llm(
                timeout=self.resilience_config.rate_limit_timeout
            ):
                return await self.sql_generator.generate(
                    question=question,
                    schema=schema,
                    previous_attempt=previous_attempt,
                    error_feedback=error_feedback,
                )
        except TimeoutError as e:
            raise RateLimitExceededError(
                message=(
                    "Rate limit exceeded: timed out waiting for an LLM slot "
                    f"({self.resilience_config.rate_limit_timeout}s)"
                ),
                details={
                    "resource": "llm",
                    "timeout_seconds": self.resilience_config.rate_limit_timeout,
                },
            ) from e
        finally:
            self.metrics.observe_llm_latency("generate_sql", time.time() - start_time)

    async def _generate_sql_with_retry(
        self,
        question: str,
        schema: Any,
        request_id: str,
        validator: SQLValidator,
    ) -> tuple[str, ValidationResult, int | None]:
        """Generate and validate SQL with retry logic and exponential backoff.

        This method implements a retry loop that:
        1. Checks circuit breaker state
        2. Waits an exponentially increasing delay before each retry
            (``retry_delay * backoff_factor ** attempt``)
        3. Generates SQL using LLM (rate limited, with metrics)
        4. Validates the generated SQL with the database's security policy
        5. On validation failure, retries with error feedback
        6. Retries transient LLM failures (timeouts, unavailable, rate limits)
        7. Records success/failure to circuit breaker

        Args:
            question: User's natural language question.
            schema: Database schema for context.
            request_id: Request ID for tracing.
            validator: Security validator of the resolved database.

        Returns:
            tuple: (generated_sql, validation_result, tokens_used)

        Raises:
            LLMError: If circuit breaker is open or generation fails.
            RateLimitExceededError: If the LLM rate limiter times out.
            SecurityViolationError: If SQL fails validation after all retries.
            SQLParseError: If SQL cannot be parsed.

        Example:
            >>> sql, validation, tokens = await orchestrator._generate_sql_with_retry(
            ...     question="Count users",
            ...     schema=db_schema,
            ...     request_id="123",
            ...     validator=validator,
            ... )
        """
        # Check circuit breaker
        if not self.circuit_breaker.allow_request():
            raise LLMError(
                message="SQL generation service is temporarily unavailable (circuit breaker open)",
                details={
                    "circuit_state": self.circuit_breaker.state,
                    "failure_count": self.circuit_breaker.failure_count,
                },
            )

        previous_sql: str | None = None
        error_feedback: str | None = None
        max_retries = self.resilience_config.max_retries
        tokens_used: int | None = None

        for attempt in range(max_retries + 1):
            try:
                # Exponential backoff before every retry attempt
                if attempt > 0:
                    delay = self.resilience_config.retry_delay * (
                        self.resilience_config.backoff_factor ** (attempt - 1)
                    )
                    logger.debug(
                        "Waiting before SQL generation retry",
                        extra={
                            "request_id": request_id,
                            "attempt": attempt + 1,
                            "delay_seconds": delay,
                        },
                    )
                    await self._sleep(delay)

                logger.debug(
                    "Generating SQL",
                    extra={
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "max_retries": max_retries + 1,
                    },
                )

                # Generate SQL (rate limited + instrumented)
                generated_sql = await self._generate_sql(
                    question=question,
                    schema=schema,
                    previous_attempt=previous_sql,
                    error_feedback=error_feedback,
                    request_id=request_id,
                )

                logger.debug(
                    "SQL generated",
                    extra={
                        "request_id": request_id,
                        "sql_length": len(generated_sql),
                    },
                )

                # Validate SQL with the security policy of the resolved database
                try:
                    validator.validate_or_raise(generated_sql)
                except (SecurityViolationError, SQLParseError) as validation_error:
                    self.metrics.increment_sql_rejected(reason=validation_error.code.value)
                    if attempt < max_retries:
                        # Record as failure and retry with feedback
                        logger.warning(
                            "SQL validation failed, retrying with feedback",
                            extra={
                                "request_id": request_id,
                                "attempt": attempt + 1,
                                "error": str(validation_error),
                            },
                        )
                        previous_sql = generated_sql
                        error_feedback = str(validation_error)
                        continue
                    else:
                        # Out of retries, record failure and raise
                        self.circuit_breaker.record_failure()
                        logger.error(
                            "SQL validation failed after all retries",
                            extra={
                                "request_id": request_id,
                                "attempts": attempt + 1,
                                "error": str(validation_error),
                            },
                        )
                        raise

                # Validation successful
                self.circuit_breaker.record_success()
                logger.info(
                    "SQL generated and validated successfully",
                    extra={
                        "request_id": request_id,
                        "attempts": attempt + 1,
                    },
                )

                # Build validation result
                validation_result = ValidationResult(
                    is_valid=True,
                    is_select=True,
                    allows_data_modification=False,
                    uses_blocked_functions=[],
                    error_message=None,
                )

                return generated_sql, validation_result, tokens_used

            except (LLMTimeoutError, LLMUnavailableError) as transient_error:
                # Transient LLM failure: retry with exponential backoff
                if attempt < max_retries:
                    logger.warning(
                        "Transient LLM error, retrying with backoff",
                        extra={
                            "request_id": request_id,
                            "attempt": attempt + 1,
                            "error": str(transient_error),
                        },
                    )
                    continue
                self.circuit_breaker.record_failure()
                logger.error(
                    "Transient LLM error persisted after all retries",
                    extra={
                        "request_id": request_id,
                        "attempts": attempt + 1,
                        "error": str(transient_error),
                    },
                )
                raise
            except (LLMError, RateLimitExceededError, SecurityViolationError, SQLParseError):
                # Re-raise known errors
                raise
            except Exception as e:
                # Unexpected error during generation
                self.circuit_breaker.record_failure()
                logger.exception(
                    "Unexpected error during SQL generation",
                    extra={"request_id": request_id},
                )
                raise LLMError(
                    message=f"SQL generation failed unexpectedly: {e!s}",
                    details={"error_type": type(e).__name__},
                ) from e

        # Should not reach here, but just in case
        self.circuit_breaker.record_failure()
        raise LLMError(
            message="SQL generation failed after all retry attempts",
            details={"max_retries": max_retries},
        )

    async def _validate_results_safely(
        self,
        question: str,
        sql: str,
        results: list[dict[str, Any]],
        row_count: int,
        request_id: str,
    ) -> int:
        """Validate query results with error handling (non-blocking).

        This method attempts to validate results using LLM (rate limited and
        instrumented), but failures don't cause the overall query to fail.
        Returns a confidence score.

        Args:
            question: User's original question.
            sql: Generated SQL query.
            results: Query results.
            row_count: Total row count.
            request_id: Request ID for tracking.

        Returns:
            int: Confidence score (0-100). Returns 100 if validation disabled/fails.

        Example:
            >>> confidence = await orchestrator._validate_results_safely(
            ...     question="Count users",
            ...     sql="SELECT COUNT(*) FROM users",
            ...     results=[{"count": 42}],
            ...     row_count=1,
            ...     request_id="123",
            ... )
        """
        if not self.validation_config.enabled:
            return 100

        try:
            logger.debug(
                "Validating results",
                extra={"request_id": request_id},
            )

            self.metrics.increment_llm_call("validate_result")
            start_time = time.time()
            try:
                async with self.rate_limiter.for_llm(
                    timeout=self.resilience_config.rate_limit_timeout
                ):
                    validation_result = await self.result_validator.validate(
                        question=question,
                        sql=sql,
                        results=results,
                        row_count=row_count,
                    )
            finally:
                self.metrics.observe_llm_latency("validate_result", time.time() - start_time)

            logger.info(
                "Result validation completed",
                extra={
                    "request_id": request_id,
                    "confidence": validation_result.confidence,
                    "is_acceptable": validation_result.is_acceptable,
                },
            )

            return validation_result.confidence

        except Exception as e:
            # Log but don't fail the query (includes LLM rate limit timeouts)
            logger.warning(
                "Result validation failed, continuing with default confidence",
                extra={
                    "request_id": request_id,
                    "error": str(e),
                },
            )
            return 100  # Default to high confidence if validation fails

    def _resolve_database(self, database: str | None) -> str:
        """Resolve database name from request or auto-select.

        If database is specified, validate it exists.
        If not specified and only one database available, auto-select it.

        Args:
            database: Database name from request (optional).

        Returns:
            str: Resolved database name.

        Raises:
            DatabaseError: If database is invalid or cannot be auto-selected.

        Example:
            >>> name = orchestrator._resolve_database("mydb")  # Validates "mydb" exists
            >>> name = orchestrator._resolve_database(None)  # Auto-selects if only one DB
        """
        if database is not None:
            # Validate specified database exists
            if database not in self.pools:
                raise DatabaseError(
                    message=f"Database '{database}' not found",
                    details={
                        "requested_database": database,
                        "available_databases": list(self.pools.keys()),
                    },
                )
            return database

        # Auto-select if only one database available
        available_dbs = list(self.pools.keys())
        if len(available_dbs) == 0:
            raise DatabaseError(
                message="No databases configured",
                details={},
            )
        if len(available_dbs) == 1:
            return available_dbs[0]

        # Multiple databases, must specify
        raise DatabaseError(
            message="Multiple databases available, please specify which to query",
            details={"available_databases": available_dbs},
        )

    @staticmethod
    def _get_current_time_ms() -> float:
        """Get current time in milliseconds.

        Returns:
            float: Current time in milliseconds since epoch.
        """
        return time.time() * 1000
