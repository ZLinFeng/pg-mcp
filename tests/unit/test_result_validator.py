"""Unit tests for the ResultValidator service.

Tests cover the validation-disabled shortcut, prompt building/sampling,
LLM response parsing, confidence bounds and error mapping (all using mocks).
"""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from pg_mcp.config.settings import OpenAIConfig, ValidationConfig
from pg_mcp.models.errors import LLMError, LLMTimeoutError, LLMUnavailableError
from pg_mcp.services.result_validator import ResultValidator


def make_validator(
    validation_config: ValidationConfig | None = None,
) -> ResultValidator:
    """Create a ResultValidator with a test configuration."""
    config = OpenAIConfig(api_key=SecretStr("sk-test-key-12345"))
    return ResultValidator(
        openai_config=config,
        validation_config=validation_config or ValidationConfig(),
    )


def completion_response(content: str | None) -> MagicMock:
    """Build a mock ChatCompletion with the given message content."""
    response = MagicMock()
    if content is None:
        response.choices = []
        return response
    choice = MagicMock()
    if content == "":
        choice.message.content = None
    else:
        choice.message.content = content
    response.choices = [choice]
    return response


def mock_create(validator: ResultValidator, content: str | None) -> AsyncMock:
    """Replace the OpenAI client with an AsyncMock returning *content*."""
    create = AsyncMock(return_value=completion_response(content))
    validator.client.chat.completions.create = create
    return create


class TestValidationDisabled:
    """Validation can be turned off via config."""

    @pytest.mark.asyncio
    async def test_disabled_returns_full_confidence_without_llm_call(self) -> None:
        """No LLM call is made when validation is disabled."""
        validator = make_validator(ValidationConfig(enabled=False))
        create = AsyncMock()
        validator.client.chat.completions.create = create

        result = await validator.validate(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 5}],
            row_count=1,
        )

        assert result.confidence == 100
        assert result.is_acceptable is True
        create.assert_not_awaited()


class TestSuccessfulValidation:
    """Happy-path validation against a mocked LLM."""

    @pytest.mark.asyncio
    async def test_confident_result_is_acceptable(self) -> None:
        """A high confidence JSON response is acceptable."""
        validator = make_validator()
        payload = json.dumps(
            {"confidence": 95, "explanation": "Counts match", "suggestion": None}
        )
        create = mock_create(validator, payload)

        result = await validator.validate(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 5}],
            row_count=1,
        )

        assert result.confidence == 95
        assert result.is_acceptable is True
        assert result.explanation == "Counts match"
        create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_low_confidence_is_not_acceptable(self) -> None:
        """Confidence below confidence_threshold is flagged unacceptable."""
        validator = make_validator(ValidationConfig(confidence_threshold=70))
        payload = json.dumps({"confidence": 50, "explanation": "Weak match"})

        mock_create(validator, payload)
        result = await validator.validate(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 5}],
            row_count=1,
        )

        assert result.confidence == 50
        assert result.is_acceptable is False

    @pytest.mark.asyncio
    async def test_results_are_sampled_before_prompting(self) -> None:
        """Only sample_rows results are sent to the LLM."""
        validator = make_validator(ValidationConfig(sample_rows=2))
        payload = json.dumps({"confidence": 90, "explanation": "ok"})
        create = mock_create(validator, payload)

        results = [
            {"id": 1, "marker": "keep_1"},
            {"id": 2, "marker": "keep_2"},
            {"id": 3, "marker": "excluded_3"},
        ]
        await validator.validate(
            question="List users",
            sql="SELECT * FROM users",
            results=results,
            row_count=3,
        )

        prompt: str = create.await_args.kwargs["messages"][1]["content"]
        assert "keep_2" in prompt
        assert "excluded_3" not in prompt

    @pytest.mark.asyncio
    async def test_deterministic_settings_used(self) -> None:
        """Validation uses temperature 0 and JSON response format."""
        validator = make_validator()
        payload = json.dumps({"confidence": 80, "explanation": "ok"})
        create = mock_create(validator, payload)

        await validator.validate(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 1}],
            row_count=1,
        )

        kwargs: dict[str, Any] = create.await_args.kwargs
        assert kwargs["temperature"] == 0.0
        assert kwargs["response_format"] == {"type": "json_object"}


class TestConfidenceBounds:
    """Confidence values are normalized into 0-100."""

    @pytest.mark.asyncio
    async def test_out_of_range_int_clamped(self) -> None:
        """Confidence above 100 is clamped to 100."""
        validator = make_validator()
        mock_create(validator, json.dumps({"confidence": 150, "explanation": "x"}))

        result = await validator.validate(
            question="q", sql="SELECT 1", results=[{"x": 1}], row_count=1
        )
        assert result.confidence == 100

    @pytest.mark.asyncio
    async def test_float_confidence_converted(self) -> None:
        """Float confidences are converted to integers."""
        validator = make_validator()
        mock_create(validator, json.dumps({"confidence": 72.7, "explanation": "x"}))

        result = await validator.validate(
            question="q", sql="SELECT 1", results=[{"x": 1}], row_count=1
        )
        assert result.confidence == 72
        assert isinstance(result.confidence, int)

    @pytest.mark.asyncio
    async def test_non_numeric_confidence_defaults_to_fifty(self) -> None:
        """Non-numeric confidence values fall back to 50."""
        validator = make_validator()
        mock_create(validator, json.dumps({"confidence": "high", "explanation": "x"}))

        result = await validator.validate(
            question="q", sql="SELECT 1", results=[{"x": 1}], row_count=1
        )
        assert result.confidence == 50


class TestValidationErrors:
    """Error mapping for malformed and failing LLM responses."""

    @pytest.mark.asyncio
    async def test_empty_choices_raises_llm_error(self) -> None:
        """No choices in the response raises LLMError."""
        validator = make_validator()
        mock_create(validator, None)

        with pytest.raises(LLMError, match="empty response"):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )

    @pytest.mark.asyncio
    async def test_empty_content_raises_llm_error(self) -> None:
        """Empty message content raises LLMError."""
        validator = make_validator()
        mock_create(validator, "")

        with pytest.raises(LLMError, match="empty message content"):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )

    @pytest.mark.asyncio
    async def test_invalid_json_returns_moderate_confidence(self) -> None:
        """Malformed JSON in the LLM response degrades to confidence 60."""
        validator = make_validator()
        mock_create(validator, "definitely not json {")

        result = await validator.validate(
            question="q", sql="SELECT 1", results=[], row_count=0
        )

        assert result.confidence == 60
        assert result.is_acceptable is False
        assert result.suggestion is not None

    @pytest.mark.asyncio
    async def test_timeout_maps_to_llm_timeout_error(self) -> None:
        """TimeoutError from the API maps to LLMTimeoutError."""
        validator = make_validator()
        validator.client.chat.completions.create = AsyncMock(side_effect=TimeoutError())

        with pytest.raises(LLMTimeoutError):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )

    @pytest.mark.asyncio
    async def test_authentication_error_maps_to_unavailable(self) -> None:
        """Authentication failures map to LLMUnavailableError."""
        validator = make_validator()
        validator.client.chat.completions.create = AsyncMock(
            side_effect=Exception("invalid api_key provided")
        )

        with pytest.raises(LLMUnavailableError, match="authentication"):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )

    @pytest.mark.asyncio
    async def test_rate_limit_error_maps_to_unavailable(self) -> None:
        """Rate limit errors map to LLMUnavailableError."""
        validator = make_validator()
        validator.client.chat.completions.create = AsyncMock(
            side_effect=Exception("rate_limit exceeded for this model")
        )

        with pytest.raises(LLMUnavailableError, match="rate limit"):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )

    @pytest.mark.asyncio
    async def test_unknown_error_maps_to_llm_error(self) -> None:
        """Other API failures map to a generic LLMError."""
        validator = make_validator()
        validator.client.chat.completions.create = AsyncMock(
            side_effect=Exception("connection reset")
        )

        with pytest.raises(LLMError, match="Result validation failed"):
            await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )
