"""Unit tests for structured logging configuration and formatters."""

import json
import logging
import sys

import pytest

from pg_mcp.observability.logging import (
    JSONFormatter,
    SensitiveDataFilter,
    TextFormatter,
    configure_logging,
    get_logger,
)


@pytest.fixture
def root_logger_state() -> logging.Logger:
    """Snapshot and restore the root logger around reconfiguration tests."""
    root = logging.getLogger()
    handlers = root.handlers[:]
    level = root.level
    yield root
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)


def make_record(
    msg: str = "message",
    args: object = None,
    exc_info: tuple | None = None,
) -> logging.LogRecord:
    """Create a bare LogRecord for formatter/filter tests."""
    return logging.LogRecord(
        name="test.logging",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


class TestSensitiveDataFilter:
    """Tests for sensitive data sanitization."""

    def test_redacts_sensitive_extra_fields(self) -> None:
        """Sensitive keys on the record are redacted."""
        record = make_record()
        record.password = "hunter2"
        record.database = "db1"

        assert SensitiveDataFilter().filter(record) is True
        assert record.password == "***REDACTED***"
        assert record.database == "db1"

    def test_redacts_nested_dict_values(self) -> None:
        """Sensitive keys are redacted recursively in dicts."""
        record = make_record()
        record.details = {
            "api_key": "sk-123",
            "host": "localhost",
            "nested": {"token": "abc"},
        }

        SensitiveDataFilter().filter(record)

        assert record.details["api_key"] == "***REDACTED***"
        assert record.details["host"] == "localhost"
        assert record.details["nested"]["token"] == "***REDACTED***"

    def test_sanitizes_dict_args(self) -> None:
        """Dict args are sanitized by key."""
        record = make_record(args={"password": "hunter2", "user": "bob"})

        SensitiveDataFilter().filter(record)

        assert record.args["password"] == "***REDACTED***"  # type: ignore[index]
        assert record.args["user"] == "bob"  # type: ignore[index]

    def test_sensitive_key_case_insensitive(self) -> None:
        """Key matching is case-insensitive."""
        record = make_record()
        record.__dict__["API_Key"] = "sk-123"

        SensitiveDataFilter().filter(record)

        assert record.__dict__["API_Key"] == "***REDACTED***"


class TestJSONFormatter:
    """Tests for the JSON formatter."""

    def test_formats_structured_json(self) -> None:
        """Records are serialized as JSON with request_id and extras."""
        formatter = JSONFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        record = make_record(msg="hello")
        record.request_id = "req-1"
        record.database = "db1"

        data = json.loads(formatter.format(record))

        assert data["message"] == "hello"
        assert data["level"] == "INFO"
        assert data["logger"] == "test.logging"
        assert data["request_id"] == "req-1"
        assert data["extra"]["database"] == "db1"

    def test_includes_exception_info(self) -> None:
        """Exception details are included when present."""
        formatter = JSONFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        try:
            raise ValueError("boom")
        except ValueError:
            record = make_record(msg="failed", exc_info=sys.exc_info())

        data = json.loads(formatter.format(record))

        assert "exception" in data
        assert "ValueError: boom" in data["exception"]

    def test_omits_request_id_when_absent(self) -> None:
        """No request_id key is emitted when the record has none."""
        formatter = JSONFormatter(datefmt="%Y-%m-%dT%H:%M:%S")
        data = json.loads(formatter.format(make_record()))
        assert "request_id" not in data


class TestTextFormatter:
    """Tests for the human-readable formatter."""

    def test_formats_readable_line(self) -> None:
        """Base format includes level, logger and message."""
        formatter = TextFormatter(datefmt="%Y-%m-%d %H:%M:%S")
        record = make_record(msg="hello world")

        output = formatter.format(record)

        assert "[INFO]" in output
        assert "test.logging" in output
        assert "hello world" in output

    def test_includes_request_id(self) -> None:
        """request_id is appended when present."""
        formatter = TextFormatter(datefmt="%Y-%m-%d %H:%M:%S")
        record = make_record()
        record.request_id = "req-9"

        assert "[request_id=req-9]" in formatter.format(record)

    def test_includes_exception(self) -> None:
        """Exception details are appended when present."""
        formatter = TextFormatter(datefmt="%Y-%m-%d %H:%M:%S")
        try:
            raise RuntimeError("kaboom")
        except RuntimeError:
            record = make_record(exc_info=sys.exc_info())

        assert "RuntimeError: kaboom" in formatter.format(record)


class TestConfigureLogging:
    """Tests for configure_logging wiring."""

    def test_json_configuration(self, root_logger_state: logging.Logger) -> None:
        """JSON format, level and sensitive filter are applied."""
        configure_logging(level="DEBUG", log_format="json")

        assert len(root_logger_state.handlers) == 1
        handler = root_logger_state.handlers[0]
        assert isinstance(handler.formatter, JSONFormatter)
        assert any(isinstance(f, SensitiveDataFilter) for f in handler.filters)
        assert root_logger_state.level == logging.DEBUG
        assert logging.getLogger("openai").level == logging.WARNING

    def test_text_configuration_without_filter(
        self, root_logger_state: logging.Logger
    ) -> None:
        """Text format works and the filter can be disabled."""
        configure_logging(level="INFO", log_format="text", enable_sensitive_filter=False)

        handler = root_logger_state.handlers[0]
        assert isinstance(handler.formatter, TextFormatter)
        assert handler.filters == []
        assert root_logger_state.level == logging.INFO

    def test_replaces_existing_handlers(self, root_logger_state: logging.Logger) -> None:
        """Reconfiguration removes previously installed handlers."""
        configure_logging(log_format="json")
        configure_logging(log_format="text")

        assert len(root_logger_state.handlers) == 1


class TestGetLogger:
    """Tests for get_logger."""

    def test_returns_named_logger(self) -> None:
        """get_logger returns a logger with the requested name."""
        logger = get_logger("pg_mcp.test_logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "pg_mcp.test_logger"
