"""Unit tests for server-side component builders.

These tests verify the multi-database wiring of the server: one security
policy, validator and executor per configured database, so requests can never
run against the wrong database or with another database's policy.
"""

from unittest.mock import MagicMock

from pg_mcp.config.settings import DatabaseConfig, OpenAIConfig, SecurityConfig, Settings
from pg_mcp.server import build_executors, build_security_policies, build_validators


def make_multi_db_settings() -> Settings:
    """Create settings with two databases, the first with a policy override."""
    return Settings(
        openai=OpenAIConfig(api_key="sk-test"),
        security=SecurityConfig(blocked_columns="ssn"),
        databases=[
            DatabaseConfig(
                name="db1",
                host="db1.host",
                security=SecurityConfig(
                    allow_explain=True,
                    blocked_tables="secrets",
                ),
            ),
            DatabaseConfig(name="db2", host="db2.host"),
        ],
    )


class TestBuildSecurityPolicies:
    """Tests for build_security_policies."""

    def test_one_policy_per_database(self) -> None:
        """Every configured database gets its own effective policy."""
        policies = build_security_policies(make_multi_db_settings())
        assert set(policies) == {"db1", "db2"}

    def test_global_policy_inherited_by_all_databases(self) -> None:
        """Global settings apply to databases without overrides."""
        policies = build_security_policies(make_multi_db_settings())
        assert policies["db1"].blocked_columns == ["ssn"]
        assert policies["db2"].blocked_columns == ["ssn"]

    def test_per_database_override_applies_only_to_that_database(self) -> None:
        """The db1 override does not leak to db2."""
        policies = build_security_policies(make_multi_db_settings())

        assert policies["db1"].allow_explain is True
        assert policies["db1"].blocked_tables == ["secrets"]

        assert policies["db2"].allow_explain is False
        assert policies["db2"].blocked_tables == []

    def test_single_database_default_policy(self) -> None:
        """Without DATABASES the primary database is the only policy entry."""
        settings = Settings(
            openai=OpenAIConfig(api_key="sk-test"),
            security=SecurityConfig(blocked_tables="audit_log"),
        )
        policies = build_security_policies(settings)
        assert list(policies) == [settings.database.name]
        assert policies[settings.database.name].blocked_tables == ["audit_log"]


class TestBuildValidators:
    """Tests for build_validators."""

    def test_one_validator_per_database(self) -> None:
        """Validators are created for every configured database."""
        validators = build_validators(make_multi_db_settings())
        assert set(validators) == {"db1", "db2"}

    def test_validator_enforces_database_specific_table_blocks(self) -> None:
        """A table blocked on db1 stays queryable on db2."""
        validators = build_validators(make_multi_db_settings())

        is_valid, error = validators["db1"].validate("SELECT * FROM secrets")
        assert not is_valid
        assert error is not None

        is_valid, _error = validators["db2"].validate("SELECT * FROM secrets")
        assert is_valid

    def test_validator_enforces_database_specific_explain_policy(self) -> None:
        """EXPLAIN is allowed on db1 only."""
        validators = build_validators(make_multi_db_settings())

        is_valid, _error = validators["db1"].validate("EXPLAIN SELECT 1")
        assert is_valid

        is_valid, _error = validators["db2"].validate("EXPLAIN SELECT 1")
        assert not is_valid


class TestBuildExecutors:
    """Tests for build_executors."""

    def test_executor_bound_to_own_pool_and_policy(self) -> None:
        """Each executor uses its own pool, config and security policy."""
        settings = make_multi_db_settings()
        pool1, pool2 = MagicMock(), MagicMock()

        executors = build_executors(settings, {"db1": pool1, "db2": pool2})

        assert set(executors) == {"db1", "db2"}
        assert executors["db1"].pool is pool1
        assert executors["db1"].db_config.name == "db1"
        assert executors["db1"].security_config.allow_explain is True
        assert executors["db2"].pool is pool2
        assert executors["db2"].db_config.name == "db2"
        assert executors["db2"].security_config.allow_explain is False

    def test_pool_without_configuration_rejected(self) -> None:
        """A pool without a matching database configuration fails fast."""
        settings = make_multi_db_settings()
        pools = {"db1": MagicMock(), "mystery_db": MagicMock()}

        try:
            build_executors(settings, pools)
        except ValueError as exc:
            assert "mystery_db" in str(exc)
        else:
            raise AssertionError("Expected ValueError for pool without configuration")
