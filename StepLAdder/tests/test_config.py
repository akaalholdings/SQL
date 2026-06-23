from __future__ import annotations

import pytest

from src.config import Config, ConfigError


def _set_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SQL_SERVER_NAME", "server")
    monkeypatch.setenv("SQL_DATABASE_NAME", "db")
    monkeypatch.setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")


def test_defaults_are_safe_and_realistic(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)

    config = Config.from_env()

    assert config.dry_run is True
    assert config.min_vcores_business == 12
    assert config.min_vcores_offhours == 2
    assert config.max_vcores == 16
    assert config.up_cpu_threshold == 85
    assert config.up_io_threshold == 85
    assert config.down_threshold == 35
    assert config.sync_maxdop is True
    assert config.maxdop_required is False


def test_allowed_vcores_must_include_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    monkeypatch.setenv("ALLOWED_VCORES", "2,4,8")

    with pytest.raises(ConfigError, match="MAX_VCORES"):
        Config.from_env()


def test_down_threshold_must_be_below_up_thresholds(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    monkeypatch.setenv("UP_CPU_THRESHOLD", "80")
    monkeypatch.setenv("UP_IO_THRESHOLD", "85")
    monkeypatch.setenv("DOWN_THRESHOLD", "80")

    with pytest.raises(ConfigError, match="DOWN_THRESHOLD"):
        Config.from_env()
