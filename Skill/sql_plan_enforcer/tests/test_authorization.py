"""The apply gate is fail-closed: nothing applies unless the kill switch is off, apply
mode is explicitly on, AND the target is allowlisted. Any one missing => dry-run only."""

import json

from authorization import can_apply, is_allowed, load_allowlist

ALLOWLIST = {
    "environments": ["mid_dev", "mid_prod"],
    "query_ids": [42, 99],
    "deny_query_ids": [13],
}


def _enable(monkeypatch):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_APPLY", "1")
    monkeypatch.delenv("SQL_PLAN_ENFORCER_DISABLE", raising=False)


def test_allowed_target_passes(monkeypatch):
    _enable(monkeypatch)
    ok, _ = can_apply("mid_prod", 42, ALLOWLIST)
    assert ok is True


def test_dry_run_by_default(monkeypatch):
    monkeypatch.delenv("SQL_PLAN_ENFORCER_APPLY", raising=False)
    monkeypatch.delenv("SQL_PLAN_ENFORCER_DISABLE", raising=False)
    ok, reason = can_apply("mid_prod", 42, ALLOWLIST)
    assert ok is False
    assert "dry-run" in reason


def test_kill_switch_overrides_everything(monkeypatch):
    monkeypatch.setenv("SQL_PLAN_ENFORCER_APPLY", "1")
    monkeypatch.setenv("SQL_PLAN_ENFORCER_DISABLE", "1")
    ok, reason = can_apply("mid_prod", 42, ALLOWLIST)
    assert ok is False
    assert "kill switch" in reason


def test_environment_not_allowlisted_denied(monkeypatch):
    _enable(monkeypatch)
    ok, reason = can_apply("mid_sandbox", 42, ALLOWLIST)
    assert ok is False
    assert "environment" in reason


def test_denylist_wins_over_allow(monkeypatch):
    _enable(monkeypatch)
    deny_list = {"environments": ["mid_prod"], "query_ids": "*", "deny_query_ids": [13]}
    ok, reason = can_apply("mid_prod", 13, deny_list)
    assert ok is False
    assert "denylist" in reason


def test_query_not_in_allowlist_denied(monkeypatch):
    _enable(monkeypatch)
    ok, reason = can_apply("mid_prod", 777, ALLOWLIST)
    assert ok is False
    assert "not in allowlist" in reason


def test_wildcard_query_ids(monkeypatch):
    _enable(monkeypatch)
    wildcard = {"environments": ["mid_prod"], "query_ids": "*"}
    ok, _ = can_apply("mid_prod", 12345, wildcard)
    assert ok is True


def test_is_allowed_is_pure():
    ok, _ = is_allowed("mid_dev", 99, ALLOWLIST)
    assert ok is True
    ok, _ = is_allowed("mid_dev", 100, ALLOWLIST)
    assert ok is False


def test_missing_allowlist_file_denies(tmp_path):
    # Fail-closed: an unreadable/missing allowlist grants nothing.
    loaded = load_allowlist(tmp_path / "nope.json")
    assert loaded == {}
    ok, _ = is_allowed("mid_prod", 42, loaded)
    assert ok is False


def test_load_allowlist_reads_file(tmp_path):
    path = tmp_path / "allowlist.json"
    path.write_text(json.dumps(ALLOWLIST), encoding="utf-8")
    assert load_allowlist(path) == ALLOWLIST
