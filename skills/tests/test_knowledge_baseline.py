from __future__ import annotations

import hashlib
import json
import pathlib

PACK = pathlib.Path(__file__).resolve().parents[1] / "knowledge" / "azure-sql-mcp-learning-pack.json"


def test_learning_pack_is_one_deterministic_mcp_artifact() -> None:
    payload = json.loads(PACK.read_text(encoding="utf-8"))
    assert set(payload) == {
        "content_hash",
        "lessons",
        "pack_type",
        "provenance",
        "schema_version",
    }
    assert payload["pack_type"] == "azure-sql-mcp-learning-pack"
    assert payload["schema_version"] == 1
    assert payload["lessons"] == []
    assert payload["provenance"] == {
        "contract_version": 1,
        "producer": "azure-sql-mcp-learning",
        "source": "local-owner-only-learning-store",
    }
    content = dict(payload)
    content_hash = content.pop("content_hash")
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    expected = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert content_hash == expected


def test_learning_pack_contains_no_runtime_or_private_payload() -> None:
    text = PACK.read_text(encoding="utf-8").casefold()
    assert "raw sql" not in text
    assert "password" not in text
    assert "token" not in text
    assert "chain" not in text


def test_learning_pack_exports_active_lessons_only() -> None:
    payload = json.loads(PACK.read_text(encoding="utf-8"))
    lesson_ids = [lesson["lesson_id"] for lesson in payload["lessons"]]
    assert all(lesson["status"] == "active" for lesson in payload["lessons"])
    assert lesson_ids == sorted(lesson_ids)
    assert len(lesson_ids) == len(set(lesson_ids))
