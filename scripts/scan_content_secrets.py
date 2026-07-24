#!/usr/bin/env python3
"""Scan repository content for credentials and runtime knowledge dependencies.

The output intentionally contains only a detector name and file:line location.
Values are never included in findings.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
}


@dataclass(frozen=True)
class Finding:
    detector: str
    path: Path
    line: int


def _tracked_or_untracked_files(root: Path) -> list[Path]:
    try:
        output = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-co", "--exclude-standard", "-z"],
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return sorted(
            path for path in root.rglob("*") if path.is_file() and not _skip(path, root)
        )
    return sorted(
        root / item.decode("utf-8")
        for item in output.split(b"\0")
        if item and (root / item.decode("utf-8")).is_file()
    )


def _skip(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    return any(part in _SKIP_DIRS for part in relative.parts)


def _is_env_file(path: Path) -> bool:
    return path.name == ".env" or path.name.startswith(".env.")


def _is_placeholder(value: str) -> bool:
    normalized = value.strip().strip("'\"").casefold()
    if not normalized:
        return True
    safe_prefixes = (
        "your-",
        "your_",
        "replace-with",
        "replace_with",
        "change-me",
        "change_me",
        "placeholder",
        "example",
        "dummy",
        "redacted",
        "<",
        "${",
        "$(",
        "...",
        "localtest-",
        "test-",
    )
    return (
        normalized.startswith(safe_prefixes)
        or "random-token" in normalized
        or normalized in {"sql-password", "testpass", "p@ssw0rd!", "test-password"}
    )


_DETECTORS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "private-key-block",
        re.compile(r"-----BEGIN [A-Z0-9 _-]{1,80}PRIVATE KEY-----"),
    ),
    ("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("github-token", re.compile(r"\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}\b")),
    ("openai-key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{20,}\b")),
    ("azure-sas-signature", re.compile(r"(?:[?&])sig=[A-Za-z0-9%+/=]{16,}")),
)
_ASSIGNMENT = re.compile(
    r"(?i)(?P<key>(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?token|bearer)[A-Z0-9_-]*)"
    r"\s*[:=]\s*(['\"])(?P<value>[^'\"]{8,})\1"
)
_ENV_ASSIGNMENT = re.compile(
    r"^\s*(?:export\s+)?[A-Z][A-Z0-9_]*(?:PASSWORD|PASSWD|PWD|SECRET|TOKEN|API[_-]?KEY)"
    r"\s*=\s*(?P<value>[^\s#;]+)"
)
_KNOWLEDGE_LINK = re.compile(
    r"https?://(?:learn\.microsoft\.com|aka\.ms|techcommunity\.microsoft\.com|"
    r"stackoverflow\.com|sqlperformance\.com|brentozar\.com)(?:/|\b)",
    re.IGNORECASE,
)
_LOCAL_USER_PATH = re.compile(
    r"(?:^|[\s`'\"])(?:"
    r"/(?:Users|home)/[^/\s`'\"]+/"
    r"|[A-Z]:[\\/]+Users[\\/]+[^\\/\s`'\"]+[\\/]"
    r")",
    re.IGNORECASE,
)


def _prohibits_external_knowledge(path: Path, root: Path) -> bool:
    """Return whether this instruction surface must remain self-contained."""

    relative = path.relative_to(root)
    return path.name == "SKILL.md" or relative in {
        Path("README.md"),
        Path("skills/README.md"),
    }


def _assignment_findings(line: str) -> bool:
    for match in _ASSIGNMENT.finditer(line):
        if not _is_placeholder(match.group("value")):
            return True
    match = _ENV_ASSIGNMENT.search(line)
    return bool(match and not _is_placeholder(match.group("value")))


def scan_files(root: Path = REPO_ROOT) -> list[Finding]:
    findings: list[Finding] = []
    for path in _tracked_or_untracked_files(root):
        if _skip(path, root):
            continue
        if _is_env_file(path):
            if path.name != ".env.example":
                findings.append(Finding("credential-file", path, 1))
            continue
        if path.name == "scan_content_secrets.py":
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for number, line in enumerate(lines, start=1):
            for detector, pattern in _DETECTORS:
                if pattern.search(line):
                    findings.append(Finding(detector, path, number))
            if _assignment_findings(line):
                findings.append(Finding("secret-assignment", path, number))
            if _prohibits_external_knowledge(path, root) and _KNOWLEDGE_LINK.search(line):
                findings.append(Finding("external-knowledge-reference", path, number))
            if _LOCAL_USER_PATH.search(line):
                findings.append(Finding("local-user-path", path, number))
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    findings = scan_files(root)
    if findings:
        for finding in findings:
            print(f"{finding.detector} {finding.path.relative_to(root)}:{finding.line}")
        return 1
    print("repository content scan ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
