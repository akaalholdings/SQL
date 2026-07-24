#!/usr/bin/env python3
"""Check local Markdown links and heading fragments without network access."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

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
_INLINE_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]*)\)")
_REFERENCE_DEF = re.compile(r"^\s{0,3}\[([^\]]+)\]:\s*(\S+)")
_REFERENCE_USE = re.compile(r"(?<!!)\[[^\]]+\]\[([^\]]+)\]")
_HEADING = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")


@dataclass(frozen=True)
class LinkIssue:
    path: Path
    line: int
    target: str
    reason: str


def _is_skipped(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    return any(part in _SKIP_DIRS for part in relative.parts)


def iter_markdown(root: Path) -> list[Path]:
    paths: list[Path] = []
    for path in root.rglob("*.md"):
        if not path.is_file() or _is_skipped(path, root):
            continue
        if path.relative_to(root).as_posix() == "docs/plan-ultimate-sql-tuner.md":
            continue
        paths.append(path)
    return sorted(paths)


def _without_fenced_code(lines: list[str]) -> list[str | None]:
    masked: list[str | None] = []
    in_fence = False
    for line in lines:
        if re.match(r"^\s*(```|~~~)", line):
            in_fence = not in_fence
            masked.append(None)
        else:
            masked.append(None if in_fence else line)
    return masked


def _target_from_match(raw_target: str) -> str:
    target = raw_target.strip()
    if target.startswith("<") and ">" in target:
        return target[1 : target.index(">")]
    return target.split(maxsplit=1)[0] if target else target


def _extract_links(lines: list[str]) -> list[tuple[int, str]]:
    masked = _without_fenced_code(lines)
    references: dict[str, str] = {}
    links: list[tuple[int, str]] = []
    for index, line in enumerate(masked, start=1):
        if line is None:
            continue
        definition = _REFERENCE_DEF.match(line)
        if definition:
            references[definition.group(1).strip().casefold()] = definition.group(2)
        for match in _INLINE_LINK.finditer(line):
            target = _target_from_match(match.group(1))
            if target:
                links.append((index, target))
        for match in _REFERENCE_USE.finditer(line):
            target = references.get(match.group(1).strip().casefold())
            if target:
                links.append((index, _target_from_match(target)))
    return links


def _github_slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*]", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return re.sub(r"\s", "-", text)


def _heading_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    lines = _without_fenced_code(path.read_text(encoding="utf-8").splitlines())
    for line in lines:
        if line is None:
            continue
        match = _HEADING.match(line)
        if not match:
            continue
        base = _github_slug(match.group(2))
        suffix = counts.get(base, 0)
        counts[base] = suffix + 1
        anchors.add(base if suffix == 0 else f"{base}-{suffix}")
    return anchors


def _check_target(markdown_path: Path, line: int, target: str, root: Path) -> LinkIssue | None:
    parsed = urlsplit(target)
    if parsed.scheme or target.startswith("//"):
        return None

    fragment = unquote(parsed.fragment)
    relative_target = unquote(parsed.path)
    candidate = markdown_path.parent if not relative_target else markdown_path.parent / relative_target
    candidate = candidate.resolve()
    if not candidate.exists():
        return LinkIssue(markdown_path, line, target, "target does not exist")
    fragment_path = markdown_path if not relative_target else candidate
    if (
        fragment
        and fragment_path.is_file()
        and fragment_path.suffix.lower() == ".md"
        and fragment not in _heading_anchors(fragment_path)
    ):
        return LinkIssue(markdown_path, line, target, "heading fragment does not exist")
    return None


def check_links(root: Path = REPO_ROOT) -> list[LinkIssue]:
    issues: list[LinkIssue] = []
    for path in iter_markdown(root):
        lines = path.read_text(encoding="utf-8").splitlines()
        for line, target in _extract_links(lines):
            issue = _check_target(path, line, target, root)
            if issue:
                issues.append(issue)
    return issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    issues = check_links(root)
    if issues:
        for issue in issues:
            print(
                f"{issue.path.relative_to(root)}:{issue.line}: "
                f"broken Markdown link {issue.target!r}: {issue.reason}",
                file=sys.stderr,
            )
        return 1
    print(f"markdown links ok ({len(iter_markdown(root))} files checked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
