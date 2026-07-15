#!/usr/bin/env python3
"""Build a derived Kendra Little indexing corpus for the SQL optimizer skill."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any


BASE_URL = "https://kendralittle.com"
TOPIC_URL = f"{BASE_URL}/topics/index-design-and-tuning/"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
SQLWORKBOOKS_URL = "https://github.com/LitKnd/SQLWorkbooks"
SQLWORKBOOKS_GIT = f"{SQLWORKBOOKS_URL}.git"
USER_AGENT = "SQL-Optimizer-Kendra-Indexing-Corpus/1.0 (+local skill research)"
WORD_RE = re.compile(r"[A-Za-z0-9_#]+")

TOPIC_RULES: dict[str, tuple[str, list[str], str]] = {
    "deduplicate": (
        "Deduplicate and consolidate indexes",
        ["deduplicate", "duplicate", "redundant", "overlap", "safest ones to drop"],
        "Group index definitions before proposing drops; drops need broader evidence.",
    ),
    "one_best_index": (
        "Design the best rowstore index for one query",
        ["best index", "one year wonders", "single nonclustered index", "logical reads"],
        "Optimize one query by measuring logical reads and testing candidate shapes.",
    ),
    "keys_includes": (
        "Index keys and INCLUDE columns",
        ["key columns", "included columns", "includes", "include columns", "index structure"],
        "Separate seek/order keys from payload columns; INCLUDEs are useful but not free.",
    ),
    "windowing": (
        "Indexing for window functions",
        ["windowing", "row_number", "rank", "over()", "window aggregate"],
        "Support PARTITION BY and ORDER BY patterns with measured rowstore or columnstore designs.",
    ),
    "partition_columnstore": (
        "Partitioning and columnstore plans",
        ["partition", "partitioned", "columnstore", "rowgroup", "predicate pushdown"],
        "Verify partition and rowgroup elimination in actual plans; do not assume it.",
    ),
    "regression": (
        "Indexes can slow queries down",
        ["make a query slower", "slow down", "slower", "regression"],
        "Adding an index can change plan choice negatively; benchmark before and after.",
    ),
    "sargability": (
        "Non-SARGable predicates",
        ["sarg", "non-sargable", "seekable", "computed column"],
        "Fix predicate shape or use computed-column strategies only when measured.",
    ),
    "group_order": (
        "GROUP BY, TOP, and ORDER BY indexing",
        ["group by", "order by", "top", "sort"],
        "Use key order to reduce sorts and reads for grouping, top, and ordering patterns.",
    ),
}

INDEX_RELEVANT_WORKBOOK_DIRS = {
    "deduplicating_indexes_sqlchallenge",
    "execution_plans_partitioning_columnstore",
    "how_index_keys_and_includes_work",
    "index_one_year_wonders_sqlchallenge",
    "indexing_for_windowing_functions",
    "learn_indexing_by_solving_problems",
    "table_partitioning_performance",
    "why_creating_an_index_can_slow_down_a_query",
}


@dataclass
class FetchResult:
    url: str
    status: int
    from_cache: bool
    body: str


class LinkTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self._href = html.unescape(href)
                self._parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href:
            self.links.append((self._href, normalize_space(" ".join(self._parts))))
            self._href = None
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._href:
            text = normalize_space(data)
            if text:
                self._parts.append(text)


class ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.capture_title = False
        self.capture_h1 = False
        self.capture_main = False
        self.main_depth = 0
        self.skip_depth = 0
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta: dict[str, str] = {}
        self.code_blocks = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = {key.lower(): value for key, value in attrs if key}
        if tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content")
            if key and content:
                self.meta[key.lower()] = html.unescape(content)
            return
        if tag == "title":
            self.capture_title = True
        if tag == "h1":
            self.capture_h1 = True
        class_name = attrs_dict.get("class") or ""
        element_id = attrs_dict.get("id") or ""
        if tag in {"main", "article"} or element_id == "post-content" or "post-content" in class_name:
            self.capture_main = True
            self.main_depth = 1
        elif self.capture_main and tag in {"div", "section", "p", "ul", "ol", "li", "pre", "code"}:
            self.main_depth += 1
        if self.capture_main and tag in {"script", "style", "noscript", "svg", "form"}:
            self.skip_depth += 1
        if self.capture_main and tag in {"pre", "code"}:
            self.code_blocks += 1

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "title":
            self.capture_title = False
        if tag == "h1":
            self.capture_h1 = False
        if self.capture_main and tag in {"script", "style", "noscript", "svg", "form"}:
            self.skip_depth = max(0, self.skip_depth - 1)
        if self.capture_main and tag in {"main", "article", "div", "section", "p", "ul", "ol", "li", "pre", "code"}:
            self.main_depth -= 1
            if self.main_depth <= 0:
                self.capture_main = False

    def handle_data(self, data: str) -> None:
        text = normalize_space(data)
        if not text:
            return
        if self.capture_title:
            self.title_parts.append(text)
        if self.capture_h1:
            self.h1_parts.append(text)
        if self.capture_main and not self.skip_depth:
            self.text_parts.append(text)

    @property
    def title(self) -> str:
        return clean_title(
            self.meta.get("og:title")
            or " ".join(self.h1_parts)
            or " ".join(self.title_parts)
        )

    @property
    def text(self) -> str:
        return normalize_space(" ".join(self.text_parts))


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def clean_title(value: str) -> str:
    value = normalize_space(value)
    value = re.sub(r"\s+\|\s+KendraLittle\.com.*$", "", value)
    return value


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slug_for_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    slug = parsed.path.strip("/").replace("/", "__") or "root"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{slug}__{digest}.html"


def request_url(
    url: str,
    cache_dir: Path,
    delay: float,
    force: bool = False,
    retries: int = 2,
) -> FetchResult:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / slug_for_url(url)
    meta_file = cache_file.with_suffix(".json")
    if cache_file.exists() and not force:
        status = 200
        if meta_file.exists():
            try:
                status = int(json.loads(meta_file.read_text()).get("status", 200))
            except (OSError, TypeError, ValueError):
                status = 200
        return FetchResult(url=url, status=status, from_cache=True, body=cache_file.read_text())

    stamp_file = cache_dir / ".last_request_at"
    last = 0.0
    if stamp_file.exists():
        try:
            last = float(stamp_file.read_text())
        except ValueError:
            last = 0.0
    wait = max(0.0, delay - (time.monotonic() - last))
    if wait:
        time.sleep(wait)

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    status = 0
    body = ""
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                status = int(response.status)
                body = response.read().decode("utf-8", errors="replace")
            break
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            body = exc.read().decode("utf-8", errors="replace")
            if attempt >= retries or status < 500:
                break
            time.sleep(delay * (attempt + 1))
    stamp_file.write_text(str(time.monotonic()))
    cache_file.write_text(body)
    meta_file.write_text(json.dumps({"url": url, "status": status, "fetched_at": utc_now()}, indent=2))
    return FetchResult(url=url, status=status, from_cache=False, body=body)


def absolute_url(href: str) -> str:
    parsed = urllib.parse.urlparse(urllib.parse.urljoin(TOPIC_URL, href))
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def discover_topic_courses(topic_html: str) -> list[dict[str, str]]:
    start_marker = 'id=topic-index-design-and-tuning'
    start = topic_html.find(start_marker)
    section = topic_html[start:] if start >= 0 else topic_html
    next_topic = section.find('class=topic-card id=topic-', len(start_marker))
    if next_topic > 0:
        section = section[:next_topic]
    parser = LinkTextParser()
    parser.feed(section)
    seen: dict[str, str] = {}
    for href, text in parser.links:
        url = absolute_url(href)
        if "/course/" in url and url.rstrip("/").count("/") == 4:
            if url not in seen or (text and not seen[url]):
                seen[url] = text
    return [{"url": url, "title": title} for url, title in sorted(seen.items())]


def extract_sitemap_locs(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [
        normalize_space(node.text or "")
        for node in root.findall(".//sm:loc", namespace)
        if normalize_space(node.text or "")
    ]


def analyze_page(url: str, html_text: str, source: str) -> dict[str, Any]:
    parser = ArticleParser()
    parser.feed(html_text)
    title = parser.title or url.rstrip("/").rsplit("/", 1)[-1].replace("-", " ").title()
    text = parser.text
    topics = classify(f"{title}\n{text}")
    return {
        "url": url,
        "title": title,
        "source": source,
        "word_count": len(WORD_RE.findall(text)),
        "code_block_count": parser.code_blocks,
        "topics": topics,
        "derived_digest": digest_for(title, topics),
    }


def classify(text: str) -> list[dict[str, Any]]:
    lower = text.lower()
    topics: list[dict[str, Any]] = []
    for topic_id, (label, keywords, guidance) in TOPIC_RULES.items():
        hits = [keyword for keyword in keywords if keyword in lower]
        if hits:
            topics.append({
                "id": topic_id,
                "label": label,
                "guidance_signal": guidance,
                "matched_terms": sorted(set(hits))[:8],
            })
    return topics


def digest_for(title: str, topics: list[dict[str, Any]]) -> str:
    if not topics:
        return f"Use '{title}' as source context; review manually before turning it into indexing guidance."
    labels = ", ".join(topic["label"] for topic in topics[:4])
    return f"Use '{title}' as source evidence for {labels}; apply only after Azure SQL plan evidence confirms the pattern."


def ensure_sqlworkbooks(repo_dir: Path) -> str:
    if not repo_dir.exists():
        subprocess.run(["git", "clone", "--depth", "1", SQLWORKBOOKS_GIT, str(repo_dir)], check=True)
    else:
        subprocess.run(["git", "-C", str(repo_dir), "fetch", "--depth", "1", "origin", "main"], check=False)
    result = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def analyze_sqlworkbooks(repo_dir: Path) -> list[dict[str, Any]]:
    workbooks: list[dict[str, Any]] = []
    for path in sorted(repo_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(repo_dir)
        if rel.parts[0] == ".git":
            continue
        if rel.parts[0] not in INDEX_RELEVANT_WORKBOOK_DIRS:
            continue
        if path.suffix.lower() not in {".sql", ".md", ".ipynb", ".sqlplan"}:
            continue
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        topics = classify(f"{rel}\n{text}")
        counts = {
            "create_index": len(re.findall(r"\bCREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\b", text, re.I)),
            "drop_index": len(re.findall(r"\bDROP\s+INDEX\b", text, re.I)),
            "statistics_io": len(re.findall(r"\bSET\s+STATISTICS\s+IO\b", text, re.I)),
            "include": len(re.findall(r"\bINCLUDE\b", text, re.I)),
            "computed_column": len(re.findall(r"\bcomputed\b|\bAS\s+\(", text, re.I)),
        }
        workbooks.append({
            "path": str(rel),
            "github_url": f"{SQLWORKBOOKS_URL}/blob/main/{urllib.parse.quote(str(rel))}",
            "line_count": text.count("\n") + 1,
            "topics": topics,
            "signals": counts,
            "derived_digest": digest_for(str(rel), topics),
        })
    return workbooks


def write_outputs(
    output_dir: Path,
    pages: list[dict[str, Any]],
    courses: list[dict[str, str]],
    lessons: list[str],
    workbooks: list[dict[str, Any]],
    sqlworkbooks_commit: str,
    errors: list[dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    topic_counts = Counter(
        topic["id"]
        for source in [*pages, *workbooks]
        for topic in source.get("topics", [])
    )
    manifest = {
        "generated_at": utc_now(),
        "source_scope": {
            "kendra_topic_url": TOPIC_URL,
            "kendra_sitemap": SITEMAP_URL,
            "sqlworkbooks": SQLWORKBOOKS_URL,
            "sqlworkbooks_commit": sqlworkbooks_commit,
            "method": "topic page courses plus sitemap lessons plus cloned SQLWorkbooks script scan",
        },
        "counts": {
            "topic_courses": len(courses),
            "lesson_urls": len(lessons),
            "web_pages_analyzed": len(pages),
            "sqlworkbooks_files_analyzed": len(workbooks),
            "errors": len(errors),
        },
        "topic_counts": dict(sorted(topic_counts.items())),
        "courses": courses,
        "lesson_urls": lessons,
        "errors": errors,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    (output_dir / "web-digests.json").write_text(json.dumps(pages, indent=2, sort_keys=True))
    (output_dir / "sqlworkbooks-digests.json").write_text(json.dumps(workbooks, indent=2, sort_keys=True))
    write_markdown(output_dir / "digests.md", manifest, pages, workbooks)


def write_markdown(
    path: Path,
    manifest: dict[str, Any],
    pages: list[dict[str, Any]],
    workbooks: list[dict[str, Any]],
) -> None:
    lines = [
        "# Kendra Little Indexing and SQLWorkbooks Digests",
        "",
        "Derived local corpus for the SQL Optimizer skill. This file keeps",
        "short paraphrased digests and source URLs/paths rather than copied source bodies.",
        "",
        f"- Generated: {manifest['generated_at']}",
        f"- Topic source: {manifest['source_scope']['kendra_topic_url']}",
        f"- SQLWorkbooks source: {manifest['source_scope']['sqlworkbooks']}",
        f"- SQLWorkbooks commit: {manifest['source_scope']['sqlworkbooks_commit']}",
        f"- Topic courses: {manifest['counts']['topic_courses']}",
        f"- Lesson URLs: {manifest['counts']['lesson_urls']}",
        f"- Web pages analyzed: {manifest['counts']['web_pages_analyzed']}",
        f"- SQLWorkbooks files analyzed: {manifest['counts']['sqlworkbooks_files_analyzed']}",
        "",
        "## Topic Counts",
        "",
    ]
    for topic_id, count in manifest["topic_counts"].items():
        label = TOPIC_RULES.get(topic_id, (topic_id, [], ""))[0]
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Web Pages", ""])
    for page in pages:
        labels = ", ".join(topic["label"] for topic in page["topics"]) or "Indexing"
        lines.extend([
            f"### {page['title']}",
            "",
            f"- URL: {page['url']}",
            f"- Source set: {page['source']}",
            f"- Topics: {labels}",
            f"- Digest: {page['derived_digest']}",
            "",
        ])
    lines.extend(["## SQLWorkbooks Files", ""])
    for workbook in workbooks:
        labels = ", ".join(topic["label"] for topic in workbook["topics"]) or "Indexing"
        lines.extend([
            f"### {workbook['path']}",
            "",
            f"- GitHub URL: {workbook['github_url']}",
            f"- Lines: {workbook['line_count']}",
            f"- Topics: {labels}",
            f"- Signals: {json.dumps(workbook['signals'], sort_keys=True)}",
            f"- Digest: {workbook['derived_digest']}",
            "",
        ])
    path.write_text("\n".join(lines).rstrip() + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/kendra-indexing"))
    parser.add_argument("--repo-dir", type=Path, default=Path(".cache/SQLWorkbooks"))
    parser.add_argument("--output-dir", type=Path, default=Path("sources/kendra-indexing"))
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    topic = request_url(TOPIC_URL, args.cache_dir, args.delay, force=args.force)
    courses = discover_topic_courses(topic.body)
    sitemap = request_url(SITEMAP_URL, args.cache_dir, args.delay, force=args.force)
    locs = extract_sitemap_locs(sitemap.body)
    course_urls = {course["url"].rstrip("/") + "/" for course in courses}
    lessons = sorted(
        url
        for url in locs
        if any(url.startswith(course_url) and url != course_url for course_url in course_urls)
    )
    web_urls = sorted(course_urls | set(lessons))
    pages: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for index, url in enumerate(web_urls, start=1):
        print(f"[web {index}/{len(web_urls)}] {url}", flush=True)
        result = request_url(url, args.cache_dir, args.delay, force=args.force)
        if result.status >= 400:
            errors.append({"url": url, "status": result.status, "error": "HTTP error"})
            continue
        source = "topic-course" if url in course_urls else "sitemap-lesson"
        pages.append(analyze_page(url, result.body, source))

    commit = ensure_sqlworkbooks(args.repo_dir)
    workbooks = analyze_sqlworkbooks(args.repo_dir)
    write_outputs(args.output_dir, pages, courses, lessons, workbooks, commit, errors)
    print(f"Wrote {args.output_dir / 'manifest.json'}")
    print(f"Wrote {args.output_dir / 'web-digests.json'}")
    print(f"Wrote {args.output_dir / 'sqlworkbooks-digests.json'}")
    print(f"Wrote {args.output_dir / 'digests.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
