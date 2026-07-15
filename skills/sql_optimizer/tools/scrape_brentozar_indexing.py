#!/usr/bin/env python3
"""Build a derived Brent Ozar indexing corpus for the SQL optimizer skill.

The public Brent Ozar WordPress REST API requires authentication, so this uses
the public Indexing category archive plus public sitemap XML. It honors the
site's robots.txt crawl delay by default. Raw HTML is cached only under an
ignored local cache; durable outputs are manifests and short derived digests.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
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


BASE_URL = "https://www.brentozar.com"
INDEXING_CATEGORY_URL = f"{BASE_URL}/archive/category/indexing/"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
USER_AGENT = "SQL-Optimizer-BrentOzar-Indexing-Corpus/1.0 (+local skill research)"
POST_URL_RE = re.compile(r"^https://www\.brentozar\.com/archive/\d{4}/\d{2}/[^/#?]+/?$")
CATEGORY_PAGE_RE = re.compile(
    r"^https://www\.brentozar\.com/archive/category/indexing/page/(\d+)/?$"
)
WORD_RE = re.compile(r"[A-Za-z0-9_#]+")


TOPIC_RULES: dict[str, tuple[str, list[str], str]] = {
    "missing_index_hints": (
        "Missing index hints and DMVs",
        [
            "missing index",
            "missing indexes",
            "dm_db_missing_index",
            "green missing index",
            "index recommendations",
        ],
        "Treat missing-index hints as leads that need consolidation, overlap checks, and measurement.",
    ),
    "sargability": (
        "SARGability and predicate shape",
        [
            "sargable",
            "non-sargable",
            "sargability",
            "function in the where",
            "implicit conversion",
            "leading wildcard",
            "wildcard search",
        ],
        "Favor predicates that let SQL Server seek; computed-column or search-specific patterns need explicit proof.",
    ),
    "key_order": (
        "Index key order",
        [
            "key order",
            "column order",
            "equality",
            "inequality",
            "selectivity",
            "most selective",
            "desc in indexes",
            "descending",
        ],
        "Order keys around equality, range, join, grouping, and ordering needs rather than folklore alone.",
    ),
    "coverage_includes": (
        "Covering indexes and INCLUDE columns",
        [
            "include columns",
            "included columns",
            "covering index",
            "key lookup",
            "bookmark lookup",
            "lookup",
        ],
        "Use INCLUDE columns to remove expensive lookups, but account for write cost and size.",
    ),
    "filtered_indexes": (
        "Filtered indexes",
        [
            "filtered index",
            "filter predicate",
            "is not null",
            "where is",
            "soft delete",
        ],
        "Use filtered indexes only when the query predicate reliably matches the filter and parameters do not hide it.",
    ),
    "computed_columns_json": (
        "Computed columns and JSON indexing",
        [
            "computed column",
            "json",
            "json_value",
            "persisted",
            "expression",
        ],
        "Use computed-column/expression indexing when it makes a recurring predicate seekable and write overhead is acceptable.",
    ),
    "columnstore": (
        "Columnstore indexes",
        [
            "columnstore",
            "rowgroup",
            "batch mode",
            "ordered columnstore",
            "segment",
        ],
        "Consider columnstore for analytic scans and aggregations; test rowgroup/order behavior and operational side effects.",
    ),
    "fragmentation_maintenance": (
        "Fragmentation and index maintenance",
        [
            "fragmentation",
            "rebuild",
            "reorganize",
            "fill factor",
            "fillfactor",
            "index maintenance",
            "adr",
            "rcsi",
        ],
        "Do not treat rebuilds, reorganizes, or fill factor as default tuning fixes; measure the actual bottleneck first.",
    ),
    "duplicates_unused": (
        "Duplicate, overlapping, and unused indexes",
        [
            "duplicate index",
            "duplicates",
            "unused index",
            "unused indexes",
            "too many indexes",
            "index hoarding",
            "drop index",
        ],
        "Identify duplicates, prefixes, and low-use indexes, but require workload-wide evidence before dropping.",
    ),
    "parameter_sensitivity": (
        "Parameter sensitivity",
        [
            "parameter sniffing",
            "parameter sensitive",
            "optimize for",
            "recompile",
            "local variable",
        ],
        "Test index/rewrite choices across representative parameter buckets before declaring a fix.",
    ),
    "temp_tables": (
        "Temp table indexing",
        [
            "temp table",
            "#temp",
            "temporary table",
            "tempdb",
        ],
        "Index temp tables only when the combined load, update, and read workload proves it helps.",
    ),
    "partitioning": (
        "Partitioning and vertical partitioning",
        [
            "partitioning",
            "partitioned",
            "vertical partitioning",
            "partition elimination",
        ],
        "Treat partitioning as a manageability or workload-isolation design, not a default single-query index fix.",
    ),
    "indexed_views": (
        "Indexed views",
        [
            "indexed view",
            "materialized view",
            "view index",
            "noexpand",
        ],
        "Indexed views can precompute expensive work, but they add write cost and strict eligibility requirements.",
    ),
}


@dataclass
class FetchResult:
    url: str
    status: int
    from_cache: bool
    body: str


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if href:
            self.links.append(html.unescape(href))


class ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.article_depth = 0
        self.skip_depth = 0
        self.capture_h1 = False
        self.capture_title = False
        self.capture_time = False
        self.h1_parts: list[str] = []
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.datetimes: list[str] = []
        self.meta: dict[str, str] = {}
        self.code_blocks = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = {k.lower(): v for k, v in attrs if k}

        if tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content")
            if key and content:
                self.meta[key.lower()] = html.unescape(content)
            return

        if tag == "article":
            self.article_depth += 1
        elif self.article_depth:
            self.article_depth += 1

        if self.article_depth and tag in {"script", "style", "noscript", "svg", "form"}:
            self.skip_depth += 1

        if tag == "h1":
            self.capture_h1 = True
        if tag == "title":
            self.capture_title = True
        if tag == "time":
            self.capture_time = True
            dt = attrs_dict.get("datetime")
            if dt:
                self.datetimes.append(dt)
        if self.article_depth and tag in {"pre", "code"}:
            self.code_blocks += 1

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "h1":
            self.capture_h1 = False
        if tag == "title":
            self.capture_title = False
        if tag == "time":
            self.capture_time = False
        if self.article_depth and tag in {"script", "style", "noscript", "svg", "form"}:
            self.skip_depth = max(0, self.skip_depth - 1)
        if self.article_depth:
            self.article_depth -= 1

    def handle_data(self, data: str) -> None:
        text = normalize_space(data)
        if not text:
            return
        if self.capture_h1:
            self.h1_parts.append(text)
        if self.capture_title:
            self.title_parts.append(text)
        if self.capture_time:
            self.datetimes.append(text)
        if self.article_depth and not self.skip_depth:
            self.text_parts.append(text)

    @property
    def title(self) -> str:
        if self.meta.get("og:title"):
            return clean_title(self.meta["og:title"])
        if self.h1_parts:
            return clean_title(" ".join(self.h1_parts))
        return clean_title(" ".join(self.title_parts))

    @property
    def published(self) -> str | None:
        for key in ("article:published_time", "og:updated_time"):
            if self.meta.get(key):
                return self.meta[key]
        for value in self.datetimes:
            if re.search(r"\d{4}", value):
                return normalize_space(value)
        return None

    @property
    def text(self) -> str:
        return normalize_space(" ".join(self.text_parts))


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def clean_title(value: str) -> str:
    value = normalize_space(value)
    value = re.sub(r"\s+-\s+Brent Ozar Unlimited.*$", "", value)
    return value


def slug_for_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    slug = parsed.path.strip("/").replace("/", "__") or "root"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{slug}__{digest}.html"


def request_url(url: str, cache_dir: Path, delay: float, force: bool = False) -> FetchResult:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / slug_for_url(url)
    meta_file = cache_file.with_suffix(".json")
    if cache_file.exists() and not force:
        status = 200
        if meta_file.exists():
            try:
                status = int(json.loads(meta_file.read_text()).get("status", 200))
            except (OSError, ValueError, TypeError):
                status = 200
        return FetchResult(url=url, status=status, from_cache=True, body=cache_file.read_text())

    stamp_file = cache_dir / ".last_request_at"
    now = time.monotonic()
    last = 0.0
    if stamp_file.exists():
        try:
            last = float(stamp_file.read_text())
        except ValueError:
            last = 0.0
    wait = max(0.0, delay - (now - last))
    if wait:
        time.sleep(wait)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    status = 0
    body_bytes = b""
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            status = int(response.status)
            body_bytes = response.read()
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body_bytes = exc.read()
    finally:
        stamp_file.write_text(str(time.monotonic()))

    body = body_bytes.decode("utf-8", errors="replace")
    cache_file.write_text(body)
    meta_file.write_text(json.dumps({"url": url, "status": status, "fetched_at": utc_now()}, indent=2))
    return FetchResult(url=url, status=status, from_cache=False, body=body)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def extract_links(html_text: str, base_url: str) -> list[str]:
    parser = LinkParser()
    parser.feed(html_text)
    links = []
    for href in parser.links:
        absolute = urllib.parse.urljoin(base_url, href)
        parsed = urllib.parse.urlparse(absolute)
        cleaned = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
        links.append(cleaned)
    return links


def discover_category_posts(cache_dir: Path, delay: float, force: bool) -> tuple[list[str], dict[str, Any]]:
    first = request_url(INDEXING_CATEGORY_URL, cache_dir, delay, force=force)
    links = extract_links(first.body, INDEXING_CATEGORY_URL)
    page_numbers = {1}
    for link in links:
        match = CATEGORY_PAGE_RE.match(link)
        if match:
            page_numbers.add(int(match.group(1)))
    max_page = max(page_numbers)

    page_urls = [INDEXING_CATEGORY_URL] + [
        f"{INDEXING_CATEGORY_URL}page/{page}/" for page in range(2, max_page + 1)
    ]
    post_urls: list[str] = []
    page_summaries: list[dict[str, Any]] = []

    for page_url in page_urls:
        result = first if page_url == INDEXING_CATEGORY_URL else request_url(
            page_url, cache_dir, delay, force=force
        )
        page_links = extract_links(result.body, page_url)
        page_posts = sorted({link for link in page_links if POST_URL_RE.match(link)})
        post_urls.extend(page_posts)
        page_summaries.append({
            "url": page_url,
            "status": result.status,
            "from_cache": result.from_cache,
            "post_count": len(page_posts),
        })

    return sorted(set(post_urls)), {
        "category_url": INDEXING_CATEGORY_URL,
        "archive_pages": page_summaries,
        "max_page": max_page,
        "deduped_post_count": len(set(post_urls)),
    }


def discover_sitemap_keyword_candidates(cache_dir: Path, delay: float, force: bool) -> list[str]:
    sitemap = request_url(SITEMAP_URL, cache_dir, delay, force=force)
    sitemap_urls = extract_sitemap_locs(sitemap.body)
    post_sitemaps = [
        url for url in sitemap_urls if re.search(r"/post-sitemap\d*\.xml$", url)
    ]
    post_urls: list[str] = []
    for sitemap_url in post_sitemaps:
        result = request_url(sitemap_url, cache_dir, delay, force=force)
        post_urls.extend(extract_sitemap_locs(result.body))
    keywords = (
        "index",
        "indexes",
        "indexing",
        "columnstore",
        "fragmentation",
        "fill-factor",
        "fillfactor",
    )
    return sorted({
        url
        for url in post_urls
        if POST_URL_RE.match(url)
        and any(keyword in urllib.parse.urlparse(url).path.lower() for keyword in keywords)
    })


def extract_sitemap_locs(xml_text: str) -> list[str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [node.text or "" for node in root.findall(".//sm:loc", namespace)]
    if not locs:
        locs = [node.text or "" for node in root.findall(".//loc")]
    return [normalize_space(loc) for loc in locs if normalize_space(loc)]


def analyze_article(url: str, html_text: str, source: str) -> dict[str, Any]:
    parser = ArticleParser()
    parser.feed(html_text)
    text = parser.text
    title = parser.title or url.rsplit("/", 2)[-2].replace("-", " ").title()
    lower = f"{title}\n{text}".lower()
    topics: list[dict[str, str]] = []
    for topic_id, (label, keywords, guidance) in TOPIC_RULES.items():
        hits = [keyword for keyword in keywords if keyword in lower]
        if hits:
            topics.append({
                "id": topic_id,
                "label": label,
                "guidance_signal": guidance,
                "matched_terms": sorted(set(hits))[:8],
            })

    words = WORD_RE.findall(text)
    return {
        "url": url,
        "title": title,
        "published": parser.published,
        "source": source,
        "word_count": len(words),
        "code_block_count": parser.code_blocks,
        "topics": topics,
        "derived_digest": build_digest(title, topics),
    }


def build_digest(title: str, topics: list[dict[str, str]]) -> str:
    if not topics:
        return (
            f"Use '{title}' as an indexing-related source to review manually before applying "
            "any query or index guidance."
        )
    labels = ", ".join(topic["label"] for topic in topics[:4])
    return (
        f"Use '{title}' as source evidence for {labels}; convert it into measured, "
        "Azure SQL-safe guidance before recommending DDL."
    )


def write_outputs(
    output_dir: Path,
    category_metadata: dict[str, Any],
    category_urls: list[str],
    sitemap_candidates: list[str],
    articles: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    category_set = set(category_urls)
    sitemap_set = set(sitemap_candidates)
    topic_counts = Counter(
        topic["id"]
        for article in articles
        for topic in article.get("topics", [])
    )
    manifest = {
        "generated_at": utc_now(),
        "source_site": BASE_URL,
        "source_scope": {
            "primary": INDEXING_CATEGORY_URL,
            "method": "public HTML category archive plus sitemap keyword gap scan",
            "robots_txt_observed": True,
            "crawl_delay_seconds": 5,
        },
        "category": category_metadata,
        "counts": {
            "category_posts": len(category_set),
            "sitemap_keyword_candidates": len(sitemap_set),
            "sitemap_candidates_not_in_category": len(sitemap_set - category_set),
            "deduped_source_urls": len(category_set | sitemap_set),
            "articles_analyzed": len(articles),
            "errors": len(errors),
        },
        "topic_counts": dict(sorted(topic_counts.items())),
        "category_urls": category_urls,
        "sitemap_keyword_candidates_not_in_category": sorted(sitemap_set - category_set),
        "errors": errors,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    (output_dir / "article-digests.json").write_text(
        json.dumps(articles, indent=2, sort_keys=True)
    )
    write_markdown_summary(output_dir / "article-digests.md", manifest, articles)


def write_markdown_summary(path: Path, manifest: dict[str, Any], articles: list[dict[str, Any]]) -> None:
    lines = [
        "# Brent Ozar Indexing Article Digests",
        "",
        "Derived local corpus for the SQL Optimizer skill. This file intentionally keeps",
        "short paraphrased digests and source URLs rather than copied article bodies.",
        "",
        f"- Generated: {manifest['generated_at']}",
        f"- Primary source: {manifest['source_scope']['primary']}",
        f"- Category posts: {manifest['counts']['category_posts']}",
        f"- Deduped source URLs: {manifest['counts']['deduped_source_urls']}",
        f"- Articles analyzed: {manifest['counts']['articles_analyzed']}",
        f"- Sitemap keyword candidates outside category: {manifest['counts']['sitemap_candidates_not_in_category']}",
        f"- Errors: {manifest['counts']['errors']}",
        "",
        "## Topic Counts",
        "",
    ]
    for topic_id, count in manifest["topic_counts"].items():
        label = TOPIC_RULES.get(topic_id, (topic_id, [], ""))[0]
        lines.append(f"- {label}: {count}")
    lines.extend(["", "## Articles", ""])
    for article in sorted(articles, key=lambda item: (item.get("published") or "", item["title"]), reverse=True):
        labels = ", ".join(topic["label"] for topic in article.get("topics", [])) or "Indexing"
        lines.extend([
            f"### {article['title']}",
            "",
            f"- URL: {article['url']}",
            f"- Published: {article.get('published') or 'unknown'}",
            f"- Source set: {article['source']}",
            f"- Topics: {labels}",
            f"- Digest: {article['derived_digest']}",
            "",
        ])
    path.write_text("\n".join(lines).rstrip() + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/brentozar-indexing"))
    parser.add_argument("--output-dir", type=Path, default=Path("sources/brentozar-indexing"))
    parser.add_argument("--delay", type=float, default=5.0)
    parser.add_argument("--force", action="store_true", help="Refetch even when cached.")
    parser.add_argument("--limit", type=int, default=0, help="Limit articles for testing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    category_urls, category_metadata = discover_category_posts(args.cache_dir, args.delay, args.force)
    sitemap_candidates = discover_sitemap_keyword_candidates(args.cache_dir, args.delay, args.force)

    category_set = set(category_urls)
    sitemap_set = set(sitemap_candidates)
    urls = sorted(category_set | sitemap_set)
    if args.limit:
        urls = urls[: args.limit]

    articles: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for index, url in enumerate(urls, start=1):
        print(f"[{index}/{len(urls)}] {url}", flush=True)
        try:
            result = request_url(url, args.cache_dir, args.delay, force=args.force)
            if result.status >= 400:
                errors.append({"url": url, "status": result.status, "error": "HTTP error"})
                continue
            source = "indexing-category"
            if url in category_set and url in sitemap_set:
                source = "indexing-category+sitemap-keyword"
            elif url in sitemap_set and url not in category_set:
                source = "sitemap-keyword"
            articles.append(analyze_article(url, result.body, source))
        except Exception as exc:  # pragma: no cover - safety for long scrape jobs
            errors.append({"url": url, "status": 0, "error": str(exc)})

    write_outputs(
        args.output_dir,
        category_metadata=category_metadata,
        category_urls=category_urls,
        sitemap_candidates=sitemap_candidates,
        articles=articles,
        errors=errors,
    )
    print(f"Wrote {args.output_dir / 'manifest.json'}")
    print(f"Wrote {args.output_dir / 'article-digests.json'}")
    print(f"Wrote {args.output_dir / 'article-digests.md'}")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
