from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts import check_markdown_links, check_retired_paths, scan_content_secrets


class MarkdownLinkTests(unittest.TestCase):
    def test_checks_local_targets_and_fragments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs").mkdir()
            (root / "docs" / "guide.md").write_text("# Install\n", encoding="utf-8")
            (root / "README.md").write_text(
                "[good](docs/guide.md#install)\n[bad](docs/missing.md)\n",
                encoding="utf-8",
            )
            issues = check_markdown_links.check_links(root)
            self.assertEqual(len(issues), 1)
            self.assertEqual(issues[0].line, 2)

    def test_ignores_remote_and_fenced_code_links(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text(
                "[remote](https://example.com)\n[missing](#missing)\n```md\n[ignored](missing.md)\n```\n",
                encoding="utf-8",
            )
            issues = check_markdown_links.check_links(root)
            self.assertEqual(len(issues), 1)
            self.assertEqual(issues[0].reason, "heading fragment does not exist")


class RetiredPathTests(unittest.TestCase):
    def test_reports_retired_directory_and_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retired_name = "".join(("connect", "or"))
            retired_reference = "".join(("query", "_", "geneva", "_", "db"))
            (root / retired_name).mkdir()
            (root / "README.md").write_text(retired_reference + "\n", encoding="utf-8")
            issues = check_retired_paths.check_retired_paths(root)
            self.assertEqual(len(issues), 2)
            self.assertTrue(any(issue.line is None for issue in issues))
            self.assertTrue(any(issue.line == 1 for issue in issues))

    def test_clean_tree_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("Retired paths remain absent.\n", encoding="utf-8")
            self.assertEqual(check_retired_paths.check_retired_paths(root), [])


class SecretScanTests(unittest.TestCase):
    def test_reports_detector_and_location_without_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = "sk-" + "x" * 24
            (root / "README.md").write_text(f"key={secret}\n", encoding="utf-8")
            findings = scan_content_secrets.scan_files(root)
            self.assertEqual([(item.detector, item.line) for item in findings], [("openai-key", 1)])
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                scan_content_secrets.main(["--root", str(root)])
            self.assertNotIn(secret, output.getvalue())
            self.assertEqual(output.getvalue().strip(), "openai-key README.md:1")

    def test_reports_unquoted_secret_environment_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "settings.yml").write_text(
                "AZURE_CLIENT_SECRET=not-a-placeholder\n", encoding="utf-8"
            )
            findings = scan_content_secrets.scan_files(root)
            self.assertEqual([(item.detector, item.line) for item in findings], [("secret-assignment", 1)])

    def test_allows_placeholder_and_env_example(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".env.example").write_text(
                "AZURE_SQL_PASSWORD=replace-with-a-secret\n", encoding="utf-8"
            )
            (root / "README.md").write_text(
                "AZURE_SQL_MCP_BEARER_TOKEN=replace-with-a-long-random-token\n",
                encoding="utf-8",
            )
            self.assertEqual(scan_content_secrets.scan_files(root), [])


if __name__ == "__main__":
    unittest.main()
