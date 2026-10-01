from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from scripts import copilot_index_manager_acceptance as acceptance

VALID_TRACE = """## Outcome
result_status=ok for appdb, window 14 days, objective cpu: 1 widen_index and
1 drop_index recommendation. Nothing was executed; no DDL was executed.

Trace:
1. check_runtime_status() -> package_version=2.4.0, review_workload_indexes is
   exposed; runtime_fingerprint=process-1, runtime_compatibility_fingerprint=compat-1,
   tool_schema_fingerprint=schema-1, sanitized_config_fingerprint=config-1.
2. list_databases() -> appdb; the user selected appdb.
3. check_capabilities(database_name=appdb) -> mcp_contract.workload_index_advisor=1.
4. review_workload_indexes(database_name=appdb, lookback_days=14, objective=cpu)
   -> result_status=ok.
5. recall_lessons was unavailable because learning is remote-disabled; the
   workflow continued unchanged.

## Coverage and gaps
100 queries analysed covering 86.2% of the workload objective. Gap, verbatim:
"Query Store capture mode AUTO skips infrequent queries; rare access paths may be missing".
Usage counters reset 41 days ago.

## Top improvements
R1 widen_index on Sales.Orders: widen FK_Sales_Orders_CustomerID to keys
(CustomerID, Status, OrderDate) include (Comments). Supporting queries 101
(31.2% of the workload) and 102 (1.6%). Confidence high. estimated_max_benefit_pct
30.9 is an upper-bound estimate from optimizer cost shares, not a promised
saving. Write penalty 0.12. The returned ddl and rollback_ddl are shown below,
inert, for DBA change control.

## Cleanup
R2 drop_index IX_Orders_Legacy on Sales.Orders, confidence medium: 0 reads and
48,210 writes since the counters reset. Usage counters reset on failover,
scaling, and restarts; the 41-day window should cover a full business cycle.
Query Store capture mode AUTO can miss rare queries (returned blocker). Confirm
no index hint, plan guide, or forced plan names it, and keep the returned
rollback_ddl with the change record.

## Per-table detail
Sales.Orders: FK_Sales_Orders_CustomerID serves the seek on CustomerID;
IX_Orders_Legacy has no reads in the window.

## Rewrite opportunities
dbo.EventLog.AccountNumber has an implicit conversion (convert_implicit) in
query 205. An index cannot fix it; route query 205 to sql-optimizer.

## Next steps
Prove R1 with sql-optimizer on a non-production copy using
start_tuning_session and benchmark_index_candidate for query 101. Only a
measured candidate goes to the human DBA's change control. Re-run the review
over an equal window after the change.
"""


class IndexManagerAcceptanceTests(unittest.TestCase):
    def test_valid_trace_passes(self) -> None:
        self.assertEqual(acceptance.validate_response(VALID_TRACE), [])

    def test_ddl_tool_calls_fail(self) -> None:
        for tool in ("execute_tsql_unrestricted", "create_test_index", "benchmark_index_candidate"):
            bad = VALID_TRACE + f"\n6. {tool}(database_name=appdb)\n"
            missing = acceptance.validate_response(bad)
            self.assertIn("no ddl or benchmark executed by the skill", missing, tool)
            self.assertIn("only approved calls", missing, tool)

    def test_learning_writes_fail(self) -> None:
        for tool in acceptance._FORBIDDEN_LEARNING_CALLS:
            bad = VALID_TRACE + f"\n{tool}(subject=index)\n"
            self.assertIn("recall-only learning", acceptance.validate_response(bad), tool)

    def test_review_before_gate_fails(self) -> None:
        bad = VALID_TRACE.replace(
            "3. check_capabilities(database_name=appdb) -> mcp_contract.workload_index_advisor=1.\n",
            "",
        )
        self.assertIn("gate before review", acceptance.validate_response(bad))

    def test_overclaiming_removal_fails(self) -> None:
        bad = VALID_TRACE.replace(
            "confidence medium:",
            "confidence medium and it is " + " ".join(("safe", "to", "drop")) + ":",  # noqa: FLY002
        )
        self.assertIn("no overclaims", acceptance.validate_response(bad))

    def test_unlabelled_estimate_fails(self) -> None:
        bad = VALID_TRACE.replace("is an upper-bound estimate", "is the saving")
        self.assertIn("estimate labelled as upper bound", acceptance.validate_response(bad))

    def test_missing_removal_discipline_fails(self) -> None:
        bad = VALID_TRACE.replace("plan guide", "plan")
        self.assertIn("removal discipline", acceptance.validate_response(bad))

    def test_sections_out_of_order_fail(self) -> None:
        cleanup = VALID_TRACE.index("## Cleanup")
        detail = VALID_TRACE.index("## Per-table detail")
        top = VALID_TRACE.index("## Top improvements")
        reordered = VALID_TRACE[:top] + VALID_TRACE[cleanup:detail] + VALID_TRACE[top:cleanup] + VALID_TRACE[detail:]
        self.assertIn("sections in order", acceptance.validate_response(reordered))

    def test_cli_prints_prompt_and_validates_files(self) -> None:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(acceptance.main(["--print-prompt"]), 0)
        self.assertIn("review_workload_indexes", output.getvalue())
        with tempfile.TemporaryDirectory() as directory:
            good = Path(directory) / "good.md"
            good.write_text(VALID_TRACE, encoding="utf-8")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(acceptance.main(["--response", str(good)]), 0)
            bad = Path(directory) / "bad.md"
            bad.write_text("nothing useful", encoding="utf-8")
            errors = io.StringIO()
            with redirect_stderr(errors):
                self.assertEqual(acceptance.main(["--response", str(bad)]), 1)
            self.assertIn("missing acceptance requirement", errors.getvalue())
            self.assertNotIn("nothing useful", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
