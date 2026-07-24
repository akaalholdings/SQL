from __future__ import annotations

import unittest

from scripts import copilot_optimizer_acceptance as acceptance

REWRITE = """SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE o.CreatedAt >= @TargetDate
  AND o.CreatedAt < DATEADD(day, 1, CONVERT(datetime2(7), @TargetDate));"""

CAST_REWRITE = """SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE o.CreatedAt >= CAST(@TargetDate AS datetime2(7))
  AND o.CreatedAt < DATEADD(day, 1, CAST(@TargetDate AS datetime2(7)));"""


def valid_response(
    *details: str,
    sql: str = REWRITE,
    outcome: str = "static candidate",
) -> str:
    detail_text = "\n".join(details)
    return f"""Outcome: {outcome}.
Stopping reason: MCP is unavailable, so this run remains in static mode.
Semantic contract: OrderId is bigint NOT NULL and CreatedAt is datetime2(7) NOT NULL.
@TargetDate is date, restricted to 2000-01-01 through 9999-12-30 so the exclusive
upper bound remains representable. Preserve output types, duplicates, NULL behavior,
and unordered rows. The rewrite is a static candidate and unmeasured.
{detail_text}
Experiment leaderboard:
- Rewrite candidate: unmeasured.
- Prior index: regressed, 12 percent slower, cleanup confirmed.
Rejected experiments: reject only that index; continue to the next experiment.
```sql
{sql}
```
Evidence gaps: no plan, runtime timing, reads, or other metrics were collected.
Deployment: none; this result is not deployable.
Next evidence: collect the plan and benchmark the rewrite without inventing metrics.
"""


class CopilotOptimizerAcceptanceTests(unittest.TestCase):
    def test_static_rewrite_without_plan(self) -> None:
        response = valid_response("MCP is unavailable, so no plan or timing is claimed.")

        self.assertEqual(acceptance.validate_response(response), [])
        self.assertEqual(acceptance.extract_sql_blocks(response), (REWRITE,))

    def test_first_index_slower_then_continue(self) -> None:
        response = valid_response(
            "The first isolated index was 12 percent slower and cleanup was confirmed."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_leaderboard_requires_rewrite_and_index_rows(self) -> None:
        response = valid_response().replace(
            "- Rewrite candidate: unmeasured.\n",
            "",
        )

        self.assertIn(
            "complete static leaderboard",
            acceptance.validate_response(response),
        )

    def test_static_acceptance_rejects_contradictory_winner_outcome(self) -> None:
        response = valid_response().replace(
            "Outcome: static candidate.",
            "Outcome: deployable winner.",
        )

        self.assertIn("outcome", acceptance.validate_response(response))

    def test_actionable_next_steps_are_a_continuation_and_evidence_plan(self) -> None:
        response = valid_response().replace(
            "Rejected experiments: reject only that index; continue to the next experiment.",
            "Rejected experiments: the prior index is rejected.",
        ).replace(
            "Next evidence: collect the plan and benchmark the rewrite without inventing metrics.",
            "Next steps:\n1. Start a performance case.\n2. Benchmark the rewrite.",
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_negated_next_steps_do_not_count_as_continuation(self) -> None:
        response = valid_response().replace(
            "Rejected experiments: reject only that index; continue to the next experiment.",
            "Rejected experiments: reject only that index; do not continue.",
        ).replace(
            "Next evidence: collect the plan and benchmark the rewrite without inventing metrics.",
            "Next steps: do not benchmark or test another candidate.",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_invented_static_rewrite_measurements_are_rejected(self) -> None:
        response = (
            valid_response()
            + "\nThe rewrite measured 1 ms, improved 90 percent, and is deployable.\n"
        )

        self.assertIn(
            "no invented measurements",
            acceptance.validate_response(response),
        )

    def test_candidate_alias_cannot_hide_invented_measurements(self) -> None:
        response = valid_response() + "\nCandidate A ran in 1 ms and was 90 percent faster.\n"

        self.assertIn(
            "no invented measurements",
            acceptance.validate_response(response),
        )

    def test_non_sql_fence_cannot_hide_invented_measurements(self) -> None:
        response = valid_response() + "\n```text\nCandidate A ran in 1 ms.\n```\n"

        self.assertIn(
            "no invented measurements",
            acceptance.validate_response(response),
        )

    def test_invented_index_percentage_is_rejected(self) -> None:
        response = valid_response().replace("12 percent slower", "8 percent slower")

        missing = acceptance.validate_response(response)
        self.assertIn("no invented measurements", missing)
        self.assertIn("complete static leaderboard", missing)

    def test_leading_cleanup_negation_is_rejected(self) -> None:
        response = valid_response().replace(
            "cleanup confirmed",
            "no cleanup was confirmed",
        )

        self.assertIn(
            "losing index recorded and cleaned",
            acceptance.validate_response(response),
        )

    def test_no_next_experiment_is_not_continuation(self) -> None:
        response = valid_response().replace(
            "continue to the next experiment",
            "there is no next experiment",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_would_not_continue_is_not_continuation(self) -> None:
        response = valid_response().replace(
            "continue to the next experiment",
            "would not continue",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_blended_leaderboard_record_is_rejected(self) -> None:
        response = valid_response().replace(
            "- Rewrite candidate: unmeasured.\n"
            "- Prior index: regressed, 12 percent slower, cleanup confirmed.",
            "- Rewrite/index candidate: static, regressed, 12 percent slower, "
            "cleanup confirmed.",
        )

        self.assertIn(
            "complete static leaderboard",
            acceptance.validate_response(response),
        )

    def test_no_index_was_rejected_is_not_affirmative(self) -> None:
        response = valid_response().replace(
            "reject only that index",
            "no index was rejected",
        )

        self.assertIn(
            "losing index recorded and cleaned",
            acceptance.validate_response(response),
        )

    def test_accepts_explicit_unavailable_evidence_wording(self) -> None:
        response = valid_response().replace(
            "Evidence gaps: no plan, runtime timing, reads, or other metrics were collected.",
            "Plan and performance evidence could not be collected; no MCP evidence is available.",
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_timeout_and_cleanup_do_not_end_the_session(self) -> None:
        response = valid_response(
            "A timed-out index benchmark is inconclusive; its lease is cleanup_required."
        )

        self.assertEqual(acceptance.validate_response(response), [])
        self.assertIn("cleanup_required", response)
        self.assertIn("continue", response.casefold())

    def test_unconfirmed_cleanup_does_not_satisfy_acceptance(self) -> None:
        response = valid_response().replace(
            "cleanup confirmed",
            "cleanup was not confirmed",
        )

        self.assertIn(
            "losing index recorded and cleaned",
            acceptance.validate_response(response),
        )

    def test_negated_continuation_does_not_satisfy_acceptance(self) -> None:
        response = valid_response().replace(
            "continue to the next experiment",
            "do not continue",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_cannot_continue_does_not_satisfy_acceptance(self) -> None:
        response = valid_response().replace(
            "continue to the next experiment",
            "cannot continue",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_unable_to_continue_does_not_satisfy_acceptance(self) -> None:
        response = valid_response().replace(
            "continue to the next experiment",
            "unable to continue",
        )

        self.assertIn("session continues", acceptance.validate_response(response))

    def test_negated_index_rejection_does_not_satisfy_acceptance(self) -> None:
        response = valid_response().replace(
            "reject only that index",
            "do not reject this index",
        )

        self.assertIn(
            "losing index recorded and cleaned",
            acceptance.validate_response(response),
        )

    def test_negated_rewrite_rejection_does_not_cancel_index_rejection(self) -> None:
        response = valid_response(
            "Reject this index; do not reject the independent rewrite candidate."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_static_continuation_survives_measured_continuation_negation(self) -> None:
        response = valid_response(
            "Measured work cannot continue because MCP is unavailable; "
            "continue statically with the next candidate."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_duplicate_and_order_semantics_are_prose_not_sql(self) -> None:
        response = valid_response(
            "Compare duplicate-aware multisets when order is not contractual; compare the "
            "ordered sequence and ties when it is contractual."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_parameter_bucket_matrix_is_named(self) -> None:
        response = valid_response(
            "Test common, rare, NULL, and boundary parameter buckets when valid."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_optional_parameter_join_fanout_and_aggregate_scenarios(self) -> None:
        response = valid_response(
            "Preserve optional-parameter NULL semantics, join fan-out, aggregate and "
            "window behavior before benchmarking."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_view_definition_and_plan_diagnostic_scenarios(self) -> None:
        response = valid_response(
            "Record the view definition and rollback. Inspect residual predicates, "
            "implicit conversions, and key lookups in the plan."
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_no_change_report_still_has_a_complete_static_candidate(self) -> None:
        response = valid_response(
            "All pattern families were considered; evidence did not support a "
            "deployable winner.",
            outcome="no_change",
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_accepts_explicit_datetime2_cast_in_lower_bound(self) -> None:
        response = valid_response(sql=CAST_REWRITE)

        self.assertEqual(acceptance.validate_response(response), [])

    def test_accepts_unmeasured_label_in_leading_sql_comment(self) -> None:
        response = valid_response(
            sql="-- Static candidate, unmeasured\n" + REWRITE
        ).replace(
            "The rewrite is a static candidate and unmeasured.",
            "The rewrite is a static candidate.",
        ).replace(
            "- Rewrite candidate: unmeasured.",
            "- Rewrite candidate: static.",
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_unmeasured_losing_index_does_not_label_rewrite(self) -> None:
        response = valid_response().replace(
            "The rewrite is a static candidate and unmeasured.",
            "The rewrite is measured and deployable.",
        ).replace(
            "- Rewrite candidate: unmeasured.",
            "- Rewrite candidate: measured.",
        ).replace(
            "Prior index: regressed, 12 percent slower, cleanup confirmed.",
            "Prior index: unmeasured, regressed, 12 percent slower, cleanup confirmed.",
        )

        self.assertIn("unmeasured label", acceptance.validate_response(response))

    def test_rejects_invalid_datetime2_precision(self) -> None:
        response = valid_response(sql=CAST_REWRITE.replace("datetime2(7)", "datetime2(8)"))

        self.assertIn("rewritten query structure", acceptance.validate_response(response))

    def test_rejects_missing_typed_date_contract(self) -> None:
        response = valid_response().replace(
            "OrderId is bigint NOT NULL and CreatedAt is datetime2(7) NOT NULL.\n"
            "@TargetDate is date, restricted to 2000-01-01 through 9999-12-30 so the exclusive\n"
            "upper bound remains representable. ",
            "",
        )

        self.assertIn(
            "typed date boundary contract",
            acceptance.validate_response(response),
        )

    def test_rejects_unrepresentable_maximum_date_domain(self) -> None:
        response = valid_response().replace("9999-12-30", "9999-12-31")

        self.assertIn(
            "typed date boundary contract",
            acceptance.validate_response(response),
        )

    def test_sql_identifiers_do_not_satisfy_semantic_contract_prose(self) -> None:
        response = valid_response().replace(
            "Preserve output types, duplicates, NULL behavior,\n"
            "and unordered rows.",
            "Preserve output types and duplicates.",
        )

        self.assertIn("semantic contract", acceptance.validate_response(response))

    def test_sql_text_does_not_satisfy_typed_contract_prose(self) -> None:
        response = valid_response().replace(
            "OrderId is bigint NOT NULL and CreatedAt is datetime2(7) NOT NULL.\n",
            "",
        )

        self.assertIn(
            "typed date boundary contract",
            acceptance.validate_response(response),
        )

    def test_multiline_typed_contract_is_accepted(self) -> None:
        response = valid_response().replace(
            "OrderId is bigint NOT NULL and CreatedAt is datetime2(7) NOT NULL.",
            "OrderId:\n  bigint NOT NULL\nCreatedAt:\n  datetime2(7) NOT NULL.",
        ).replace(
            "@TargetDate is date, restricted",
            "@TargetDate:\n  date\nrestricted",
        )

        self.assertEqual(acceptance.validate_response(response), [])

    def test_rejects_plan_only_give_up_response(self) -> None:
        response = "The index was slower. Please provide an execution plan."

        missing = acceptance.validate_response(response)

        self.assertIn("concrete SQL code block", missing)
        self.assertIn("unmeasured label", missing)
        self.assertIn("session continues", missing)

    def test_rejects_keyword_only_incomplete_report(self) -> None:
        response = f"""Semantic contract: preserve duplicates, NULLs, and order.
The rewrite is unmeasured. The index regressed; continue.
```sql
{REWRITE}
```
"""

        missing = acceptance.validate_response(response)

        self.assertIn("outcome", missing)
        self.assertIn("explicit static stopping reason", missing)
        self.assertIn("complete static leaderboard", missing)
        self.assertIn("evidence gaps", missing)
        self.assertIn("not deployable", missing)

    def test_prose_inside_a_sql_fence_does_not_satisfy_sql(self) -> None:
        response = valid_response(
            sql="""This is a SARGable rewrite. It preserves duplicates and NULLs.
The next experiment should collect a plan."""
        )

        missing = acceptance.validate_response(response)

        self.assertIn("concrete SQL code block", missing)
        self.assertIn("rewritten query structure", missing)

    def test_sql_shaped_prefix_with_trailing_prose_does_not_satisfy_sql(self) -> None:
        missing = acceptance.validate_response(
            valid_response(sql=REWRITE + "\nThis is explanatory prose, not SQL.")
        )

        self.assertIn("rewritten query structure", missing)

    def test_quoted_comment_marker_cannot_change_validated_sql(self) -> None:
        sql = REWRITE.rstrip(";") + "\n-- a comment containing '--'"

        missing = acceptance.validate_response(valid_response(sql=sql))

        self.assertIn("rewritten query structure", missing)

    def test_original_query_does_not_count_as_rewrite(self) -> None:
        original = """SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE CONVERT(date, o.CreatedAt) = @TargetDate;"""

        missing = acceptance.validate_response(valid_response(sql=original))

        self.assertIn("concrete SQL code block", missing)
        self.assertIn("SARGable lower bound", missing)

    def test_non_sql_fence_is_not_extracted(self) -> None:
        response = valid_response().replace("```sql", "```text", 1)

        self.assertEqual(acceptance.extract_sql_blocks(response), ())
        self.assertIn("concrete SQL code block", acceptance.validate_response(response))

    def test_multiple_blocks_use_the_valid_rewritten_query(self) -> None:
        response = valid_response().replace(
            "```sql\n" + REWRITE + "\n```",
            "```sql\nThe original query is not a candidate.\n```\n"
            "```sql\n" + REWRITE + "\n```",
        )

        self.assertEqual(acceptance.validate_response(response), [])
        self.assertEqual(len(acceptance.extract_sql_blocks(response)), 2)


if __name__ == "__main__":
    unittest.main()
