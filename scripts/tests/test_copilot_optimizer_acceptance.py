from __future__ import annotations

import unittest

from scripts import copilot_optimizer_acceptance


class CopilotOptimizerAcceptanceTests(unittest.TestCase):
    def test_accepts_rewrite_that_continues_after_losing_index(self) -> None:
        response = """Semantic contract: preserve duplicates, NULL, and unordered rows.
The prior index is regressed and rejected; continue to the next experiment.
Winning SQL (unmeasured):
```sql
SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE o.CreatedAt >= @TargetDate
  AND o.CreatedAt < DATEADD(day, 1, CONVERT(datetime2, @TargetDate));
```
"""

        self.assertEqual(
            copilot_optimizer_acceptance.validate_response(response),
            [],
        )

    def test_rejects_plan_only_give_up_response(self) -> None:
        response = "The index was slower. Please provide an execution plan."

        missing = copilot_optimizer_acceptance.validate_response(response)

        self.assertIn("concrete SQL code block", missing)
        self.assertIn("unmeasured label", missing)
        self.assertIn("session continues", missing)

    def test_accepts_next_evidence_steps_as_continuation(self) -> None:
        response = """Semantic contract: preserve duplicates, NULL, and unordered rows.
The prior index is regressed and rejected.
Concrete rewrite (unmeasured):
```sql
SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE o.CreatedAt >= @TargetDate
  AND o.CreatedAt < DATEADD(day, 1, @TargetDate);
```
Next evidence / experiment steps: benchmark the rewrite before another index.
"""

        self.assertEqual(
            copilot_optimizer_acceptance.validate_response(response),
            [],
        )

    def test_accepts_explicit_datetime2_cast_in_lower_bound(self) -> None:
        response = """Semantic contract: preserve duplicates, NULL, and unordered rows.
The prior index is regressed and rejected; continue to the next experiment.
Concrete rewrite (unmeasured):
```sql
SELECT o.OrderId, o.CreatedAt
FROM dbo.SyntheticOrders AS o
WHERE o.CreatedAt >= CAST(@TargetDate AS datetime2)
  AND o.CreatedAt < DATEADD(day, 1, CAST(@TargetDate AS datetime2));
```
"""

        self.assertEqual(
            copilot_optimizer_acceptance.validate_response(response),
            [],
        )


if __name__ == "__main__":
    unittest.main()
