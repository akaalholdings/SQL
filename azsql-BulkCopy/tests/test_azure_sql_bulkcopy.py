import importlib.util
import pathlib
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "azure_sql_bulkcopy.py"
SPEC = importlib.util.spec_from_file_location("azure_sql_bulkcopy", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class AzureSqlBulkCopyTests(unittest.TestCase):
    def test_validate_identifier_accepts_simple_name(self):
        self.assertEqual(MODULE.validate_identifier("OrderHeader"), "OrderHeader")

    def test_validate_identifier_rejects_unsafe_name(self):
        with self.assertRaises(ValueError):
            MODULE.validate_identifier("dbo.OrderHeader")

    def test_sql_type_string_handles_nvarchar(self):
        self.assertEqual(MODULE.sql_type_string("nvarchar", 100, 0, 0), "NVARCHAR(50)")

    def test_sql_type_string_handles_decimal(self):
        self.assertEqual(MODULE.sql_type_string("decimal", 0, 18, 4), "DECIMAL(18,4)")

    def test_chunk_file_path_is_predictable(self):
        base = pathlib.Path("/tmp/parquet")
        self.assertEqual(
            MODULE.chunk_file_path(base, 99, 200),
            pathlib.Path("/tmp/parquet/chunk_100_200.parquet"),
        )

    def test_state_round_trip(self):
        state = MODULE.State(
            source_schema="dbo",
            source_table="SourceTable",
            dest_schema="dbo",
            dest_table="DestTable",
            chunk_column="Id",
            chunk_size=1000,
            source_min=1,
            source_max=9999,
            source_row_count=9999,
            next_lower_bound=5000,
            chunks_completed=5,
            rows_downloaded=5000,
            rows_uploaded=5000,
            destination_prepared=True,
            started_at_utc="2026-03-25T00:00:00Z",
            updated_at_utc="2026-03-25T00:00:00Z",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = pathlib.Path(temp_dir) / "state.json"
            MODULE.save_state(state_path, state)

            config = MODULE.Config(
                source_connection_string="source",
                dest_connection_string="dest",
                source_schema="dbo",
                source_table="SourceTable",
                dest_schema="dbo",
                dest_table="DestTable",
                chunk_column="Id",
                chunk_size=1000,
                fetch_batch_size=100,
                bulkcopy_batch_size=100,
                table_mode="recreate",
                work_dir=pathlib.Path(temp_dir),
                delete_parquet_after_upload=True,
            )

            loaded = MODULE.load_state(state_path, config)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.next_lower_bound, 5000)
            self.assertTrue(loaded.destination_prepared)


if __name__ == "__main__":
    unittest.main()
