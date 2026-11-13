"""Offline tests for the host-side portable wrappers; Docker integration remains required."""
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load_script(filename):
    spec = importlib.util.spec_from_file_location("zibal_portable_" + filename, ROOT / "scripts" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class EntryPointTests(unittest.TestCase):
    def test_startup_orders_kafka_topics_before_consumer_and_simulator_last(self):
        mod = load_script("start.py")
        operations = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in mod.REQUIRED:
                file = root / name
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text("")
            (root / ".env.example").write_text("POSTGRES_USER=zibal\nPOSTGRES_PASSWORD=test\nPOSTGRES_DB=zibal\n")
            def compose(*args, **kwargs):
                operations.append(" ".join(args))
                return ""
            with mock.patch.object(mod, "ROOT", root), \
                    mock.patch.object(mod, "compose", side_effect=compose), \
                    mock.patch.object(mod, "running", return_value=False), \
                    mock.patch.object(mod, "check_one_shot"), \
                    mock.patch.object(mod, "ensure_topics", side_effect=lambda: operations.append("topics")), \
                    mock.patch.object(mod, "clickhouse", return_value=""), \
                    mock.patch.object(mod, "wait_for_connect"), \
                    mock.patch.object(mod, "register_connector"), \
                    mock.patch.object(mod, "await_snapshot"), \
                    mock.patch.object(mod, "add_installments"), \
                    mock.patch.object(mod.shutil, "which", return_value="docker"):
                mod.main([])
                self.assertTrue((root / ".env").exists())
        self.assertLess(operations.index("topics"), next(i for i, x in enumerate(operations)
                                                      if "up -d --build cdc-consumer" in x))
        self.assertIn("up -d simulator", operations[-1])
        self.assertTrue(any("run_pipeline_cycle.py --require-caught-up" in x for x in operations))

    def test_existing_connector_does_not_trigger_registration(self):
        mod = load_script("start.py")
        with mock.patch.object(mod, "running", side_effect=lambda service, *args: service == "cdc-consumer"), \
                mock.patch.object(mod, "http_json", return_value=(200, {"connector": {}})) as rest:
            mod.register_connector({})
            rest.assert_called_once_with("GET", "/connectors/zibal-postgres-cdc/status")

    def test_acceptance_range_and_order(self):
        mod = load_script("verify.py")
        operations = []
        def compose(*args, **kwargs):
            operations.append(" ".join(args))
            return ""  # Simulator stopped.
        with mock.patch.object(mod, "compose", side_effect=compose), \
                mock.patch.object(mod, "published_window", return_value=("2026-05-15", "2026-11-13")):
            mod.main([])
        assert operations[0] == "config --quiet"
        self.assertEqual(len([x for x in operations if "dbt-runner" in x]), 1)
        self.assertTrue(any("--start 2026-05-15 --end 2026-11-13" in x for x in operations))
        self.assertTrue(any("benchmark_m4.py --check --as-of 2026-11-13" in x for x in operations))

    def test_strict_date_format(self):
        mod = load_script("verify.py")
        self.assertEqual("2026-11-13", mod.validate_date("2026-11-13"))
        for value in ("2026-1-1", "2026-02-30", "not-a-date"):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                mod.validate_date(value)


if __name__ == "__main__":
    unittest.main()
