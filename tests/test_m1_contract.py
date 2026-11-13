"""Offline source-package checks. These are not substitutes for Docker or SQL integration tests."""
import os
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parents[1]


def read(p):
    return (ROOT / p).read_text()


def test_compose_mounts_exist():
    compose = read('docker-compose.yml')
    for p in ('source/postgres/00-databases.sql', 'source/postgres/01-init.sql',
              'source/postgres/seed.sql', 'source/simulator/Dockerfile',
              'metabase/bootstrap.py'):
        assert (ROOT / p).is_file(), p
        assert p in compose or p.endswith('Dockerfile') or p.endswith('bootstrap.py')


def test_expected_source_schema_and_simulator_queue():
    ddl = read('source/postgres/01-init.sql')
    assert 'CREATE TABLE core.merchants' in ddl
    assert 'CREATE TABLE core.transactions' in ddl
    assert 'CREATE TABLE simulator.pending_actions' in ddl
    assert 'ON DELETE CASCADE' in ddl
    for name in ('merchant_id', 'transaction_id', 'risk_level', 'owner_national_id',
                 'owner_mobile', 'card_bin', 'created_at', 'updated_at'):
        assert name in ddl
    assert ddl.count('REPLICA IDENTITY FULL') == 2
    assert ddl.count('EXECUTE FUNCTION core.touch_updated_at') == 2


def test_simulator_queue_is_atomic_and_is_not_captured():
    simulator = read('source/simulator/simulate.py')
    assert 'with conn.transaction():' in simulator
    assert 'INSERT INTO simulator.pending_actions' in simulator
    assert 'DELETE FROM simulator.pending_actions' in simulator
    assert 'UPDATE core.transactions SET status=' in simulator
    assert 'UPDATE core.transactions SET status=\'FAILED\' WHERE status=\'PENDING\'' not in simulator
    assert 'REPLICA IDENTITY FULL' not in simulator


def test_cards_are_five_with_no_unresolved_sql_templates():
    os.environ.setdefault('MB_ADMIN_EMAIL', 'unit-test@example.test')
    os.environ.setdefault('MB_ADMIN_PASSWORD', 'DummyPass123!')
    os.environ.setdefault('MB_SOURCE_PASSWORD', 'dummy')
    os.environ.setdefault('MB_CH_REPORTING_PASSWORD', 'dummy')
    path = ROOT / 'metabase/bootstrap.py'
    spec = importlib.util.spec_from_file_location('m1_bootstrap_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cards = module.parse_cards(ROOT / 'metabase/cards/baseline.sql')
    assert len(cards) == 5
    for _, sql in cards:
        compiled = module.compile_sql(sql)
        assert '{{' not in compiled
        assert 'SELECT' in compiled
    assert 'date_trunc(\'month\'' in cards[4][1]
    assert 'm.merchant_id ASC' in cards[2][1]


def test_metabase_bootstrap_and_benchmark_have_evidence_outputs():
    code = read('metabase/bootstrap.py')
    assert '/api/setup' in code
    assert '/api/dashboard/' in code
    assert '/api/card/' in code
    assert 'baseline_first_run.json' in code
    assert 'baseline_benchmark.json' in code


def test_versions_and_localhost_exposure():
    env = read('.env.example')
    assert 'POSTGRES_VERSION=16.4' in env
    assert 'CLICKHOUSE_VERSION=24.8' in env
    assert 'METABASE_VERSION=v0.58.40' in env
    assert '127.0.0.1:3000:3000' in read('docker-compose.yml')
