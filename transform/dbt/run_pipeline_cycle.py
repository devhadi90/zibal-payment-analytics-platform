#!/usr/bin/env python3
"""M5-C: one bounded, sequential pass through the existing checkpointed pipeline.

Run manually, or invoke periodically from the host. No daemon or new services.
Requires a single writer; do not run concurrently with another pipeline cycle.
"""
from __future__ import annotations

import argparse
import subprocess
import sys


def steps(skip_tests: bool, require_caught_up: bool):
    commands = [
        ('stage', ['python', 'run_staging_checkpointed.py', '--stream', 'both', '--drain']),
        ('current', ['python', 'run_current_checkpointed.py', '--stream', 'both', '--drain']),
        ('facts', ['python', 'run_fact_checkpointed.py', '--drain']),
        ('aggregate', ['python', 'run_daily_aggregates.py']),
    ]
    if not skip_tests:
        commands.append(('data quality', ['dbt', 'test', '--select', 'tag:m5_quality']))
    health = ['python', 'check_pipeline_health.py']
    if require_caught_up:
        health.append('--require-caught-up')
    commands.append(('health', health))
    return commands


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--skip-tests', action='store_true', help='omit quality tests on this cycle (not for final acceptance)')
    p.add_argument('--require-caught-up', action='store_true', help='strict raw-to-aggregate catchup; pause simulator first')
    args = p.parse_args()
    for name, command in steps(args.skip_tests, args.require_caught_up):
        print(f'M5C_STEP_START {name}', flush=True)
        subprocess.run(command, check=True)
        print(f'M5C_STEP_OK {name}', flush=True)
    print('M5C_PIPELINE_CYCLE_OK', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'M5C_PIPELINE_CYCLE_FAILED: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
