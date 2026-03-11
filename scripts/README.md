# run_exp.py README

This document explains how to use [run_exp.py](run_exp.py) to run Minerva benchmark experiments.

## What run_exp.py does

[run_exp.py](run_exp.py) automates experiment execution by:

- Loading an experiment template JSON.
- Loading a list of server IPs.
- Generating one or more experiment configurations.
- Writing runtime configs to [configs/minerva_config.json](configs/minerva_config.json) and [temp](temp).
- Starting/stopping DB servers using [start_server.py](start_server.py).
- Running the benchmark client binary.
- Saving per-run logs and a CSV summary under [results](results).

It supports TPCC and YCSB benchmark templates and can vary one field at a time across experiments.

## Prerequisites

- Minerva project is built.
- Client binary exists at:
  - `src/Client/bin/Release/net9.0/Client`
- Remote server orchestration works via [start_server.py](start_server.py):
  - `send_config(...)`
  - `start_server(...)`
  - `stop_server(...)`
- Optional latency simulation script exists and is executable:
  - [network_sim.sh](network_sim.sh)

## Input files

### 1) Template JSON

Pass a template JSON path as the first argument.

The script expects this structure:

- `BenchmarkConfig`
  - `benchmark_type`: `TPCC` or `YCSB`
  - `duration`: benchmark duration
  - `clients`: integer or list (variable field)
  - `servers`: integer or list (number of servers)
- `MinervaConfig`
  - `SolverExact`: bool or list (variable field)
  - `LocalEpochInterval`: int or list (variable field)
  - `CoordinatorGlobalEpochInterval`: int or list (variable field)
- `TPCCConfig` (required when benchmark type is TPCC)
  - `NumWarehouse`
- `YCSBConfig` (required when benchmark type is YCSB)
  - `YCSB_type` (value or list)
  - `contention_ratio` (value or list)
  - `transaction_size`
  - `key_size`
  - `value_size`
  - `record_count`
- Optional top-level `Latency`
  - List of latency tuples, for example: `[[0, 0], [5, 1], [10, 2]]`
  - Each tuple is `[delay_ms, jitter_ms]`

Important: only one variable field is supported at a time (one list-valued field).

### 2) IP list file

Second argument is a text file with one IP per line.

Example:

```text
10.0.0.1
10.0.0.2
10.0.0.3
```

## Usage

Run from the project root:

```bash
python3 scripts/run_exp.py <template_json> <ip_list>
```

Examples:

```bash
python3 scripts/run_exp.py scripts/example.json scripts/ips.txt
python3 scripts/run_exp.py Experiments/sample_tpcc.json scripts/ips.txt --probe
python3 scripts/run_exp.py Experiments/sample_ycsb.json scripts/ips.txt --single
python3 scripts/run_exp.py Experiments/sample_ycsb.json scripts/ips.txt --auto
```

## Modes and flags

- Default mode:
  - Runs all generated experiments.
  - Repeats each experiment 5 times.
- `--single` (`-s`):
  - Runs each experiment once.
- `--probe` (`-p`):
  - Uses client-count sweep `[200, 400, ..., 3000]`.
  - Runs once per point.
- `--auto` (`-a`):
  - For each variable-field experiment:
    1. Runs probe sweep to find best client count by highest `Overall Throughput`.
    2. Runs full experiment with optimal clients for 5 repetitions.

`--auto` cannot be combined with `--probe` or `--single`.

## Variable-field behavior

The script detects one list-valued field and expands experiments accordingly:

- Benchmark field (`clients`, `YCSB_type`, `contention_ratio`):
  - 1 Minerva config -> N experiment configs
- Shared field (`servers`):
  - N Minerva configs -> N experiment configs
- Minerva field (`SolverExact`, `LocalEpochInterval`, `CoordinatorGlobalEpochInterval`):
  - N Minerva configs -> 1 benchmark structure per value
- Network field (`Latency`):
  - Configs stay the same; latency is applied at runtime per experiment

## Output

Each run creates a timestamped results directory:

- `scripts/results/<template_name>_<YYYYMMDD_HHMMSS>/`

Contains:

- `exp_<idx>_...txt` files with per-repetition raw output.
- `summary.csv` with aggregated metrics:
  - variable value
  - average median latency (ms)
  - average max throughput (txs/sec)
  - std dev of max throughput

## Notes

- [run_exp.py](run_exp.py) rewrites [configs/minerva_config.json](configs/minerva_config.json).
- Temporary benchmark configs are generated in [temp](temp).
- During experiment execution, servers are restarted between repetitions.
- If `Latency` is used, latency is applied before run(s) and removed after.
- In non-single mode, logs may be deleted by server stop logic unless explicitly preserved.

## Troubleshooting

- Client binary not found:
  - Build the client in Release mode and verify path:
    - `src/Client/bin/Release/net9.0/Client`
- Server start/stop failures:
  - Verify SSH/connectivity and [start_server.py](start_server.py) behavior.
- No throughput parsed in probe mode:
  - Confirm client output contains `Results for Type All` and `Overall Throughput` lines.
- Latency script issues:
  - Check executable permission and dependencies for [network_sim.sh](network_sim.sh).
