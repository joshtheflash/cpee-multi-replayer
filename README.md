# CPEE Multi-Log Replayer

## Repository Layout

- `app/replay.py` – FastAPI application that matches incoming CPEE requests to stored calls and replays the recorded responses.
- `app/loadLogs.py` – Log ingestion utilities that parse `.xes.yaml` files and populate the database.
- `app/db/db_cli.py` – Typer-based CLI for creating tables, loading logs, inspecting data, and managing replay settings.
- `app/db/dbManager.py` – Helper module managing database interactions.
- `logs/` – Sample process logs for experimentation; defaults point to `logs/coopis2010`.

## Prerequisites

- Python 3.10+
- SQLite (bundled with Python)
- `pip` for managing Python packages

## Installation

Install the replayer from PyPI (or any compatible index) in a single step:

```bash
pip install cpee-multi-replay
```

The install exposes two convenience commands:

- `cpee-db-cli` – invoke any of the utility helpers defined in `app.db.dbManager`.
- `cpee-replay-daemon` – launch the background Uvicorn daemon defined in `run_replay_daemon.py`.

Runtime data (SQLite databases and settings) live under `~/.cpee_multi_replay` by default. Override that directory with the `CPEE_REPLAY_HOME` environment variable if you prefer a custom location.

## Virtual Environment & Dependencies

Set up an isolated environment (recommended) and install the project requirements:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # or `pip install -e .` for editable installs
```

## Loading Process Logs

The loader expects log files ending in `.xes.yaml`. Use the CLI to create a table and ingest logs into the bundled SQLite database (`app/db/events.db`).

```bash
# Create or re-use a table named "calls"
python -m app.db.db_cli create-table calls

# Parse logs (existing data in the table is replaced)
python -m app.db.db_cli load-logs logs/coopis2010 calls

# Append instead of replacing
python -m app.db.db_cli load-logs logs/coopis2010 calls --append
```

Useful inspection commands:

```bash
python -m app.db.db_cli list-tables
python -m app.db.db_cli metadata calls
python -m app.db.db_cli list-rows calls --limit 5
```

To control which table the replay service uses, update the persisted setting:

```bash
python -m app.db.db_cli set-replay-table calls
```

## Running the Replay API

Start the FastAPI service from the project root:

```bash
uvicorn app.replay:app --reload --host 0.0.0.0 --port 8000
```

After installing the project (or from the repo directly) you can also use the helper CLI:

```bash
python server.py start  # or just `server start` after `pip install`
```

## Additional Notes

- Runtime settings (e.g., `active_table`) are stored in the `_settings` table inside `events.db`.
- `loadLogs.py` can also be imported as a module for custom pipelines via `parse_logs` and `append_logs`.
- Sample logs under `logs/` are safe to modify; keep backups if you plan to overwrite them.
