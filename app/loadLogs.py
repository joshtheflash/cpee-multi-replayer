"""
loadLogs.py
------------
Parses XES-YAML process logs exported from the CPEE engine and loads
them into the SQLite database used by the replay system.
Each call, response, and instantiation event is stored as a row
in the selected table.
"""

import os
import json
import yaml
import sqlite3
from glob import glob
from app.db import dbManager

# Default paths (can be overridden by CLI arguments)
DEFAULT_LOG_DIR = os.path.join(os.path.dirname(__file__), "../logs/coopis2010")


# --------------------------------------------------------
# Database helpers
# --------------------------------------------------------

def _ensure_table(c: sqlite3.Cursor, table_name: str):
    dbManager.create_table(table_name)
    


def _insert_records(c: sqlite3.Cursor, records: list, table_name: str):
    """Insert collected records into the database."""
    if not records:
        return
    qtable = f'"{table_name}"'
    sql = f"""
        INSERT OR IGNORE INTO {qtable} (
            instance_uuid, endpoint_name, call_timestamp,
            input_params_json, activity_uuid, responses_json, event_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    c.executemany(sql, records)


# --------------------------------------------------------
# Log parsing logic
# --------------------------------------------------------

def _parse_event(doc):
    """Extract relevant fields from a single YAML event document."""
    if not isinstance(doc, dict) or "event" not in doc:
        return None

    event = doc["event"]
    return {
        "instance_uuid": event.get("cpee:instance"),
        "activity_uuid": event.get("cpee:activity_uuid"),
        "endpoint_name": event.get("concept:endpoint"),
        "timestamp": event.get("time:timestamp"),
        "lifecycle": event.get("cpee:lifecycle:transition"),
        "data": event.get("data") or event.get("raw") or []
    }


def _process_log_file(file_path: str, all_records: list):
    """Read one YAML log file and extract database records."""
    with open(file_path, "r") as f:
        docs_iter = yaml.load_all(f, Loader=yaml.CSafeLoader)

        # Skip metadata document
        next(docs_iter, None)

        current_calls = {}

        for doc in docs_iter:
            parsed = _parse_event(doc)
            if not parsed:
                continue

            inst_uuid = parsed["instance_uuid"]
            act_uuid = parsed["activity_uuid"]
            endpoint = parsed["endpoint_name"]
            timestamp = parsed["timestamp"]
            lifecycle = parsed["lifecycle"]
            data = parsed["data"]

            # Handle different event types
            if lifecycle == "activity/calling":
                input_params = {
                    d["name"]: d.get("value") for d in data
                    if isinstance(d, dict) and "name" in d
                }
                current_calls[(inst_uuid, act_uuid)] = {
                    "instance_uuid": inst_uuid,
                    "endpoint_name": endpoint,
                    "call_timestamp": timestamp,
                    "input_params_json": json.dumps(input_params),
                    "activity_uuid": act_uuid,
                    "responses": []
                }

            elif lifecycle in ["activity/receiving", "task/instantiation", "activity/done"]:
                key = (inst_uuid, act_uuid)
                if lifecycle == "activity/receiving":
                    # Ensure the call exists or create a placeholder
                    if key not in current_calls:
                        current_calls[key] = {
                            "instance_uuid": inst_uuid,
                            "endpoint_name": endpoint,
                            "call_timestamp": timestamp,
                            "input_params_json": "{}",
                            "activity_uuid": act_uuid,
                            "responses": []
                        }
                    current_calls[key]["responses"].append({
                        "timestamp": timestamp,
                        "lifecycle": lifecycle,
                        "data": data,
                    })

                elif lifecycle == "task/instantiation":
                    if key in current_calls:
                        current_calls[key]["instantiation"] = "true"

                elif lifecycle == "activity/done":
                    call = current_calls.pop(key, None)
                    if not call:
                        continue  # skip if nothing to finalize
                    event_type = "instantiation" if "instantiation" in call else "call"
                    all_records.append((
                        call["instance_uuid"],
                        call["endpoint_name"],
                        call["call_timestamp"],
                        call["input_params_json"],
                        call["activity_uuid"],
                        json.dumps(call["responses"]),
                        event_type
                    ))



# --------------------------------------------------------
# Main ingestion routine
# --------------------------------------------------------

def _ingest_logs(logs_dir: str, table_name: str, clear_first: bool = False, chunk_size: int = 10000):
    """Read all logs from directory and insert into SQLite."""
    logs_dir = logs_dir or DEFAULT_LOG_DIR
    conn = dbManager.get_connection()
    c = conn.cursor()

    _ensure_table(c, table_name)
    dbManager.set_setting("last_loaded_directory", logs_dir)

    if clear_first:
        c.execute(f"DROP TABLE IF EXISTS '{table_name}'")
        _ensure_table(c, table_name)

    all_records = []

    for log_file in glob(os.path.join(logs_dir, "*.xes.yaml")):
        _process_log_file(log_file, all_records)

        # Insert in chunks to save memory
        if len(all_records) >= chunk_size:
            _insert_records(c, all_records, table_name)
            all_records.clear()

    # Insert remaining records
    _insert_records(c, all_records, table_name)
    conn.commit()
    conn.close()


# --------------------------------------------------------
# Public interface
# --------------------------------------------------------

def parse_logs(logs_dir: str = DEFAULT_LOG_DIR, table_name: str = "calls"):
    """Parse logs and replace existing table contents."""
    _ingest_logs(logs_dir, table_name, clear_first=True)


def append_logs(logs_dir: str = DEFAULT_LOG_DIR, table_name: str = "calls"):
    """Append logs without removing existing data."""
    _ingest_logs(logs_dir, table_name, clear_first=False)
