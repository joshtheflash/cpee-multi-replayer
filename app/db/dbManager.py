"""
dbManager.py
-------------
Provides a simple runtime interface to the SQLite database used by the replay system.
Includes connection handling and persistent key–value settings storage.
"""

import sqlite3
import os
import json
from pathlib import Path

from app import loadLogs as logLoader

APP_HOME = Path(os.environ.get("CPEE_REPLAY_HOME", Path.home() / ".cpee_multi_replay")).expanduser()
DB_DIR = APP_HOME / "db"
CONFIG_FILE = APP_HOME / "_config.json"
DEFAULT_DB = DB_DIR / "events.db"
DB_PATH = str(DEFAULT_DB)


def _ensure_storage_dirs() -> None:
    """Create runtime directories for config and database files."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)


_ensure_storage_dirs()

# Default schema for replay event tables
DEFAULT_TABLE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS {table_name} (
    instance_uuid TEXT,
    activity_uuid TEXT,
    endpoint_name TEXT,
    call_timestamp TEXT,
    input_params_json TEXT,
    responses_json TEXT,
    event_type TEXT,
    UNIQUE (instance_uuid, activity_uuid, endpoint_name, input_params_json)
)
'''

def _read_config() -> dict:
    """Load global DB config from disk."""
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {}

def _write_config(data: dict) -> None:
    """Save global DB config to disk."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(data, indent=2))

def set_active_db(path: str) -> None:
    """Persistently mark the given path as the active database."""
    cfg = _read_config()
    cfg["active_db_path"] = os.path.abspath(path)
    _write_config(cfg)

def get_active_db() -> str:
    """Return the active DB path (falls back to default)."""
    cfg = _read_config()
    path = cfg.get("active_db_path")
    if path:
        return path

    default_path = str(DEFAULT_DB)
    set_active_db(default_path)
    return default_path


def setup(db_name: str = "events.db") -> str:
    global DB_PATH

    DB_DIR.mkdir(parents=True, exist_ok=True)

    db_name = f"{db_name}.db" if not db_name.endswith(".db") else db_name
    db_path = Path(db_name)
    if not db_path.is_absolute():
        db_path = DB_DIR / db_path.name
    db_path = db_path.resolve()

    DB_PATH = str(db_path)
    set_active_db(DB_PATH)

    # Create _settings table
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

    create_table("default_events")
    print(f"[dbManager] New database created at: {DB_PATH}")
    return DB_PATH


def quote_ident(name: str) -> str:
    """Safely quote SQLite identifiers (e.g., table names)."""
    return '"' + name.replace('"', '""') + '"'


def get_connection() -> sqlite3.Connection:
    """
    Open a SQLite connection with predefined pragmas for reliability and performance.
    """
    db_path = get_active_db()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=2, isolation_level=None)
    conn.execute("PRAGMA busy_timeout = 2000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def set_setting(key: str, value: str) -> None:
    """
    Store or update a persistent runtime setting.
    Example: set_setting('active_table', 'calls')
    """
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute(
            "INSERT OR REPLACE INTO _settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()

def set_db_path(path: str) -> None:
    """
    Update the global database path to a new SQLite file.
    """
    global DB_PATH
    DB_PATH = os.path.abspath(path)
    set_active_db(DB_PATH)

def get_setting(key: str) -> str | None:
    """
    Retrieve a stored setting value.
    Returns None if the key does not exist.
    Example: get_setting('active_table')
    """
    with get_connection() as conn:
        cursor = conn.execute("SELECT value FROM _settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else None


def clear_settings() -> None:
    """
    Remove all entries from the _settings table.
    Useful for resetting runtime state during testing.
    """
    with get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS _settings")
        conn.commit()


def show_config() -> dict:
    """Return current runtime configuration details for inspection."""
    cfg = _read_config()
    active_db = cfg.get("active_db_path") or str(DEFAULT_DB)
    db_path = Path(active_db)
    return {
        "app_home": str(APP_HOME),
        "config_file": str(CONFIG_FILE),
        "config_exists": CONFIG_FILE.exists(),
        "active_db_path": str(db_path),
        "database_exists": db_path.exists(),
    }

def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    with get_connection() as conn:
        cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def create_table(table_name: str) -> None:
    """Create a table using the default schema unless a custom one is provided."""
    qname = quote_ident(table_name)
    index_name = quote_ident(f"{table_name}_endpoint_params_idx")
    with get_connection() as conn:
        conn.execute(DEFAULT_TABLE_SCHEMA.format(table_name=qname))
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {qname} (endpoint_name, input_params_json)"
        )
        conn.commit()


def list_tables() -> list[tuple[str, int | None]]:
    """Return a list of (table_name, row_count) pairs for user tables."""
    with get_connection() as conn:
        cursor = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        tables: list[tuple[str, int | None]] = []
        for (name,) in cursor.fetchall():
            qname = quote_ident(name)
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {qname}").fetchone()[0]
            except sqlite3.Error:
                count = None
            tables.append((name, count))
        return tables


def get_table_metadata(table_name: str) -> tuple[list[tuple], int]:
    """Fetch column metadata and row count for a table."""
    with get_connection() as conn:
        qname = quote_ident(table_name)
        columns = conn.execute(f"PRAGMA table_info({qname})").fetchall()
        count = conn.execute(f"SELECT COUNT(*) FROM {qname}").fetchone()[0]
        return columns, count


def drop_table(table_name: str) -> None:
    """Drop a table if it exists."""
    with get_connection() as conn:
        qname = quote_ident(table_name)
        conn.execute(f"DROP TABLE IF EXISTS {qname}")
        conn.commit()


def fetch_rows(table_name: str, limit: int) -> list[tuple]:
    """Return up to `limit` rows ordered by insertion (newest first)."""
    with get_connection() as conn:
        qname = quote_ident(table_name)
        rows = conn.execute(
            f"SELECT * FROM {qname} ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return rows


def delete_instance_rows(table_name: str, instance_uuid: str) -> int:
    """Delete rows matching a specific instance UUID and return the count removed."""
    if not instance_uuid:
        raise ValueError("'instance_uuid' must not be empty.")
    with get_connection() as conn:
        qname = quote_ident(table_name)
        cursor = conn.execute(
            f"DELETE FROM {qname} WHERE instance_uuid = ?",
            (instance_uuid,),
        )
        conn.commit()
        return cursor.rowcount or 0

def normalize_value(val):
    """Normalize JSON-like strings and None/empty for comparison."""
    if val is None or val == "":
        return None
    if isinstance(val, str):
        stripped = val.strip()
        # If looks like JSON, canonicalize it
        if (stripped.startswith("{") and stripped.endswith("}")) or \
           (stripped.startswith("[") and stripped.endswith("]")):
            try:
                obj = json.loads(stripped)
                # Compact form removes irrelevant whitespace
                return json.dumps(obj, separators=(",", ":"))
            except Exception:
                pass
        return stripped
    return val


def get_matching_call(endpoint: str, params: dict, table_name: str):
    """Find a matching call in the database for given endpoint + parameters."""
    conn = get_connection()
    try:
        norm_params = {k: normalize_value(v) for k, v in params.items()}
        query = f"SELECT * FROM {quote_ident(table_name)} WHERE endpoint_name = ?"
        values = [endpoint]
        for k, v in norm_params.items():
            query += (
                f" AND REPLACE(COALESCE(CAST(json_extract(input_params_json, '$.{k}') AS TEXT), ''), ' ', '') = "
                f"REPLACE(COALESCE(CAST(? AS TEXT), ''), ' ', '')"
            )
            values.append(v if v is not None else "")

        query += " ORDER BY RANDOM() LIMIT 1"

        result = conn.execute(query, values).fetchone()
        return result
    finally:
        conn.close()


def get_call_by_endpoint(endpoint: str, table_name: str):
    """Fetch any recorded call for the given endpoint."""
    with get_connection() as conn:
        query = (
            f"SELECT * FROM {quote_ident(table_name)} "
            "WHERE endpoint_name = ? "
            "ORDER BY RANDOM() LIMIT 1"
        )
        return conn.execute(query, (endpoint,)).fetchone()

def load_logs(logs_dir: str, table_name: str, clear_first: bool = False) -> None:
    """
    Load log files from a directory into the specified database table.
    """
    if clear_first:
        logLoader.parse_logs(logs_dir, table_name)
    else:
        logLoader.append_logs(logs_dir, table_name)
