"""
dbManager.py
-------------
Provides a simple runtime interface to the SQLite database used by the replay system.
Includes connection handling and persistent key–value settings storage.
"""

import sqlite3
import os

# Path to the database file relative to this script
DB_PATH = os.path.join(os.path.dirname(__file__), "events.db")


def get_connection() -> sqlite3.Connection:
    """
    Open a SQLite connection with predefined pragmas for reliability and performance.
    """
    conn = sqlite3.connect(DB_PATH, timeout=2, isolation_level=None)
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
