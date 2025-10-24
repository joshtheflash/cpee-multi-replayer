#!/usr/bin/env python3
"""
db_cli.py
----------
Command-line interface for managing the SQLite database used by the replay system.
Provides commands for creating, inspecting, and maintaining tables, as well as
loading log data via the log loader module.
"""

import os
import sqlite3
import typer
from app import loadLogs as loader
from app.db.dbManager import get_connection, set_setting

app = typer.Typer(help="Database management CLI for the replay system.")


# --------------------------------------------------------
# Helper utilities
# --------------------------------------------------------

def quote_ident(name: str) -> str:
    """Safely quote SQLite identifiers (e.g., table names)."""
    return '"' + name.replace('"', '""') + '"'


SCHEMA = '''
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


# --------------------------------------------------------
# TABLE MANAGEMENT
# --------------------------------------------------------

@app.command()
def create_table(table_name: str):
    """Create a new table with the standard schema."""
    if not table_name:
        typer.echo("Table name must not be empty.")
        raise typer.Exit(code=1)
    with get_connection() as conn:
        try:
            conn.execute(SCHEMA.format(table_name=quote_ident(table_name)))
            conn.commit()
            typer.echo(f"Table '{table_name}' created (if not exists).")
        except sqlite3.Error as e:
            typer.echo(f"Failed to create table '{table_name}': {e}")
            raise typer.Exit(code=3)


@app.command()
def list_tables():
    """List all user tables and row counts."""
    with get_connection() as conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [name for (name,) in cursor.fetchall()]
            if not tables:
                typer.echo("(no tables found)")
                return
            for name in tables:
                qname = quote_ident(name)
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {qname}")
                    count = cursor.fetchone()[0]
                except sqlite3.Error:
                    count = "?"
                typer.echo(f"{name} ({count} rows)")
        except sqlite3.Error as e:
            typer.echo(f"Failed to list tables: {e}")
            raise typer.Exit(code=3)


@app.command()
def metadata(table_name: str):
    """Show metadata (columns and row count) for a specific table."""
    with get_connection() as conn:
        try:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not c.fetchone():
                typer.echo(f"Table '{table_name}' does not exist.")
                raise typer.Exit(code=1)

            qname = quote_ident(table_name)
            c.execute(f"PRAGMA table_info({qname})")
            columns = c.fetchall()
            c.execute(f"SELECT COUNT(*) FROM {qname}")
            count = c.fetchone()[0]

            typer.echo(f"\n Table: {table_name}")
            typer.echo("Columns:")
            for col in columns:
                nullable = "NOT NULL" if col[3] else "NULL"
                pk = " PRIMARY KEY" if col[5] else ""
                typer.echo(f" - {col[1]} ({col[2]}) {nullable}{pk}")
            typer.echo(f"\nTotal rows: {count}")
        except sqlite3.Error as e:
            typer.echo(f"Metadata read failed: {e}")
            raise typer.Exit(code=3)


@app.command()
def delete_table(table_name: str):
    """Drop a table if it exists."""
    if not table_name:
        typer.echo("Table name must not be empty.")
        raise typer.Exit(code=1)
    with get_connection() as conn:
        try:
            qname = quote_ident(table_name)
            conn.execute(f"DROP TABLE IF EXISTS {qname}")
            typer.echo(f"Dropped table '{table_name}'.")
        except sqlite3.Error as e:
            typer.echo(f"Failed to drop table '{table_name}': {e}")
            raise typer.Exit(code=3)


# --------------------------------------------------------
# DATA OPERATIONS
# --------------------------------------------------------

@app.command()
def list_rows(table_name: str, limit: int = 10):
    """List rows from a table (default: 10 most recent)."""
    if limit <= 0:
        typer.echo("Limit must be greater than zero.")
        raise typer.Exit(code=1)
    with get_connection() as conn:
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not c.fetchone():
                typer.echo(f"Table '{table_name}' does not exist.")
                raise typer.Exit(code=1)
            qname = quote_ident(table_name)
            c.execute(f"SELECT * FROM {qname} ORDER BY rowid DESC LIMIT ?", (limit,))
            rows = c.fetchall()
            if not rows:
                typer.echo("(no rows)")
            else:
                for row in rows:
                    typer.echo(row)
        except sqlite3.Error as e:
            typer.echo(f"Failed to list rows from '{table_name}': {e}")
            raise typer.Exit(code=3)


@app.command()
def delete_instance(table_name: str, instance_uuid: str):
    """Delete all rows belonging to a specific instance UUID."""
    if not instance_uuid:
        typer.echo("'instance_uuid' must not be empty.")
        raise typer.Exit(code=1)
    with get_connection() as conn:
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not c.fetchone():
                typer.echo(f"Table '{table_name}' does not exist.")
                raise typer.Exit(code=1)
            qname = quote_ident(table_name)
            c.execute(f"DELETE FROM {qname} WHERE instance_uuid = ?", (instance_uuid,))
            deleted = c.rowcount or 0
            conn.commit()
            typer.echo(f"Deleted {deleted} row(s) with instance_uuid={instance_uuid}.")
        except sqlite3.Error as e:
            typer.echo(f"Deletion failed: {e}")
            raise typer.Exit(code=3)


# --------------------------------------------------------
# LOG LOADING AND SETTINGS
# --------------------------------------------------------

@app.command()
def load_logs(table_name: str, logs_dir: str, append: bool = typer.Option(False, help="Append instead of replace existing rows.")):
    """Parse and load XES-YAML log files into the database."""
    if not os.path.isdir(logs_dir):
        typer.echo(f"Logs directory does not exist: {logs_dir}")
        raise typer.Exit(code=1)

    try:
        if append:
            loader.append_logs(logs_dir=logs_dir, table_name=table_name)
            typer.echo(f"Appended logs into {table_name}.")
        else:
            loader.parse_logs(logs_dir=logs_dir, table_name=table_name)
            typer.echo(f"Parsed logs into {table_name} (table cleared first).")
    except Exception as e:
        typer.echo(f"Log loading failed: {e}")
        raise typer.Exit(code=3)


@app.command()
def set_replay_table(table_name: str):
    """Set the table to use for replaying (used by replay.py)."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not c.fetchone():
            typer.echo(f"Table '{table_name}' does not exist.")
            raise typer.Exit(code=1)
    set_setting("active_table", table_name)
    typer.echo(f"Replay table set to '{table_name}'.")


if __name__ == "__main__":
    app()
