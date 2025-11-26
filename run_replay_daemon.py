#!/usr/bin/env python3
"""
run_replay_daemon.py
---------------------
Daemon entrypoint that starts the FastAPI replay service (`app.replay`).
The script relies on `python-daemon` to perform the double-fork and detaches
Uvicorn into the background.

Environment variables:
- REPLAY_DAEMON_HOST: Listen address (default: 0.0.0.0)
- REPLAY_DAEMON_PORT: Listen port (default: 8000)
- REPLAY_DAEMON_LOG_DIR: Directory for stdout/stderr logs (default: <repo>/var/log)
- REPLAY_DAEMON_PID_FILE: PID file location (default: <repo>/var/run/replay_daemon.pid)

Usage:
    python run_replay_daemon.py
Stop the daemon with:
    kill "$(cat <pid-file>)"
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path

try:
    from daemon import DaemonContext
    from daemon.pidfile import PIDLockFile
except ImportError as exc:  # pragma: no cover - guidance for missing dependency
    raise SystemExit(
        "python-daemon is required. Install it with 'pip install python-daemon lockfile'."
    ) from exc

import uvicorn

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_LOG_DIR = REPO_ROOT / "var" / "log"
DEFAULT_PID_FILE = REPO_ROOT / "var" / "run" / "replay_daemon.pid"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"Environment variable {name} must be an integer (got: {raw!r}).") from exc


def _ensure_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _make_uvicorn_server(host: str, port: int) -> uvicorn.Server:
    config = uvicorn.Config(
        "app.replay:app",
        host=host,
        port=port,
        reload=False,
        log_config=None,
    )
    return uvicorn.Server(config)


def _run_server(host: str, port: int) -> None:
    server = _make_uvicorn_server(host, port)
    server.run()


def main() -> None:
    host = os.environ.get("REPLAY_DAEMON_HOST", "0.0.0.0")
    port = _env_int("REPLAY_DAEMON_PORT", 8000)

    log_dir = Path(os.environ.get("REPLAY_DAEMON_LOG_DIR", str(DEFAULT_LOG_DIR)))
    pid_path = Path(os.environ.get("REPLAY_DAEMON_PID_FILE", str(DEFAULT_PID_FILE)))

    stdout_path = log_dir / "replay_daemon.out"
    stderr_path = log_dir / "replay_daemon.err"

    _ensure_path(stdout_path)
    _ensure_path(stderr_path)
    _ensure_path(pid_path)

    stdout_handle = open(stdout_path, "a+", buffering=1)
    stderr_handle = open(stderr_path, "a+", buffering=1)
    stdin_handle = open(os.devnull, "r")

    context = DaemonContext(
        working_directory=str(REPO_ROOT),
        umask=0o022,
        pidfile=PIDLockFile(str(pid_path)),
        stdout=stdout_handle,
        stderr=stderr_handle,
        stdin=stdin_handle,
    )

    def _shutdown(signum, _frame):
        logging.info("Received signal %s, shutting down daemon.", signum)
        raise SystemExit(0)

    context.signal_map = {
        signal.SIGTERM: _shutdown,
        signal.SIGHUP: _shutdown,
    }
    context.files_preserve = [stdout_handle.fileno(), stderr_handle.fileno()]

    print(
        "[cpee-replay-daemon] Starting daemon in background "
        f"(host={host} port={port}). PID file: {pid_path}. "
        f"Logs -> stdout: {stdout_path}, stderr: {stderr_path}",
        flush=True,
    )

    try:
        with context:
            logging.basicConfig(
                level=logging.INFO,
                stream=stderr_handle,
                format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            )
            logging.info(
                "Starting replay daemon with host=%s port=%s pidfile=%s", host, port, pid_path
            )
            _run_server(host, port)
    finally:
        stdout_handle.close()
        stderr_handle.close()
        stdin_handle.close()


if __name__ == "__main__":
    # Ensure the repository root is on sys.path so 'app' can be imported after daemonizing.
    sys.path.insert(0, str(REPO_ROOT))
    main()

