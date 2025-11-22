"""Simple CLI for running the replay server locally."""
from __future__ import annotations

import typer
import uvicorn

app = typer.Typer(help="Convenience commands for starting the CPEE replay server.")

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


@app.command()
def start(
    host: str = typer.Option(DEFAULT_HOST, "--host", help="Server host/IP to bind."),
    port: int = typer.Option(DEFAULT_PORT, "--port", help="Port to listen on."),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload (dev only)."),
) -> None:
    """Start the FastAPI replay server via uvicorn."""
    config = uvicorn.Config(
        "app.replay:app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=["app"],
        log_level="info",
    )
    server = uvicorn.Server(config)
    typer.echo(f"Starting replay server on http://{host}:{port}")
    server.run()


if __name__ == "__main__":
    app()
