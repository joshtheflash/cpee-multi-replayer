#!/usr/bin/env python3
import inspect
import json
import typer
from functools import wraps
from app.db import dbManager as dbm

app = typer.Typer(help="Auto-generated CLI for dbManager functions.")

EXCLUDE_FUNCS = {
    "get_connection",
    "normalize_value",
    "get_matching_call",
    "quote_ident",
}

def make_command(func):
    sig = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is not None:
            if isinstance(result, list):
                for row in result:
                    typer.echo(row)
            elif isinstance(result, dict):
                typer.echo(json.dumps(result, indent=2))
            else:
                typer.echo(result)

    wrapper.__signature__ = sig
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__ or f"Run dbManager.{func.__name__}()"
    app.command(name=func.__name__.replace("_", "-"))(wrapper)

    # Auto-register all functions from dbManager
for name, func in inspect.getmembers(dbm, inspect.isfunction):
    if name not in EXCLUDE_FUNCS and not name.startswith("_"):
        make_command(func)


if __name__ == "__main__":
    app()
