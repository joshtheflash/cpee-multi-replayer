"""
replay.py
----------
FastAPI-based replay service for the CPEE environment.
Replays recorded endpoint responses based on process logs.
"""
from fastapi import FastAPI, Request, Header, Query, Response
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from time import perf_counter
import asyncio
import httpx
import json
import base64
import re
import logging
from contextlib import asynccontextmanager

from app.db import dbManager as dbm

# --------------------------------------------------------
# Configuration
# --------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

BASE64_URI_PATTERN = re.compile(r"data:[^,]+,([A-Za-z0-9+/=\n\r]+)")

http_client: Optional[httpx.AsyncClient] = None

# --------------------------------------------------------
# Parsing Utilities
# --------------------------------------------------------

def parse_header_params(header: Optional[str]) -> Dict[str, str]:
    """Parse space-separated key=value pairs from header."""
    if not header:
        return {}
    
    params = {}
    for token in header.split():
        if "=" in token:
            key, value = token.split("=", 1)
            params[key.strip()] = value.strip()
    return params

def parse_form_value(value: str) -> Any:
    """Convert form string to appropriate type (int/float/str)."""
    if not isinstance(value, str):
        return value
    
    if value.isdigit():
        return int(value)
    
    try:
        if "." in value:
            return float(value)
    except ValueError:
        pass
    
    return value

def extract_form_data(form_data) -> Dict[str, Any]:
    """Extract and type-convert form data."""
    return {key: parse_form_value(value) for key, value in form_data.items()}

# --------------------------------------------------------
# Response Processing
# --------------------------------------------------------

def structure_response_data(response: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Convert response data list to name-keyed dictionary."""
    data_list = response.get("data", [])
    if not isinstance(data_list, list):
        return {}
    
    return {
        item["name"]: {k: v for k, v in item.items() if k != "name"}
        for item in data_list
        if "name" in item
    }

def decode_content(content: Any) -> bytes:
    """Decode content to bytes, handling base64 data URIs and text."""
    if isinstance(content, (bytes, bytearray)):
        return bytes(content)
    
    if isinstance(content, str):
        content = content.strip()
        
        # Try base64 data URI
        match = BASE64_URI_PATTERN.match(content)
        if match:
            try:
                return base64.b64decode(match.group(1))
            except Exception:
                pass
        
        return content.encode("utf-8", errors="replace")
    
    return str(content).encode("utf-8", errors="replace")

def build_multipart_payload(structured_data: Dict[str, Dict[str, Any]]) -> List[tuple]:
    """Build multipart file payload for httpx."""
    files = []
    for name, entry in structured_data.items():
        mimetype = entry.get("mimetype", "text/plain")
        content = decode_content(entry.get("data"))
        files.append((name, ("", content, mimetype)))
    return files

# --------------------------------------------------------
# Response Replay Logic
# --------------------------------------------------------

async def send_callback(callback_url: str, payload: Dict[str, Any], is_final: bool) -> None:
    """Send a recorded response payload to the callback URL."""
    files = build_multipart_payload(structure_response_data(payload))
    headers = {} if is_final else {"CPEE-UPDATE": "true"}

    try:
        client = http_client
        if client is None:
            async with httpx.AsyncClient() as temp_client:
                await temp_client.put(callback_url, files=files, headers=headers)
        else:
            await client.put(callback_url, files=files, headers=headers)
    except Exception:
        logger.exception("Failed to send callback to %s", callback_url)

async def replay_responses(
    callback_url: str,
    responses: List[Dict[str, Any]],
    start_time: datetime
) -> None:
    """Replay all responses with correct timing."""
    if not responses:
        return

    base_ts = min(
        start_time,
        *(datetime.fromisoformat(responses[0]["timestamp"]) for _ in [0]),
    )
    replay_started = perf_counter()

    for index, response in enumerate(responses):
        try:
            target_ts = datetime.fromisoformat(response["timestamp"])
        except Exception:
            continue

        delay = max((target_ts - base_ts).total_seconds(), 0)
        remaining = delay - (perf_counter() - replay_started)
        if remaining > 0:
            await asyncio.sleep(remaining)

        await send_callback(callback_url, response, is_final=(index == len(responses) - 1))


async def replay_delays_only(
    responses: List[Dict[str, Any]],
    start_time: Union[datetime, str],
    context: str
) -> None:
    """Wait using recorded response timings without sending data."""
    if not responses:
        return

    if isinstance(start_time, datetime):
        base_ts = start_time
    else:
        try:
            base_ts = datetime.fromisoformat(start_time)
        except Exception:
            base_ts = datetime.utcnow()

    try:
        first_ts = datetime.fromisoformat(responses[0]["timestamp"])
        if first_ts < base_ts:
            base_ts = first_ts
    except Exception:
        pass

    replay_started = perf_counter()

    for response in responses:
        timestamp_raw = response.get("timestamp")
        if not timestamp_raw:
            continue
        try:
            target_ts = datetime.fromisoformat(timestamp_raw)
        except Exception:
            continue

        delay = max((target_ts - base_ts).total_seconds(), 0)
        remaining = delay - (perf_counter() - replay_started)
        if remaining > 0:
            await asyncio.sleep(remaining)

    
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    if http_client is None:
        limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
        http_client = httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(20.0))
        logger.debug("Initialised shared HTTP client for replay callbacks")
    yield
    if http_client is not None:
        await http_client.aclose()
        http_client = None
        logger.debug("Closed shared HTTP client")

app = FastAPI(title="CPEE Replay System", lifespan=lifespan)

@app.api_route("/cpee/replay", methods=["POST", "PUT", "GET"])
async def replay_endpoint(
    request: Request,
    oep: str = Query(..., alias="original_endpoint"),
    cpee_callback: Optional[str] = Header(None, alias="cpee-callback"),
    sim_target: Optional[str] = Header(None, alias="cpee-sim-target"),
    sim_engine: Optional[str] = Header(None, alias="cpee-attr-sim-engine"),
    sim_translate: Optional[str] = Header(None, alias="cpee-attr-sim-translate")
):
    """Handle replay requests from CPEE."""
    try:
        # Determine active table
        target_params = parse_header_params(sim_target)
        table_name = target_params.get("table") or dbm.get_setting("active_table") or "calls"
        
        if target_params.get("table"):
            if not dbm.table_exists(table_name):
                logger.error(f"Specified table does not exist: {table_name}")
                return Response(status_code=400, content=f"Table '{table_name}' does not exist.")
            dbm.set_setting("active_table", table_name)
        
        logger.debug(f"Using table: {table_name}")
        
        # Parse request form data
        form = extract_form_data(await request.form())
        logger.debug(f"Replay request: endpoint={oep}, form={form}")
        
        # Find matching call
        call = dbm.get_matching_call(oep, form, table_name)
        if not call:
            logger.debug("No matching call found; attempting endpoint-only replay")
            fallback_call = dbm.get_call_by_endpoint(oep, table_name)
            if fallback_call:
                responses_json = fallback_call[5] or "[]"
                try:
                    responses = json.loads(responses_json)
                except Exception:
                    logger.exception("Failed to parse responses for fallback call; using empty list")
                    responses = []
                if not isinstance(responses, list):
                    logger.debug("Fallback responses payload is not a list; ignoring contents")
                    responses = []

                await replay_delays_only(responses, fallback_call[3], context="endpoint-fallback")
            else:
                logger.debug("No recorded calls available for endpoint fallback")
            return Response(status_code=200, headers={"CPEE-CALLBACK": "false"})
        
        # Handle instantiation task
        if call[6] == "instantiation":
            headers = {
                "CPEE-SIM-ENGINE": sim_engine,
                "CPEE-SIM-TRANSLATE": sim_translate,
                "CPEE-SIM-TASKTYPE": "i",
                "CPEE-SIM-MODEL": form.get("url", ""),
                "CPEE-SIM-TARGET": sim_target or ""
            }
            logger.debug(f"Returning instantiation response (561) with headers: {headers}")
            return Response(status_code=561, headers=headers)
        
        # Schedule normal response replay
        responses = json.loads(call[5])
        start_time = datetime.fromisoformat(call[3])
        
        logger.debug(f"Found {len(responses)} responses starting at {start_time}")
        
        if cpee_callback:
            asyncio.create_task(replay_responses(cpee_callback, responses, start_time))
            logger.debug(
                "Started asynchronous replay task for callback %s (responses=%d, start=%s)",
                cpee_callback,
                len(responses),
                start_time.isoformat(),
            )
        else:
            logger.warning("No callback URL provided")
        
        return Response(status_code=200, headers={"CPEE-CALLBACK": "true"})
        
    except Exception as e:
        logger.exception("Replay error")
        return {"error": str(e)}
