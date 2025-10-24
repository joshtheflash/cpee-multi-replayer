from distutils.command import config
from fastapi import FastAPI, Header, Body, Query, Request, Form, BackgroundTasks
from fastapi.responses import Response
from typing import Dict, Any, List, Optional
import asyncio
import httpx
from datetime import datetime
import sqlite3
import json
import db.db_cli as dbcli
import db.dbManager as dbm
import scripts.loadLogs as loader

async def send_back(cpee_callback: str, sendback,start,is_last):
    headers: Dict[str, str] = {}
    try:
        async with httpx.AsyncClient() as client:
                if not is_last:
                    headers["CPEE-UPDATE"] = "true"
                    
                if isinstance(sendback.get('data'), list) and sendback.get('data'):
                    first_item = sendback.get('data', [{}])[0]
                    key = 'value' if 'value' in first_item else 'data'
                    data = {item['name']: item[key] for item in sendback.get('data', [])}
                else:
                    print('Warning: Empty data received')
                    data = {}
                    
                ts = datetime.fromisoformat(sendback.get('timestamp'))
                dur = ts - start
                
                try:
                    if sendback.get('lifecycle') == 'activity/receiving':
                        print(f"Replaying response after {dur.total_seconds()}s delay")
                        await asyncio.sleep(dur.total_seconds()) 
                        print(f"Sending response to {cpee_callback} with headers: {headers}")
                        res = await client.put(cpee_callback, data=data, headers=headers)
                        print(f"Response sent: {res.status_code}")
                except httpx.TimeoutException:
                    print(f"Timeout: Callback server at {cpee_callback} did not respond")
                except httpx.ConnectError as e:
                    print(f"Connection error: Could not connect to {cpee_callback}")
                except Exception as e:
                    print(f"HTTP request error: {e}")
    except Exception as e:
        print(f"Error in send_back: {e}")


def get_call(form: Dict[str, Any], db, oep, table_name: str) -> Optional[tuple]:
    print(f"Searching for: {oep} with params: {form}")

    query = f"SELECT * FROM {dbcli.quote_ident(table_name)} WHERE endpoint_name = ?"
    params_list = [oep]

    # Add json_extract conditions for each key in params
    # - Compare type-insensitively
    # - Treat NULL in DB as equivalent to empty string from the form
    # - For JSON-like strings such as 'init', ignore whitespace differences
    for key, value in form.items():
        if key == 'init' and isinstance(value, str):
            query += (
                " AND REPLACE(CAST(json_extract(input_params_json, '$.init') AS TEXT), ' ', '') = "
                "REPLACE(CAST(? AS TEXT), ' ', '')"
            )
            params_list.append(value)
        else:
            query += (
                f" AND COALESCE(CAST(json_extract(input_params_json, '$.{key}') AS TEXT), '') = "
                f"COALESCE(CAST(? AS TEXT), '')"
            )
            params_list.append(value)

    query += " ORDER BY RANDOM() LIMIT 1"

    event = db.execute(query, params_list).fetchone()
    if event:
        print(f"Found matching call: ID {event[0]}")
        return event
    else:
        print("No matching call found")
        return None

def get_instantiation(call: Dict[str, Any], db, table_name: str) -> Optional[tuple]:
    instance_uuid = call[0]
    activity_uuid = call[1]
    query = f"SELECT * FROM {dbcli.quote_ident(table_name)} WHERE instance_uuid = ? AND activity_uuid = ? AND event_type = 'instantiation'"
    params = (instance_uuid, activity_uuid)
    return db.execute(query, params).fetchone()


def extract_form_params(form_data) -> Dict[str, Any]:
    """Extract and convert parameters from form data"""
    params = {}
    for key, value in form_data.items():
        if isinstance(value, str):
            # Try integer conversion
            if value.isdigit():
                params[key] = int(value)
            # Try float conversion  
            elif value.replace('.', '', 1).isdigit():
                params[key] = float(value)
            else:
                params[key] = value
        else:
            params[key] = value
    return params

async def send_back_all(cpee_callback: str, responses: List[Dict[str, Any]], start: datetime):
    for i, response in enumerate(responses):
            is_last = (i == len(responses) - 1)
            await send_back(cpee_callback, response, start, is_last)

app = FastAPI()

@app.api_route("/cpee/replay", methods=["POST", "PUT", "GET", "DELETE", "PATCH"])
async def DoIt(
    request: Request,
    oep: str = Query(..., alias="original_endpoint"),
    cpee_callback: Optional[str] = Header(None, alias="cpee-callback"),
    sim_target: Optional[str] = Header(None, alias="cpee-sim-target")
):
    try:
        last_sim_target = dbm.get_setting('last_sim_target') or ""
        parts = dict(pair.split("=", 1) for pair in sim_target.split())
        table_name = parts.get("table")
        if table_name:
            dbcli.create_table(table_name)
            dbm.set_setting('active_table', table_name)

        table_name = table_name or dbm.get_setting('active_table') or 'calls'
        print(f"Using table: {table_name}")
        last_sim_target = sim_target
        dbm.set_setting('last_sim_target', last_sim_target)

        form_data = await request.form()
        form = extract_form_params(form_data)
        
        db = sqlite3.connect('../db/events.db')
        call = get_call(form, db, oep, table_name)
        if call:
            instantiation = get_instantiation(call, db, table_name)
            if call[6] == "call":
                responses = json.loads(call[5])
                start = datetime.fromisoformat(call[3])               
                if instantiation is not None:
                    headers = {"CPEE-SIM-TASKTYPE": "i"}
                    sim_engine = request.headers.get("cpee-attr-sim-engine")
                    sim_translate = request.headers.get("cpee-attr-sim-translate")
                    if sim_engine is not None:
                        headers["CPEE-SIM-ENGINE"] = str(sim_engine)
                    if sim_translate is not None:
                        headers["CPEE-SIM-TRANSLATE"] = str(sim_translate)
                    headers["CPEE-SIM-MODEL"] = "https://cpee.org/hub/server/Templates.dir/Wait.xml"
                    headers["CPEE-SIM-TARGET"] = str(sim_target)
                    headers["CPEE-CALLBACK"] = "true"
                    print("Headers: ",headers)
                    res = Response(status_code=561, headers=headers)
                    print("Returning 561 with headers: ", headers)
                else:
                    if cpee_callback:
                        asyncio.create_task(send_back_all(cpee_callback, responses, start))
                    res = Response(status_code=200, headers={"CPEE-CALLBACK": "true"})
                return res
        else:
            return Response(status_code=200, headers={"CPEE-CALLBACK": "false"})
            
    except Exception as e:
        print(f"Error in DoIt: {e}")
        return {"error": "Internal server error"}