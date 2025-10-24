# Data Structure

:::mermaid

classDiagram
    class dbManager {
        +str DB_PATH
        +get_connection() Connection
        +set_setting(key: str, value: str) void
        +get_setting(key: str) str
    }

    class db_cli {
        +Typer app
        +str DB_PATH
        +str SCHEMA
        +quote_ident(name: str) str
        +get_connection() Connection
        +create_table(table_name: str) void
        +metadata(table_name: str) void
        +delete_instance(table_name: str, instance_uuid: str) void
        +load_logs(table_name: str, logs_dir: str, append: bool) void
        +list_rows(table_name: str, limit: int) void
        +list_tables() void
        +delete_table(table_name: str) void
        +set_active_table(table_name: str) void
        +get_active_table() str
        +set_replay_table(table_name: str) void
    }

    class replay {
        +FastAPI app
        +send_back(cpee_callback: str, sendback: dict, start: datetime, is_last: bool) async
        +get_call(form: Dict, db: Connection, oep: str, table_name: str) Optional[tuple]
        +get_instantiation(call: Dict, db: Connection, table_name: str) Optional[tuple]
        +extract_form_params(form_data: dict) Dict[str, Any]
        +send_back_all(cpee_callback: str, responses: List, start: datetime) async
        +DoIt(request: Request, oep: str, cpee_callback: str, sim_target: str) Response
    }

    class loadLogs {
        +str DB_PATH
        +str LOGS_DIR
        +_open_connection() tuple
        +_quote_ident(name: str) str
        +_ensure_table(c: Cursor, table_name: str) void
        +_ingest_logs(clear_first: bool, logs_dir: str, table_name: str, chunk_size: int) void
        +parse_logs(logs_dir: str, table_name: str) void
        +append_logs(logs_dir: str, table_name: str) void
    }

    class EventsDB {
        <<database>>
        +_settings(key: TEXT, value: TEXT)
        +tables(instance_uuid: TEXT, activity_uuid: TEXT, endpoint_name: TEXT, call_timestamp: TEXT, input_params_json: TEXT, responses_json: TEXT, event_type: TEXT)
    }

    dbManager --> EventsDB : reads/writes
    db_cli --> EventsDB : manages tables
    db_cli ..> loadLogs : imports and calls
    db_cli --> dbManager : uses set/get_setting
    replay --> EventsDB : queries data
    replay --> dbManager : uses get/set_setting
    replay --> db_cli : uses quote_ident and create_table
    loadLogs --> EventsDB : inserts log data
    loadLogs --> dbManager : uses set_setting

    note for EventsDB "SQLite database storing:\n- Settings (_settings table)\n- Event logs (dynamic tables)\n- Activity calls and responses"
:::
