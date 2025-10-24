import os
import sqlite3
import yaml
import json
from glob import glob
import cProfile
import pstats
import db.dbManager as dbm

DB_PATH = os.path.join(os.path.dirname(__file__), '../db/events.db')
LOGS_DIR = os.path.join(os.path.dirname(__file__), '../logs/coopis2010')

def _open_connection():
    conn = sqlite3.connect(DB_PATH, timeout=2, check_same_thread=False, isolation_level=None)
    c = conn.cursor()
    c.execute('PRAGMA busy_timeout = 2000')
    c.execute('PRAGMA journal_mode = WAL')
    c.execute('PRAGMA synchronous = OFF')  # Faster writes
    c.execute('PRAGMA cache_size = -64000')   # 64MB cache
    return conn, c

def _quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'

def _ensure_table(c, table_name):
    qtable = _quote_ident(table_name)
    c.execute(f'''
CREATE TABLE IF NOT EXISTS {qtable} (
    instance_uuid TEXT,
    activity_uuid TEXT,
    endpoint_name TEXT,
    call_timestamp TEXT,
    input_params_json JSON,
    responses_json TEXT,
    event_type TEXT,
    PRIMARY KEY (instance_uuid, activity_uuid, event_type)
)
''')

def _ingest_logs(clear_first=False, logs_dir=None, table_name='calls', chunk_size=10000):
    conn, c = _open_connection()
    _ensure_table(c, table_name)
    dbm.set_setting('last_loaded_directory', logs_dir or LOGS_DIR)
    if clear_first:
        qtable = _quote_ident(table_name)
        c.execute(f'DROP TABLE IF EXISTS {qtable}')
        _ensure_table(c, table_name)

    target_logs_dir = logs_dir or LOGS_DIR
    qtable = _quote_ident(table_name)
    
    insert_sql = f'''
        INSERT OR IGNORE INTO {qtable} (
            instance_uuid, endpoint_name, call_timestamp, input_params_json, 
            activity_uuid, responses_json, event_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    
    all_records = []
    c.execute('BEGIN')

    for log_file in glob(os.path.join(target_logs_dir, '*.xes.yaml')):
        with open(log_file, 'r') as f:
            docs_iter = yaml.load_all(f,Loader=yaml.CSafeLoader)
            current_calls = {}

            # Skip metadata
            next(docs_iter, None)

            for doc in docs_iter:
                if not isinstance(doc, dict) or 'event' not in doc:
                    continue 

                event = doc['event']
                instance_uuid = event.get('cpee:instance')
                activity_uuid = event.get('cpee:activity_uuid')
                endpoint_name = event.get('concept:endpoint')
                timestamp = event.get('time:timestamp')
                lifecycle = event.get('cpee:lifecycle:transition')
                data = event.get('data') or event.get('raw') or []

                if lifecycle == 'activity/calling':
                    input_params = {d['name']: d['value'] for d in data 
                                    if isinstance(d, dict) and 'name' in d and 'value' in d}
                    current_calls[(instance_uuid, activity_uuid)] = {
                        'instance_uuid': instance_uuid,
                        'endpoint_name': endpoint_name,
                        'call_timestamp': timestamp,
                        'input_params_json': json.dumps(input_params, sort_keys=False),
                        'activity_uuid': activity_uuid,
                        'responses': []
                    }
                elif lifecycle == 'task/instantiation':
                    input_params = {d['name']: d['value'] for d in data 
                                    if isinstance(d, dict) and 'name' in d and 'value' in d}
                    all_records.append((
                        instance_uuid, endpoint_name, timestamp,
                        json.dumps(input_params, sort_keys=False),
                        activity_uuid, None, 'instantiation'
                    ))
                elif activity_uuid and lifecycle in ['activity/receiving', 'task/instantiation', 'activity/done']:
                    call_key = (instance_uuid, activity_uuid)
                    if call_key in current_calls:
                        if lifecycle in ['activity/receiving', 'task/instantiation']:
                            current_calls[call_key]['responses'].append({
                                'timestamp': timestamp, 'lifecycle': lifecycle, 'data': data
                            })
                        elif lifecycle == 'activity/done':
                            call = current_calls.pop(call_key)
                            all_records.append((
                                call['instance_uuid'], call['endpoint_name'],
                                call['call_timestamp'], call['input_params_json'],
                                call['activity_uuid'],
                                json.dumps(call['responses'], sort_keys=False), 'call'
                            ))

                if len(all_records) >= chunk_size:
                    c.executemany(insert_sql, all_records)
                    all_records.clear()

    # Reste einfügen
    if all_records:
        c.executemany(insert_sql, all_records)
    
    c.execute('COMMIT')
    # Indexe nach allen Inserts
    idx_prefix = f'idx_{table_name}'
    c.execute(f'CREATE INDEX IF NOT EXISTS {idx_prefix}_endpoint_name ON {qtable} (endpoint_name)')
    c.execute(f'CREATE INDEX IF NOT EXISTS {idx_prefix}_input_params ON {qtable} (input_params_json)')
    c.execute(f'CREATE INDEX IF NOT EXISTS {idx_prefix}_event_type ON {qtable} (event_type)')
    conn.commit()
    conn.close()

def parse_logs(logs_dir='../../../logs/turmv4_batch6', table_name='calls'):
    try:
        print(f'Parsing logs from {logs_dir}')
        _ingest_logs(clear_first=True, logs_dir=logs_dir, table_name=table_name)
    except sqlite3.OperationalError as e:
        print(f'SQLite error during parse_logs: {e}', flush=True)
        raise

def append_logs(logs_dir=None, table_name='calls'):
    try:
        _ingest_logs(clear_first=False, logs_dir=logs_dir, table_name=table_name)
    except sqlite3.OperationalError as e:
        print(f'SQLite error during append_logs: {e}', flush=True)
        raise

if __name__ == "__main__":
    cProfile.run('parse_logs()','parse_logs.prof', sort='cumulative')
    pstats.Stats('parse_logs.prof').sort_stats('cumulative').print_stats()
    #parse_logs()