import os
import requests
import csv
import psycopg2
from psycopg2.extras import execute_values
from google.cloud import storage
from io import BytesIO
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from datetime import datetime
from typing import List, Optional, Set, Tuple, Dict
from email.utils import parsedate_to_datetime
from dotenv import load_dotenv
import urllib3

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DOWNLOAD_SEMAPHORE = threading.Semaphore(3)
DOWNLOAD_DELAY = 0.2

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": os.environ.get("DB_NAME"),
    "connect_timeout": 60,
    "options": "-c statement_timeout=600000",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

GCS_BUCKET = os.environ.get("GCS_BUCKET", "bls-econ-data")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")
PROXY_HOST = os.environ.get("PROXY_HOST", "unblock.oxylabs.io")
PROXY_PORT = os.environ.get("PROXY_PORT", "60000")
PROXY_VERIFY_SSL = os.environ.get("PROXY_VERIFY_SSL", "false").lower() == "true"

def get_proxies():
    if PROXY_USER and PROXY_PASS:
        proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    return None

def should_verify_ssl():
    if get_proxies() and not PROXY_VERIFY_SSL:
        return False
    return True

BLS_DATASETS = {
    "cpi": {
        "name": "Consumer Price Index",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/cu",
        "data_file": "cu.data.1.AllItems",
        "series_file": "cu.series",
        "dimension_files": {
            "area": "cu.area",
            "item": "cu.item",
            "period": "cu.period",
            "seasonal": "cu.seasonal",
        },
        "table": "cpi_data",
        "schema_file": "cpi_schema.sql",
    },
    "employment": {
        "name": "Current Employment Statistics",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/ce",
        "data_file": "ce.data.0.AllCESSeries",
        "series_file": "ce.series",
        "dimension_files": {
            "supersector": "ce.supersector",
            "industry": "ce.industry",
            "datatype": "ce.datatype",
            "period": "ce.period",
            "seasonal": "ce.seasonal",
        },
        "table": "ce_data",
        "schema_file": "ce_schema.sql",
    },
    "unemployment": {
        "name": "Local Area Unemployment Statistics",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/la",
        "data_file": "la.data.2.AllStatesU",
        "series_file": "la.series",
        "dimension_files": {
            "area": "la.area",
            "area_type": "la.area_type",
            "measure": "la.measure",
            "seasonal": "la.seasonal",
            "state": "la.state_region_division",
        },
        "table": "la_data",
        "schema_file": "la_schema.sql",
    },
    "state_employment": {
        "name": "State and Area Employment",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/sa",
        "data_file": "sa.data.0.Current",
        "series_file": "sa.series",
        "dimension_files": {
            "area": "sa.area",
            "state": "sa.state",
            "industry": "sa.industry",
            "data_type": "sa.data_type",
            "detail": "sa.detail",
        },
        "table": "sa_data",
        "schema_file": "sa_schema.sql",
    },
    "occupational": {
        "name": "Occupational Employment and Wage Statistics",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/oe",
        "data_file": "oe.data.1.AllData",
        "series_file": "oe.series",
        "dimension_files": {
            "area": "oe.area",
            "areatype": "oe.areatype",
            "industry": "oe.industry",
            "occupation": "oe.occupation",
            "datatype": "oe.datatype",
            "sector": "oe.sector",
            "seasonal": "oe.seasonal",
        },
        "table": "oe_data",
        "schema_file": "oe_schema.sql",
    },
    "compensation": {
        "name": "Compensation and Working Conditions",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/ci",
        "data_file": "ci.data.1.AllData",
        "series_file": "ci.series",
        "dimension_files": {
            "area": "ci.area",
            "owner": "ci.owner",
            "industry": "ci.industry",
            "occupation": "ci.occupation",
            "estimate": "ci.estimate",
            "periodicity": "ci.periodicity",
            "seasonal": "ci.seasonal",
        },
        "table": "ci_data",
        "schema_file": "ci_schema.sql",
    },
    "jolts": {
        "name": "Job Openings and Labor Turnover Survey",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/jt",
        "data_file": "jt.data.1.AllItems",
        "series_file": "jt.series",
        "dimension_files": {
            "industry": "jt.industry",
            "state": "jt.state",
            "area": "jt.area",
            "sizeclass": "jt.sizeclass",
            "dataelement": "jt.dataelement",
            "ratelevel": "jt.ratelevel",
            "seasonal": "jt.seasonal",
        },
        "table": "jt_data",
        "schema_file": "jt_schema.sql",
    },
    "ppi": {
        "name": "Producer Price Index",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/pr",
        "data_file": "pr.data.1.AllData",
        "series_file": "pr.series",
        "dimension_files": {
            "measure": "pr.measure",
            "sector": "pr.sector",
            "class": "pr.class",
            "duration": "pr.duration",
            "period": "pr.period",
            "seasonal": "pr.seasonal",
        },
        "table": "ppi_data",
        "schema_file": "ppi_schema.sql",
    },
    "productivity": {
        "name": "Major Sector Total Factor Productivity",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/mp",
        "data_file": "mp.data.1.AllData",
        "series_file": "mp.series",
        "dimension_files": {
            "sector": "mp.sector",
            "measure": "mp.measure",
            "duration": "mp.duration",
            "seasonal": "mp.seasonal",
        },
        "table": "mp_data",
        "schema_file": "mp_schema.sql",
    },
    "state_metro": {
        "name": "State and Metropolitan Area Employment",
        "type": "denormalized",
        "base_url": "https://download.bls.gov/pub/time.series/sm",
        "data_file": "sm.data.1.AllData",
        "series_file": "sm.series",
        "dimension_files": {
            "state": "sm.state",
            "area": "sm.area",
            "supersector": "sm.supersector",
            "industry": "sm.industry",
            "data_type": "sm.data_type",
            "seasonal": "sm.seasonal",
        },
        "table": "sm_data",
        "schema_file": "sm_schema.sql",
    },
}

def get_db_connection():
    import time
    max_retries = 5
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"  DB connection failed (attempt {attempt+1}/{max_retries}), retrying in 10s...")
                time.sleep(10)
            else:
                raise

def get_gcs_client():
    return storage.Client()

def ensure_table_exists(table_name: str, schema_file: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, (table_name,))
        exists = cur.fetchone()[0]
        if exists:
            return False
        schema_path = os.path.join(os.path.dirname(__file__) or '/app', schema_file)
        if not os.path.exists(schema_path):
            print(f"  WARNING: Schema file not found: {schema_path}")
            return False
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        cur.execute(schema_sql)
        conn.commit()
        print(f"  Created table: {table_name}")
        return True
    except Exception as e:
        print(f"  ERROR creating table {table_name}: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def get_etag_and_last_modified(url: str, max_retries: int = 3) -> Tuple[Optional[str], Optional[datetime]]:
    for attempt in range(max_retries):
        try:
            response = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=True, proxies=get_proxies(), verify=should_verify_ssl())
            etag = response.headers.get('ETag', '').strip('"')
            last_modified_str = response.headers.get('Last-Modified')
            last_modified = None
            if last_modified_str:
                last_modified = parsedate_to_datetime(last_modified_str)
            return etag, last_modified
        except Exception as e:
            print(f"  Error fetching headers (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    print(f"  Failed to fetch headers after {max_retries} attempts")
    return None, None

def get_load_log_entry(dataset: str, data_key: str) -> Optional[dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT etag, last_modified, rows_loaded, load_timestamp, status FROM bls_load_log WHERE dataset = %s AND data_key = %s",
            (dataset, data_key)
        )
        row = cur.fetchone()
        if row:
            return {'etag': row[0], 'last_modified': row[1], 'rows_loaded': row[2], 'load_timestamp': row[3], 'status': row[4]}
        return None
    finally:
        cur.close()
        conn.close()

def update_load_log(dataset: str, data_key: str, etag: Optional[str], last_modified: Optional[datetime],
                    source_url: str, rows_loaded: int, status: str = 'success', error_message: Optional[str] = None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bls_load_log (dataset, data_key, etag, last_modified, source_url, rows_loaded, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataset, data_key) DO UPDATE SET
                etag = EXCLUDED.etag,
                last_modified = EXCLUDED.last_modified,
                source_url = EXCLUDED.source_url,
                rows_loaded = EXCLUDED.rows_loaded,
                load_timestamp = NOW(),
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message
        """, (dataset, data_key, etag, last_modified, source_url, rows_loaded, status, error_message))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def should_reload(dataset: str, data_key: str, current_etag: Optional[str]) -> bool:
    entry = get_load_log_entry(dataset, data_key)
    if not entry:
        return True
    if entry['status'] == 'failed':
        return True
    if not current_etag:
        # ETag unavailable (flaky proxy) — don't force reload if previous load succeeded
        print(f"  ETag unavailable, previous load status: {entry['status']} ({entry['rows_loaded']:,} rows)")
        return False
    return entry['etag'] != current_etag

def validate_row_count(dataset: str, data_key: str, rows_loaded: int) -> bool:
    """Validate that the new row count is not suspiciously low compared to previous load.
    Returns True if valid, False if the row count is less than 50% of the previous successful load."""
    entry = get_load_log_entry(dataset, data_key)
    if not entry or entry['status'] != 'success' or not entry['rows_loaded'] or entry['rows_loaded'] <= 0:
        return True
    previous_rows = entry['rows_loaded']
    if rows_loaded < previous_rows * 0.5:
        print(f"  WARNING: Row count validation FAILED! New count {rows_loaded:,} is less than 50% of previous {previous_rows:,}")
        print(f"  This likely indicates a truncated download. Marking load as failed.")
        return False
    if rows_loaded < previous_rows * 0.9:
        print(f"  WARNING: Row count dropped from {previous_rows:,} to {rows_loaded:,} ({rows_loaded/previous_rows*100:.1f}%)")
    return True

def check_url_available(url: str) -> bool:
    try:
        response = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=True, proxies=get_proxies(), verify=should_verify_ssl())
        return response.status_code == 200
    except:
        return False

def download_to_gcs(url: str, gcs_path: str, client) -> int:
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    
    print(f"  Downloading from: {url}")
    response = requests.get(url, headers=HEADERS, stream=True, timeout=600, proxies=get_proxies(), verify=should_verify_ssl())
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    print(f"  Size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    
    content = response.content
    
    if len(content) > 2 and content[:2] == b'\x1f\x8b':
        import gzip
        print(f"  Decompressing gzipped content...")
        content = gzip.decompress(content)
        print(f"  Decompressed size: {len(content):,} bytes")
    
    print(f"  Uploading to GCS: gs://{GCS_BUCKET}/{gcs_path}")
    blob.upload_from_string(content, content_type='text/plain')
    
    return len(content)

def download_from_gcs(gcs_path: str, client) -> bytes:
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    return blob.download_as_bytes()

def check_gcs_exists(gcs_path: str, client) -> bool:
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    return blob.exists()

def ensure_gcs_file(url: str, gcs_path: str, client, force: bool = False, reload: bool = False) -> bytes:
    if force or reload or not check_gcs_exists(gcs_path, client):
        download_to_gcs(url, gcs_path, client)
    return download_from_gcs(gcs_path, client)

def download_files_parallel(file_specs: List[dict], client, force: bool = False, reload: bool = False) -> Dict[str, bytes]:
    """
    Download multiple files in parallel with rate limiting.
    
    file_specs: [{"url": "...", "gcs_path": "...", "key": "series"}, ...]
    Returns: {"series": bytes, "area": bytes, ...}
    """
    results = {}
    errors = {}
    lock = threading.Lock()
    
    def download_one(spec):
        try:
            with DOWNLOAD_SEMAPHORE:
                time.sleep(DOWNLOAD_DELAY)
                data = ensure_gcs_file(spec['url'], spec['gcs_path'], client, force=force, reload=reload)
                with lock:
                    results[spec['key']] = data
                    print(f"    Downloaded: {spec['key']}")
        except Exception as e:
            with lock:
                errors[spec['key']] = str(e)
    
    threads = [threading.Thread(target=download_one, args=(s,)) for s in file_specs]
    for t in threads: t.start()
    for t in threads: t.join()
    
    if errors:
        raise Exception(f"Failed to download: {errors}")
    
    return results

def stream_url_to_gcs_and_process(url: str, gcs_path: str, client, process_line_callback, skip_header: bool = True) -> int:
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    
    print(f"  Streaming from: {url}")
    response = requests.get(url, headers=HEADERS, stream=True, timeout=600, proxies=get_proxies(), verify=should_verify_ssl())
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    print(f"  Size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    
    gcs_upload = blob.open('wb', content_type='text/plain')
    
    rows_processed = 0
    buffer = b''
    first_line = True
    
    for chunk in response.iter_content(chunk_size=1024*1024):
        gcs_upload.write(chunk)
        buffer += chunk
        
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n', 1)
            line = line.decode('utf-8', errors='replace')
            
            if skip_header and first_line and line.startswith('series_id'):
                first_line = False
                continue
            first_line = False
            
            if line.strip():
                process_line_callback(line)
                rows_processed += 1
    
    if buffer:
        line = buffer.decode('utf-8', errors='replace')
        if skip_header and first_line and line.startswith('series_id'):
            pass
        elif line.strip():
            process_line_callback(line)
            rows_processed += 1
    
    gcs_upload.close()
    print(f"  Uploaded to GCS: gs://{GCS_BUCKET}/{gcs_path}")
    
    return rows_processed

def stream_from_gcs_and_process(gcs_path: str, client, process_line_callback, skip_header: bool = True) -> int:
    import sys
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    
    print(f"  Streaming from GCS: gs://{GCS_BUCKET}/{gcs_path}")
    sys.stdout.flush()
    
    rows_processed = 0
    first_line = True
    
    with blob.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n\r')
            
            if skip_header and first_line and line.startswith('series_id'):
                first_line = False
                continue
            first_line = False
            
            if line.strip():
                process_line_callback(line)
                rows_processed += 1
    
    print(f"  Processed {rows_processed:,} rows from GCS")
    sys.stdout.flush()
    
    return rows_processed

def copy_from_gcs_to_table(gcs_path: str, table: str, columns: List[str], client, cur, skip_header: bool = True, null_values: List[str] = None) -> int:
    import sys
    import io
    
    if null_values is None:
        null_values = ['', '-']
    
    CHUNK_SIZE = 500000  # rows per COPY batch
    
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    
    print(f"  COPY from GCS: gs://{GCS_BUCKET}/{gcs_path} -> {table}")
    sys.stdout.flush()
    
    col_str = ", ".join(columns)
    copy_sql = f"COPY {table} ({col_str}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\b', HEADER false)"
    
    total_rows = 0
    chunk_rows = 0
    buffer = io.StringIO()
    first_line = True
    
    with blob.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n\r')
            
            if skip_header and first_line and line.startswith('series_id'):
                first_line = False
                continue
            first_line = False
            
            if not line.strip():
                continue
            
            parts = line.split('\t')
            if len(parts) < len(columns):
                continue
            parts = parts[:len(columns)]
            cleaned = []
            for p in parts:
                val = p.strip()
                if val in null_values:
                    cleaned.append('')
                else:
                    cleaned.append(val)
            
            buffer.write('\t'.join(cleaned) + '\n')
            chunk_rows += 1
            total_rows += 1
            
            if chunk_rows >= CHUNK_SIZE:
                buffer.seek(0)
                cur.copy_expert(copy_sql, buffer)
                buffer.close()
                buffer = io.StringIO()
                print(f"    Copied {total_rows:,} rows so far...")
                sys.stdout.flush()
                chunk_rows = 0
    
    # Flush remaining rows
    if chunk_rows > 0:
        buffer.seek(0)
        cur.copy_expert(copy_sql, buffer)
    buffer.close()
    
    print(f"  COPY complete: {total_rows:,} rows")
    sys.stdout.flush()
    
    return total_rows

def load_bulk_data(data_bytes: bytes, table: str, columns: List[str]) -> int:
    print(f"  Loading to {table}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute(f"TRUNCATE TABLE {table}")
    conn.commit()
    print(f"  Truncated {table}")
    
    text = data_bytes.decode('utf-8', errors='replace')
    lines = text.strip().split('\n')
    
    if lines and lines[0].startswith('series_id'):
        lines = lines[1:]
    
    col_str = ", ".join(columns)
    
    print(f"  Parsing {len(lines):,} rows...")
    
    BATCH_SIZE = 10000
    rows_loaded = 0
    
    batch = []
    for line in lines:
        if not line.strip():
            continue
        
        parts = line.split('\t')
        if len(parts) >= len(columns):
            values = []
            for i, col in enumerate(columns):
                val = parts[i].strip() if i < len(parts) else None
                if val == '' or val == '-':
                    val = None
                values.append(val)
            batch.append(tuple(values))
            
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, f"INSERT INTO {table} ({col_str}) VALUES %s", batch)
                conn.commit()
                rows_loaded += len(batch)
                if rows_loaded % 100000 == 0:
                    print(f"    Loaded {rows_loaded:,} rows...")
                batch = []
    
    if batch:
        execute_values(cur, f"INSERT INTO {table} ({col_str}) VALUES %s", batch)
        conn.commit()
        rows_loaded += len(batch)
    
    cur.close()
    conn.close()
    
    print(f"  Loaded {rows_loaded:,} rows into {table}")
    return rows_loaded

def load_dimension_table(data_bytes: bytes, table: str, pk_column: str, columns: list = None) -> int:
    print(f"  Loading dimension {table}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    text = data_bytes.decode('utf-8', errors='replace').replace('\x00', '')
    lines = text.strip().split('\n')
    
    if lines and lines[0].startswith(pk_column):
        header_line = lines[0]
        lines = lines[1:]
        file_columns = [c.strip() for c in header_line.split('\t')]
    else:
        file_columns = None
    
    rows_loaded = 0
    BATCH_SIZE = 5000
    
    for i in range(0, len(lines), BATCH_SIZE):
        batch_lines = lines[i:i+BATCH_SIZE]
        batch = []
        for line in batch_lines:
            if not line.strip():
                continue
            parts = line.split('\t')
            values = [p.strip().replace('\x00', '') if p.strip() and p.strip() != '-' else None for p in parts]
            batch.append(tuple(values))
        
        if batch:
            if columns:
                col_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
            else:
                placeholders = ", ".join(["%s"] * len(batch[0]))
                sql = f"INSERT INTO {table} VALUES ({placeholders})"
            cur.executemany(sql, batch)
            conn.commit()
            rows_loaded += len(batch)
    
    cur.close()
    conn.close()
    
    print(f"    Loaded {rows_loaded:,} rows into {table}")
    return rows_loaded

def load_dimension_dict(data_bytes: bytes, key_col: int = 0) -> dict:
    text = data_bytes.decode('utf-8', errors='replace').replace('\x00', '')
    lines = text.strip().split('\n')
    
    if lines and '\t' in lines[0]:
        header_line = lines[0]
        lines = lines[1:]
    
    result = {}
    for line in lines:
        if not line.strip():
            continue
        parts = line.split('\t')
        key = parts[key_col].strip() if len(parts) > key_col else None
        if key:
            result[key] = [p.strip().replace('\x00', '') if p.strip() and p.strip() != '-' else None for p in parts]
    
    return result

def load_cpi_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing CPI (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('cpi', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE cu_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                area_code VARCHAR(10),
                item_code VARCHAR(10),
                seasonal VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE cu_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/cpi/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'cu_series_temp',
            ['series_id', 'area_code', 'item_code', 'seasonal'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/cpi/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'cu_data_temp',
            ['series_id', 'year', 'period', 'value'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on cu_data_temp...")
        cur.execute("CREATE INDEX idx_cu_data_temp_series ON cu_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_cu_data_temp_year ON cu_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/cpi/{config['dimension_files']['area']}"},
            {"key": "item", "url": f"{base_url}/{config['dimension_files']['item']}", "gcs_path": f"raw/cpi/{config['dimension_files']['item']}"},
            {"key": "period", "url": f"{base_url}/{config['dimension_files']['period']}", "gcs_path": f"raw/cpi/{config['dimension_files']['period']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/cpi/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        item_dict = load_dimension_dict(dim_data['item'], 0)
        print(f"    Items: {len(item_dict)}")
        period_dict = load_dimension_dict(dim_data['period'], 0)
        print(f"    Periods: {len(period_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('area', area_dict), ('item', item_dict), ('period', period_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE cu_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO cu_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_cpi_series;
            DROP INDEX IF EXISTS idx_cpi_year;
            DROP INDEX IF EXISTS idx_cpi_period;
            DROP INDEX IF EXISTS idx_cpi_area;
            DROP INDEX IF EXISTS idx_cpi_item;
            DROP INDEX IF EXISTS idx_cpi_area_year;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        cur.execute(f"""
            INSERT INTO {table} 
            (series_id, year, period, period_name, value, area_code, area_name, 
             item_code, item_name, seasonal_code, seasonal_text)
            SELECT 
                d.series_id, d.year, d.period, p.name, d.value,
                s.area_code, a.name,
                s.item_code, i.name,
                s.seasonal, se.name
            FROM cu_data_temp d
            JOIN cu_series_temp s ON d.series_id = s.series_id
            LEFT JOIN cu_area_temp a ON s.area_code = a.code
            LEFT JOIN cu_item_temp i ON s.item_code = i.code
            LEFT JOIN cu_period_temp p ON d.period = p.code
            LEFT JOIN cu_seasonal_temp se ON s.seasonal = se.code
        """)
        rows_loaded = cur.rowcount
        conn.commit()
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_cpi_series ON cpi_data(series_id);
            CREATE INDEX idx_cpi_year ON cpi_data(year);
            CREATE INDEX idx_cpi_period ON cpi_data(period);
            CREATE INDEX idx_cpi_area ON cpi_data(area_code);
            CREATE INDEX idx_cpi_item ON cpi_data(item_code);
            CREATE INDEX idx_cpi_area_year ON cpi_data(area_code, year);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE cu_series_temp, cu_data_temp")
        for dim in ['area', 'item', 'period', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS cu_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('cpi', 'all', rows_loaded):
            update_load_log('cpi', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('cpi', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('cpi', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_ppi_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing PPI (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('ppi', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE pr_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                sector_code VARCHAR(10),
                class_code VARCHAR(10),
                measure_code VARCHAR(10),
                duration_code VARCHAR(10),
                seasonal VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE pr_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(50)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/ppi/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'pr_series_temp',
            ['series_id', 'sector_code', 'class_code', 'measure_code', 'duration_code', 'seasonal'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/ppi/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'pr_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on pr_data_temp...")
        cur.execute("CREATE INDEX idx_pr_data_temp_series ON pr_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_pr_data_temp_year ON pr_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "measure", "url": f"{base_url}/{config['dimension_files']['measure']}", "gcs_path": f"raw/ppi/{config['dimension_files']['measure']}"},
            {"key": "sector", "url": f"{base_url}/{config['dimension_files']['sector']}", "gcs_path": f"raw/ppi/{config['dimension_files']['sector']}"},
            {"key": "class", "url": f"{base_url}/{config['dimension_files']['class']}", "gcs_path": f"raw/ppi/{config['dimension_files']['class']}"},
            {"key": "duration", "url": f"{base_url}/{config['dimension_files']['duration']}", "gcs_path": f"raw/ppi/{config['dimension_files']['duration']}"},
            {"key": "period", "url": f"{base_url}/{config['dimension_files']['period']}", "gcs_path": f"raw/ppi/{config['dimension_files']['period']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/ppi/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        measure_dict = load_dimension_dict(dim_data['measure'], 0)
        print(f"    Measures: {len(measure_dict)}")
        sector_dict = load_dimension_dict(dim_data['sector'], 0)
        print(f"    Sectors: {len(sector_dict)}")
        class_dict = load_dimension_dict(dim_data['class'], 0)
        print(f"    Classes: {len(class_dict)}")
        duration_dict = load_dimension_dict(dim_data['duration'], 0)
        print(f"    Durations: {len(duration_dict)}")
        period_dict = load_dimension_dict(dim_data['period'], 0)
        print(f"    Periods: {len(period_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('measure', measure_dict), ('sector', sector_dict), ('class', class_dict), ('duration', duration_dict), ('period', period_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE pr_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO pr_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_ppi_year;
            DROP INDEX IF EXISTS idx_ppi_measure;
            DROP INDEX IF EXISTS idx_ppi_sector;
            DROP INDEX IF EXISTS idx_ppi_class;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        cur.execute(f"""
            INSERT INTO {table} 
            (series_id, year, period, period_name, value, measure_code, measure_name,
             sector_code, sector_name, class_code, class_name, duration_code, duration_name,
             seasonal_code, seasonal_name, footnote_codes)
            SELECT 
                d.series_id, d.year, d.period, p.name, d.value,
                s.measure_code, m.name,
                s.sector_code, sc.name,
                s.class_code, cl.name,
                s.duration_code, du.name,
                s.seasonal, se.name,
                d.footnote_codes
            FROM pr_data_temp d
            JOIN pr_series_temp s ON d.series_id = s.series_id
            LEFT JOIN pr_measure_temp m ON s.measure_code = m.code
            LEFT JOIN pr_sector_temp sc ON s.sector_code = sc.code
            LEFT JOIN pr_class_temp cl ON s.class_code = cl.code
            LEFT JOIN pr_duration_temp du ON s.duration_code = du.code
            LEFT JOIN pr_period_temp p ON d.period = p.code
            LEFT JOIN pr_seasonal_temp se ON s.seasonal = se.code
        """)
        rows_loaded = cur.rowcount
        conn.commit()
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_ppi_year ON ppi_data(year);
            CREATE INDEX idx_ppi_measure ON ppi_data(measure_code);
            CREATE INDEX idx_ppi_sector ON ppi_data(sector_code);
            CREATE INDEX idx_ppi_class ON ppi_data(class_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE pr_series_temp, pr_data_temp")
        for dim in ['measure', 'sector', 'class', 'duration', 'period', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS pr_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('ppi', 'all', rows_loaded):
            update_load_log('ppi', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('ppi', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('ppi', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_employment_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing Employment (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('employment', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE ce_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                supersector_code VARCHAR(10),
                industry_code VARCHAR(10),
                datatype_code VARCHAR(10),
                seasonal VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE ce_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/employment/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'ce_series_temp',
            ['series_id', 'supersector_code', 'industry_code', 'datatype_code', 'seasonal'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/employment/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'ce_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on ce_data_temp...")
        cur.execute("CREATE INDEX idx_ce_data_temp_series ON ce_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_ce_data_temp_year ON ce_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "supersector", "url": f"{base_url}/{config['dimension_files']['supersector']}", "gcs_path": f"raw/employment/{config['dimension_files']['supersector']}"},
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/employment/{config['dimension_files']['industry']}"},
            {"key": "datatype", "url": f"{base_url}/{config['dimension_files']['datatype']}", "gcs_path": f"raw/employment/{config['dimension_files']['datatype']}"},
            {"key": "period", "url": f"{base_url}/{config['dimension_files']['period']}", "gcs_path": f"raw/employment/{config['dimension_files']['period']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/employment/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        supersector_dict = load_dimension_dict(dim_data['supersector'], 0)
        print(f"    Supersectors: {len(supersector_dict)}")
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        datatype_dict = load_dimension_dict(dim_data['datatype'], 0)
        print(f"    Data types: {len(datatype_dict)}")
        period_dict = load_dimension_dict(dim_data['period'], 0)
        print(f"    Periods: {len(period_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('supersector', supersector_dict), ('industry', industry_dict), 
                                    ('datatype', datatype_dict), ('period', period_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE ce_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO ce_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_ce_year;
            DROP INDEX IF EXISTS idx_ce_supersector;
            DROP INDEX IF EXISTS idx_ce_industry;
            DROP INDEX IF EXISTS idx_ce_datatype;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        cur.execute(f"""
            INSERT INTO {table} 
            (series_id, year, period, period_name, value, supersector_code, supersector_name,
             industry_code, industry_name, datatype_code, datatype_name, seasonal_code, seasonal_name, footnote_codes)
            SELECT 
                d.series_id, d.year, d.period, p.name, d.value,
                s.supersector_code, sup.name,
                s.industry_code, ind.name,
                s.datatype_code, dt.name,
                s.seasonal, seas.name,
                d.footnote_codes
            FROM ce_data_temp d
            JOIN ce_series_temp s ON d.series_id = s.series_id
            LEFT JOIN ce_supersector_temp sup ON s.supersector_code = sup.code
            LEFT JOIN ce_industry_temp ind ON s.industry_code = ind.code
            LEFT JOIN ce_datatype_temp dt ON s.datatype_code = dt.code
            LEFT JOIN ce_period_temp p ON d.period = p.code
            LEFT JOIN ce_seasonal_temp seas ON s.seasonal = seas.code
        """)
        rows_loaded = cur.rowcount
        conn.commit()
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_ce_year ON ce_data(year);
            CREATE INDEX idx_ce_supersector ON ce_data(supersector_code);
            CREATE INDEX idx_ce_industry ON ce_data(industry_code);
            CREATE INDEX idx_ce_datatype ON ce_data(datatype_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE ce_series_temp, ce_data_temp")
        for dim in ['supersector', 'industry', 'datatype', 'period', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS ce_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('employment', 'all', rows_loaded):
            update_load_log('employment', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('employment', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('employment', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_la_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing LA Unemployment (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('unemployment', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE la_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                area_type_code VARCHAR(10),
                area_code VARCHAR(20),
                measure_code VARCHAR(10),
                seasonal VARCHAR(10),
                srd_code VARCHAR(10),
                series_title VARCHAR(500),
                footnote_codes VARCHAR(10),
                begin_year INTEGER,
                begin_period VARCHAR(10),
                end_year INTEGER,
                end_period VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE la_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/unemployment/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'la_series_temp',
            ['series_id', 'area_type_code', 'area_code', 'measure_code', 'seasonal',
             'srd_code', 'series_title', 'footnote_codes', 'begin_year', 'begin_period', 'end_year', 'end_period'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/unemployment/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'la_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on la_data_temp...")
        cur.execute("CREATE INDEX idx_la_data_temp_series ON la_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_la_data_temp_year ON la_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "area_type", "url": f"{base_url}/{config['dimension_files']['area_type']}", "gcs_path": f"raw/unemployment/{config['dimension_files']['area_type']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/unemployment/{config['dimension_files']['area']}"},
            {"key": "measure", "url": f"{base_url}/{config['dimension_files']['measure']}", "gcs_path": f"raw/unemployment/{config['dimension_files']['measure']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/unemployment/{config['dimension_files']['seasonal']}"},
            {"key": "state", "url": f"{base_url}/{config['dimension_files']['state']}", "gcs_path": f"raw/unemployment/{config['dimension_files']['state']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        area_type_dict = load_dimension_dict(dim_data['area_type'], 0)
        print(f"    Area types: {len(area_type_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        measure_dict = load_dimension_dict(dim_data['measure'], 0)
        print(f"    Measures: {len(measure_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        state_dict = load_dimension_dict(dim_data['state'], 0)
        print(f"    States: {len(state_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('area_type', area_type_dict), ('area', area_dict), 
                                    ('measure', measure_dict), ('seasonal', seasonal_dict), ('state', state_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE la_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO la_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_la_year;
            DROP INDEX IF EXISTS idx_la_area;
            DROP INDEX IF EXISTS idx_la_measure;
            DROP INDEX IF EXISTS idx_la_state;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM la_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, area_type_code, area_type_name,
                 area_code, area_name, measure_code, measure_name, seasonal_code, seasonal_name,
                 state_code, state_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.area_type_code, at.name,
                    s.area_code, a.name,
                    s.measure_code, m.name,
                    s.seasonal, seas.name,
                    s.srd_code, st.name,
                    d.footnote_codes
                FROM la_data_temp d
                JOIN la_series_temp s ON d.series_id = s.series_id
                LEFT JOIN la_area_type_temp at ON s.area_type_code = at.code
                LEFT JOIN la_area_temp a ON s.area_code = a.code
                LEFT JOIN la_measure_temp m ON s.measure_code = m.code
                LEFT JOIN la_seasonal_temp seas ON s.seasonal = seas.code
                LEFT JOIN la_state_temp st ON s.srd_code = st.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_la_year ON la_data(year);
            CREATE INDEX idx_la_area ON la_data(area_code);
            CREATE INDEX idx_la_measure ON la_data(measure_code);
            CREATE INDEX idx_la_state ON la_data(state_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE la_series_temp, la_data_temp")
        for dim in ['area_type', 'area', 'measure', 'seasonal', 'state']:
            cur.execute(f"DROP TABLE IF EXISTS la_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('unemployment', 'all', rows_loaded):
            update_load_log('unemployment', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('unemployment', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('unemployment', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_jt_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing JOLTS (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('jolts', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE jt_series_temp (
                series_id VARCHAR(22) PRIMARY KEY,
                seasonal VARCHAR(10),
                industry_code VARCHAR(10),
                state_code VARCHAR(10),
                area_code VARCHAR(10),
                sizeclass_code VARCHAR(10),
                dataelement_code VARCHAR(10),
                ratelevel_code VARCHAR(10),
                footnote_codes VARCHAR(10),
                begin_year INTEGER,
                begin_period VARCHAR(10),
                end_year INTEGER,
                end_period VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE jt_data_temp (
                series_id VARCHAR(22),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/jolts/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'jt_series_temp',
            ['series_id', 'seasonal', 'industry_code', 'state_code', 'area_code', 'sizeclass_code',
             'dataelement_code', 'ratelevel_code', 'footnote_codes', 'begin_year', 'begin_period',
             'end_year', 'end_period'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/jolts/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'jt_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on jt_data_temp...")
        cur.execute("CREATE INDEX idx_jt_data_temp_series ON jt_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_jt_data_temp_year ON jt_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/jolts/{config['dimension_files']['industry']}"},
            {"key": "state", "url": f"{base_url}/{config['dimension_files']['state']}", "gcs_path": f"raw/jolts/{config['dimension_files']['state']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/jolts/{config['dimension_files']['area']}"},
            {"key": "sizeclass", "url": f"{base_url}/{config['dimension_files']['sizeclass']}", "gcs_path": f"raw/jolts/{config['dimension_files']['sizeclass']}"},
            {"key": "dataelement", "url": f"{base_url}/{config['dimension_files']['dataelement']}", "gcs_path": f"raw/jolts/{config['dimension_files']['dataelement']}"},
            {"key": "ratelevel", "url": f"{base_url}/{config['dimension_files']['ratelevel']}", "gcs_path": f"raw/jolts/{config['dimension_files']['ratelevel']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/jolts/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        state_dict = load_dimension_dict(dim_data['state'], 0)
        print(f"    States: {len(state_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        sizeclass_dict = load_dimension_dict(dim_data['sizeclass'], 0)
        print(f"    Size classes: {len(sizeclass_dict)}")
        dataelement_dict = load_dimension_dict(dim_data['dataelement'], 0)
        print(f"    Data elements: {len(dataelement_dict)}")
        ratelevel_dict = load_dimension_dict(dim_data['ratelevel'], 0)
        print(f"    Rate levels: {len(ratelevel_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('industry', industry_dict), ('state', state_dict), ('area', area_dict),
                                    ('sizeclass', sizeclass_dict), ('dataelement', dataelement_dict),
                                    ('ratelevel', ratelevel_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE jt_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO jt_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_jt_year;
            DROP INDEX IF EXISTS idx_jt_industry;
            DROP INDEX IF EXISTS idx_jt_state;
            DROP INDEX IF EXISTS idx_jt_dataelement;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM jt_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, industry_code, industry_name,
                 state_code, state_name, area_code, area_name, sizeclass_code, sizeclass_name,
                 dataelement_code, dataelement_name, ratelevel_code, ratelevel_name,
                 seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.industry_code, ind.name,
                    s.state_code, st.name,
                    s.area_code, a.name,
                    s.sizeclass_code, sc.name,
                    s.dataelement_code, de.name,
                    s.ratelevel_code, rl.name,
                    s.seasonal, seas.name,
                    d.footnote_codes
                FROM jt_data_temp d
                JOIN jt_series_temp s ON d.series_id = s.series_id
                LEFT JOIN jt_industry_temp ind ON s.industry_code = ind.code
                LEFT JOIN jt_state_temp st ON s.state_code = st.code
                LEFT JOIN jt_area_temp a ON s.area_code = a.code
                LEFT JOIN jt_sizeclass_temp sc ON s.sizeclass_code = sc.code
                LEFT JOIN jt_dataelement_temp de ON s.dataelement_code = de.code
                LEFT JOIN jt_ratelevel_temp rl ON s.ratelevel_code = rl.code
                LEFT JOIN jt_seasonal_temp seas ON s.seasonal = seas.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_jt_year ON jt_data(year);
            CREATE INDEX idx_jt_industry ON jt_data(industry_code);
            CREATE INDEX idx_jt_state ON jt_data(state_code);
            CREATE INDEX idx_jt_dataelement ON jt_data(dataelement_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE jt_series_temp, jt_data_temp")
        for dim in ['industry', 'state', 'area', 'sizeclass', 'dataelement', 'ratelevel', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS jt_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('jolts', 'all', rows_loaded):
            update_load_log('jolts', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('jolts', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('jolts', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_sa_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing State Employment (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('state_employment', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE sa_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                state_code VARCHAR(10),
                area_code VARCHAR(10),
                industry_code VARCHAR(10),
                detail_code VARCHAR(10),
                data_type_code VARCHAR(10),
                seasonal VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE sa_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/state_employment/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'sa_series_temp',
            ['series_id', 'state_code', 'area_code', 'industry_code', 'detail_code', 'data_type_code', 'seasonal'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/state_employment/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'sa_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on sa_data_temp...")
        cur.execute("CREATE INDEX idx_sa_data_temp_series ON sa_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_sa_data_temp_year ON sa_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "state", "url": f"{base_url}/{config['dimension_files']['state']}", "gcs_path": f"raw/state_employment/{config['dimension_files']['state']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/state_employment/{config['dimension_files']['area']}"},
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/state_employment/{config['dimension_files']['industry']}"},
            {"key": "detail", "url": f"{base_url}/{config['dimension_files']['detail']}", "gcs_path": f"raw/state_employment/{config['dimension_files']['detail']}"},
            {"key": "data_type", "url": f"{base_url}/{config['dimension_files']['data_type']}", "gcs_path": f"raw/state_employment/{config['dimension_files']['data_type']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        state_dict = load_dimension_dict(dim_data['state'], 0)
        print(f"    States: {len(state_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        detail_dict = load_dimension_dict(dim_data['detail'], 0)
        print(f"    Details: {len(detail_dict)}")
        data_type_dict = load_dimension_dict(dim_data['data_type'], 0)
        print(f"    Data types: {len(data_type_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('state', state_dict), ('area', area_dict), ('industry', industry_dict),
                                    ('detail', detail_dict), ('data_type', data_type_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE sa_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO sa_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_sa_year;
            DROP INDEX IF EXISTS idx_sa_state;
            DROP INDEX IF EXISTS idx_sa_industry;
            DROP INDEX IF EXISTS idx_sa_data_type;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM sa_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, state_code, state_name,
                 area_code, area_name, industry_code, industry_name, detail_code, detail_name,
                 data_type_code, data_type_name, seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.state_code, st.name,
                    s.area_code, a.name,
                    s.industry_code, ind.name,
                    s.detail_code, det.name,
                    s.data_type_code, dt.name,
                    s.seasonal, CASE s.seasonal WHEN 'S' THEN 'Seasonally Adjusted' WHEN 'U' THEN 'Not Seasonally Adjusted' ELSE NULL END,
                    d.footnote_codes
                FROM sa_data_temp d
                JOIN sa_series_temp s ON d.series_id = s.series_id
                LEFT JOIN sa_state_temp st ON s.state_code = st.code
                LEFT JOIN sa_area_temp a ON s.area_code = a.code
                LEFT JOIN sa_industry_temp ind ON s.industry_code = ind.code
                LEFT JOIN sa_detail_temp det ON s.detail_code = det.code
                LEFT JOIN sa_data_type_temp dt ON s.data_type_code = dt.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_sa_year ON sa_data(year);
            CREATE INDEX idx_sa_state ON sa_data(state_code);
            CREATE INDEX idx_sa_industry ON sa_data(industry_code);
            CREATE INDEX idx_sa_data_type ON sa_data(data_type_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE sa_series_temp, sa_data_temp")
        for dim in ['state', 'area', 'industry', 'detail', 'data_type']:
            cur.execute(f"DROP TABLE IF EXISTS sa_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('state_employment', 'all', rows_loaded):
            update_load_log('state_employment', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('state_employment', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('state_employment', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_oe_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing Occupational Employment (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('occupational', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE oe_series_temp (
                series_id VARCHAR(35) PRIMARY KEY,
                seasonal VARCHAR(10),
                areatype_code VARCHAR(10),
                industry_code VARCHAR(10),
                occupation_code VARCHAR(10),
                datatype_code VARCHAR(10),
                state_code VARCHAR(10),
                area_code VARCHAR(10),
                sector_code VARCHAR(10),
                series_title VARCHAR(500),
                footnote_codes VARCHAR(250),
                begin_year INTEGER,
                begin_period VARCHAR(10),
                end_year INTEGER,
                end_period VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE oe_data_temp (
                series_id VARCHAR(35),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(250)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/occupational/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'oe_series_temp',
            ['series_id', 'seasonal', 'areatype_code', 'industry_code', 'occupation_code', 
             'datatype_code', 'state_code', 'area_code', 'sector_code', 'series_title',
             'footnote_codes', 'begin_year', 'begin_period', 'end_year', 'end_period'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/occupational/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'oe_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on oe_data_temp...")
        cur.execute("CREATE INDEX idx_oe_data_temp_series ON oe_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_oe_data_temp_year ON oe_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "areatype", "url": f"{base_url}/{config['dimension_files']['areatype']}", "gcs_path": f"raw/occupational/{config['dimension_files']['areatype']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/occupational/{config['dimension_files']['area']}"},
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/occupational/{config['dimension_files']['industry']}"},
            {"key": "occupation", "url": f"{base_url}/{config['dimension_files']['occupation']}", "gcs_path": f"raw/occupational/{config['dimension_files']['occupation']}"},
            {"key": "datatype", "url": f"{base_url}/{config['dimension_files']['datatype']}", "gcs_path": f"raw/occupational/{config['dimension_files']['datatype']}"},
            {"key": "sector", "url": f"{base_url}/{config['dimension_files']['sector']}", "gcs_path": f"raw/occupational/{config['dimension_files']['sector']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/occupational/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        areatype_dict = load_dimension_dict(dim_data['areatype'], 0)
        print(f"    Area types: {len(areatype_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        occupation_dict = load_dimension_dict(dim_data['occupation'], 0)
        print(f"    Occupations: {len(occupation_dict)}")
        datatype_dict = load_dimension_dict(dim_data['datatype'], 0)
        print(f"    Data types: {len(datatype_dict)}")
        sector_dict = load_dimension_dict(dim_data['sector'], 0)
        print(f"    Sectors: {len(sector_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [
            ('areatype', areatype_dict), ('area', area_dict), ('industry', industry_dict),
            ('occupation', occupation_dict), ('datatype', datatype_dict), ('sector', sector_dict),
            ('seasonal', seasonal_dict)
        ]:
            cur.execute(f"""
                CREATE TEMP TABLE oe_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO oe_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_oe_year;
            DROP INDEX IF EXISTS idx_oe_area;
            DROP INDEX IF EXISTS idx_oe_industry;
            DROP INDEX IF EXISTS idx_oe_occupation;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM oe_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, areatype_code, areatype_name,
                 area_code, area_name, industry_code, industry_name, occupation_code, occupation_name,
                 datatype_code, datatype_name, sector_code, sector_name,
                 seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.areatype_code, at.name,
                    s.area_code, a.name,
                    s.industry_code, i.name,
                    s.occupation_code, o.name,
                    s.datatype_code, dt.name,
                    s.sector_code, sc.name,
                    s.seasonal, se.name,
                    d.footnote_codes
                FROM oe_data_temp d
                JOIN oe_series_temp s ON d.series_id = s.series_id
                LEFT JOIN oe_areatype_temp at ON s.areatype_code = at.code
                LEFT JOIN oe_area_temp a ON s.area_code = a.code
                LEFT JOIN oe_industry_temp i ON s.industry_code = i.code
                LEFT JOIN oe_occupation_temp o ON s.occupation_code = o.code
                LEFT JOIN oe_datatype_temp dt ON s.datatype_code = dt.code
                LEFT JOIN oe_sector_temp sc ON s.sector_code = sc.code
                LEFT JOIN oe_seasonal_temp se ON s.seasonal = se.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_oe_year ON oe_data(year);
            CREATE INDEX idx_oe_area ON oe_data(area_code);
            CREATE INDEX idx_oe_industry ON oe_data(industry_code);
            CREATE INDEX idx_oe_occupation ON oe_data(occupation_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE oe_series_temp, oe_data_temp")
        for dim in ['areatype', 'area', 'industry', 'occupation', 'datatype', 'sector', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS oe_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('occupational', 'all', rows_loaded):
            update_load_log('occupational', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('occupational', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('occupational', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_ci_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing Compensation (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('compensation', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE ci_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                seasonal VARCHAR(10),
                owner_code VARCHAR(10),
                industry_code VARCHAR(10),
                occupation_code VARCHAR(10),
                subcell_code VARCHAR(10),
                area_code VARCHAR(10),
                periodicity_code VARCHAR(10),
                estimate_code VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE ci_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/compensation/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'ci_series_temp',
            ['series_id', 'seasonal', 'owner_code', 'industry_code', 'occupation_code', 
             'subcell_code', 'area_code', 'periodicity_code', 'estimate_code'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/compensation/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'ci_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on ci_data_temp...")
        cur.execute("CREATE INDEX idx_ci_data_temp_series ON ci_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_ci_data_temp_year ON ci_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "owner", "url": f"{base_url}/{config['dimension_files']['owner']}", "gcs_path": f"raw/compensation/{config['dimension_files']['owner']}"},
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/compensation/{config['dimension_files']['industry']}"},
            {"key": "occupation", "url": f"{base_url}/{config['dimension_files']['occupation']}", "gcs_path": f"raw/compensation/{config['dimension_files']['occupation']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/compensation/{config['dimension_files']['area']}"},
            {"key": "estimate", "url": f"{base_url}/{config['dimension_files']['estimate']}", "gcs_path": f"raw/compensation/{config['dimension_files']['estimate']}"},
            {"key": "periodicity", "url": f"{base_url}/{config['dimension_files']['periodicity']}", "gcs_path": f"raw/compensation/{config['dimension_files']['periodicity']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/compensation/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        owner_dict = load_dimension_dict(dim_data['owner'], 0)
        print(f"    Owners: {len(owner_dict)}")
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        occupation_dict = load_dimension_dict(dim_data['occupation'], 0)
        print(f"    Occupations: {len(occupation_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        estimate_dict = load_dimension_dict(dim_data['estimate'], 0)
        print(f"    Estimates: {len(estimate_dict)}")
        periodicity_dict = load_dimension_dict(dim_data['periodicity'], 0)
        print(f"    Periodicities: {len(periodicity_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('owner', owner_dict), ('industry', industry_dict), ('occupation', occupation_dict),
                                    ('area', area_dict), ('estimate', estimate_dict), 
                                    ('periodicity', periodicity_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE ci_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO ci_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_ci_year;
            DROP INDEX IF EXISTS idx_ci_industry;
            DROP INDEX IF EXISTS idx_ci_occupation;
            DROP INDEX IF EXISTS idx_ci_estimate;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM ci_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, owner_code, owner_name,
                 industry_code, industry_name, occupation_code, occupation_name, area_code, area_name,
                 estimate_code, estimate_name, periodicity_code, periodicity_name,
                 seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.owner_code, o.name,
                    s.industry_code, ind.name,
                    s.occupation_code, occ.name,
                    s.area_code, a.name,
                    s.estimate_code, e.name,
                    s.periodicity_code, p.name,
                    s.seasonal, seas.name,
                    d.footnote_codes
                FROM ci_data_temp d
                JOIN ci_series_temp s ON d.series_id = s.series_id
                LEFT JOIN ci_owner_temp o ON s.owner_code = o.code
                LEFT JOIN ci_industry_temp ind ON s.industry_code = ind.code
                LEFT JOIN ci_occupation_temp occ ON s.occupation_code = occ.code
                LEFT JOIN ci_area_temp a ON s.area_code = a.code
                LEFT JOIN ci_estimate_temp e ON s.estimate_code = e.code
                LEFT JOIN ci_periodicity_temp p ON s.periodicity_code = p.code
                LEFT JOIN ci_seasonal_temp seas ON s.seasonal = seas.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_ci_year ON ci_data(year);
            CREATE INDEX idx_ci_industry ON ci_data(industry_code);
            CREATE INDEX idx_ci_occupation ON ci_data(occupation_code);
            CREATE INDEX idx_ci_estimate ON ci_data(estimate_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE ci_series_temp, ci_data_temp")
        for dim in ['owner', 'industry', 'occupation', 'area', 'estimate', 'periodicity', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS ci_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('compensation', 'all', rows_loaded):
            update_load_log('compensation', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('compensation', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('compensation', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_mp_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing Mass Layoff (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('productivity', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE mp_series_temp (
                series_id VARCHAR(20) PRIMARY KEY,
                sector_code VARCHAR(10),
                measure_code VARCHAR(10),
                duration_code VARCHAR(10),
                base_year VARCHAR(10),
                series_title VARCHAR(500),
                footnote_codes VARCHAR(10),
                begin_year INTEGER,
                begin_period VARCHAR(10),
                end_year INTEGER,
                end_period VARCHAR(10)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE mp_data_temp (
                series_id VARCHAR(20),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/productivity/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'mp_series_temp',
            ['series_id', 'sector_code', 'measure_code', 'duration_code', 
             'base_year', 'series_title', 'footnote_codes', 'begin_year', 'begin_period', 'end_year', 'end_period'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/productivity/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'mp_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on mp_data_temp...")
        cur.execute("CREATE INDEX idx_mp_data_temp_series ON mp_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_mp_data_temp_year ON mp_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "sector", "url": f"{base_url}/{config['dimension_files']['sector']}", "gcs_path": f"raw/productivity/{config['dimension_files']['sector']}"},
            {"key": "measure", "url": f"{base_url}/{config['dimension_files']['measure']}", "gcs_path": f"raw/productivity/{config['dimension_files']['measure']}"},
            {"key": "duration", "url": f"{base_url}/{config['dimension_files']['duration']}", "gcs_path": f"raw/productivity/{config['dimension_files']['duration']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/productivity/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        sector_dict = load_dimension_dict(dim_data['sector'], 0)
        print(f"    Sectors: {len(sector_dict)}")
        measure_dict = load_dimension_dict(dim_data['measure'], 0)
        print(f"    Measures: {len(measure_dict)}")
        duration_dict = load_dimension_dict(dim_data['duration'], 0)
        print(f"    Durations: {len(duration_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('sector', sector_dict), ('measure', measure_dict), 
                                    ('duration', duration_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE mp_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO mp_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_mp_year;
            DROP INDEX IF EXISTS idx_mp_sector;
            DROP INDEX IF EXISTS idx_mp_measure;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM mp_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 10
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, sector_code, sector_name,
                 measure_code, measure_name, duration_code, duration_name,
                 seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.sector_code, sec.name,
                    s.measure_code, m.name,
                    s.duration_code, dur.name,
                    SUBSTRING(s.series_id, 3, 1), seas.name,
                    d.footnote_codes
                FROM mp_data_temp d
                JOIN mp_series_temp s ON d.series_id = s.series_id
                LEFT JOIN mp_sector_temp sec ON s.sector_code = sec.code
                LEFT JOIN mp_measure_temp m ON s.measure_code = m.code
                LEFT JOIN mp_duration_temp dur ON s.duration_code = dur.code
                LEFT JOIN mp_seasonal_temp seas ON SUBSTRING(s.series_id, 3, 1) = seas.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_mp_year ON mp_data(year);
            CREATE INDEX idx_mp_sector ON mp_data(sector_code);
            CREATE INDEX idx_mp_measure ON mp_data(measure_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE mp_series_temp, mp_data_temp")
        for dim in ['sector', 'measure', 'duration', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS mp_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('productivity', 'all', rows_loaded):
            update_load_log('productivity', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('productivity', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('productivity', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_sm_denormalized(client, config: dict, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing State/Metro Employment (denormalized)...")
    
    base_url = config['base_url']
    table = config['table']
    ensure_table_exists(table, config.get('schema_file', ''))
    data_url = f"{base_url}/{config['data_file']}"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_data = force or should_reload('state_metro', 'all', etag)
        if not reload_data:
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\n  Creating temp tables...")
        cur.execute("""
            CREATE TEMP TABLE sm_series_temp (
                series_id VARCHAR(22) PRIMARY KEY,
                state_code VARCHAR(2),
                area_code VARCHAR(10),
                supersector_code VARCHAR(2),
                industry_code VARCHAR(10),
                data_type_code VARCHAR(2),
                seasonal_code VARCHAR(1)
            )
        """)
        cur.execute("""
            CREATE TEMP TABLE sm_data_temp (
                series_id VARCHAR(22),
                year INTEGER,
                period VARCHAR(10),
                value NUMERIC,
                footnote_codes VARCHAR(10)
            )
        """)
        conn.commit()
        
        print("\n  Downloading series file...")
        series_path = f"raw/state_metro/{config['series_file']}"
        ensure_gcs_file(f"{base_url}/{config['series_file']}", series_path, client, force=force, reload=reload_data)
        
        print("  Loading series into temp table via COPY...")
        series_rows = copy_from_gcs_to_table(
            series_path, 'sm_series_temp',
            ['series_id', 'state_code', 'area_code', 'supersector_code', 'industry_code', 'data_type_code', 'seasonal_code'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total series: {series_rows:,}")
        
        print("\n  Downloading data file...")
        data_path = f"raw/state_metro/{config['data_file']}"
        ensure_gcs_file(f"{base_url}/{config['data_file']}", data_path, client, force=force, reload=reload_data)
        
        print("  Loading data into temp table via COPY...")
        data_rows = copy_from_gcs_to_table(
            data_path, 'sm_data_temp',
            ['series_id', 'year', 'period', 'value', 'footnote_codes'],
            client, cur, skip_header=True
        )
        conn.commit()
        print(f"    Total data rows: {data_rows:,}")
        
        print("\n  Creating indexes on sm_data_temp...")
        cur.execute("CREATE INDEX idx_sm_data_temp_series ON sm_data_temp(series_id)")
        cur.execute("CREATE INDEX idx_sm_data_temp_year ON sm_data_temp(year)")
        conn.commit()
        
        print("\n  Loading dimension lookups in parallel...")
        dim_files = [
            {"key": "state", "url": f"{base_url}/{config['dimension_files']['state']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['state']}"},
            {"key": "area", "url": f"{base_url}/{config['dimension_files']['area']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['area']}"},
            {"key": "supersector", "url": f"{base_url}/{config['dimension_files']['supersector']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['supersector']}"},
            {"key": "industry", "url": f"{base_url}/{config['dimension_files']['industry']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['industry']}"},
            {"key": "data_type", "url": f"{base_url}/{config['dimension_files']['data_type']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['data_type']}"},
            {"key": "seasonal", "url": f"{base_url}/{config['dimension_files']['seasonal']}", "gcs_path": f"raw/state_metro/{config['dimension_files']['seasonal']}"},
        ]
        dim_data = download_files_parallel(dim_files, client, force=force, reload=reload_data)
        
        state_dict = load_dimension_dict(dim_data['state'], 0)
        print(f"    States: {len(state_dict)}")
        area_dict = load_dimension_dict(dim_data['area'], 0)
        print(f"    Areas: {len(area_dict)}")
        supersector_dict = load_dimension_dict(dim_data['supersector'], 0)
        print(f"    Supersectors: {len(supersector_dict)}")
        industry_dict = load_dimension_dict(dim_data['industry'], 0)
        print(f"    Industries: {len(industry_dict)}")
        data_type_dict = load_dimension_dict(dim_data['data_type'], 0)
        print(f"    Data types: {len(data_type_dict)}")
        seasonal_dict = load_dimension_dict(dim_data['seasonal'], 0)
        print(f"    Seasonal: {len(seasonal_dict)}")
        
        print("\n  Creating dimension temp tables...")
        for dim_name, dim_dict in [('state', state_dict), ('area', area_dict), ('supersector', supersector_dict),
                                    ('industry', industry_dict), ('data_type', data_type_dict), ('seasonal', seasonal_dict)]:
            cur.execute(f"""
                CREATE TEMP TABLE sm_{dim_name}_temp (code VARCHAR(50) PRIMARY KEY, name VARCHAR(255))
            """)
            if dim_dict:
                rows = [(k, v[1] if len(v) > 1 else None) for k, v in dim_dict.items()]
                execute_values(cur, f"INSERT INTO sm_{dim_name}_temp (code, name) VALUES %s ON CONFLICT DO NOTHING", rows)
            conn.commit()
        
        print("\n  Dropping indexes for faster insert...")
        cur.execute("""
            DROP INDEX IF EXISTS idx_sm_year;
            DROP INDEX IF EXISTS idx_sm_state;
            DROP INDEX IF EXISTS idx_sm_industry;
            DROP INDEX IF EXISTS idx_sm_supersector;
        """)
        conn.commit()
        
        print("  Denormalizing and inserting into final table...")
        cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
        
        # Get year range for chunked insert to avoid statement timeout
        cur.execute("SELECT MIN(year), MAX(year) FROM sm_data_temp")
        min_year, max_year = cur.fetchone()
        CHUNK_YEARS = 2
        rows_loaded = 0
        for chunk_start in range(min_year, max_year + 1, CHUNK_YEARS):
            chunk_end = min(chunk_start + CHUNK_YEARS - 1, max_year)
            cur.execute(f"""
                INSERT INTO {table} 
                (series_id, year, period, period_name, value, state_code, state_name,
                 area_code, area_name, supersector_code, supersector_name, industry_code, industry_name,
                 data_type_code, data_type_name, seasonal_code, seasonal_name, footnote_codes)
                SELECT 
                    d.series_id, d.year, d.period,
                    CASE d.period
                        WHEN 'M01' THEN 'JAN' WHEN 'M02' THEN 'FEB'
                        WHEN 'M03' THEN 'MAR' WHEN 'M04' THEN 'APR'
                        WHEN 'M05' THEN 'MAY' WHEN 'M06' THEN 'JUN'
                        WHEN 'M07' THEN 'JUL' WHEN 'M08' THEN 'AUG'
                        WHEN 'M09' THEN 'SEP' WHEN 'M10' THEN 'OCT'
                        WHEN 'M11' THEN 'NOV' WHEN 'M12' THEN 'DEC'
                        WHEN 'M13' THEN 'AN AV'
                        WHEN 'S01' THEN 'HALF1' WHEN 'S02' THEN 'HALF2'
                        WHEN 'S03' THEN 'AN AV'
                        WHEN 'Q01' THEN 'QTR1' WHEN 'Q02' THEN 'QTR2'
                        WHEN 'Q03' THEN 'QTR3' WHEN 'Q04' THEN 'QTR4'
                        WHEN 'A01' THEN 'AN AV'
                        ELSE d.period
                    END,
                    d.value,
                    s.state_code, st.name,
                    s.area_code, a.name,
                    s.supersector_code, ss.name,
                    s.industry_code, i.name,
                    s.data_type_code, dt.name,
                    s.seasonal_code, seas.name,
                    d.footnote_codes
                FROM sm_data_temp d
                JOIN sm_series_temp s ON d.series_id = s.series_id
                LEFT JOIN sm_state_temp st ON s.state_code = st.code
                LEFT JOIN sm_area_temp a ON s.area_code = a.code
                LEFT JOIN sm_supersector_temp ss ON s.supersector_code = ss.code
                LEFT JOIN sm_industry_temp i ON s.industry_code = i.code
                LEFT JOIN sm_data_type_temp dt ON s.data_type_code = dt.code
                LEFT JOIN sm_seasonal_temp seas ON s.seasonal_code = seas.code
                WHERE d.year BETWEEN {chunk_start} AND {chunk_end}
            """)
            chunk_rows = cur.rowcount
            rows_loaded += chunk_rows
            conn.commit()
            print(f"    Years {chunk_start}-{chunk_end}: {chunk_rows:,} rows (total: {rows_loaded:,})")
        
        print("  Recreating indexes...")
        cur.execute("""
            CREATE INDEX idx_sm_year ON sm_data(year);
            CREATE INDEX idx_sm_state ON sm_data(state_code);
            CREATE INDEX idx_sm_industry ON sm_data(industry_code);
            CREATE INDEX idx_sm_supersector ON sm_data(supersector_code);
        """)
        conn.commit()
        
        cur.execute("DROP TABLE sm_series_temp, sm_data_temp")
        for dim in ['state', 'area', 'supersector', 'industry', 'data_type', 'seasonal']:
            cur.execute(f"DROP TABLE IF EXISTS sm_{dim}_temp")
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"  Loaded {rows_loaded:,} rows into {table}")
        
        if not validate_row_count('state_metro', 'all', rows_loaded):
            update_load_log('state_metro', 'all', etag, last_modified, data_url, rows_loaded, 'failed', f'Row count validation failed: {rows_loaded:,} rows is less than 50% of previous load')
            return False
        
        update_load_log('state_metro', 'all', etag, last_modified, data_url, rows_loaded, 'success')
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log('state_metro', 'all', etag or '', last_modified, data_url, 0, 'failed', str(e))
        return False

def load_series_mapping(data_bytes: bytes) -> dict:
    print(f"  Loading series mapping...")
    
    text = data_bytes.decode('utf-8', errors='replace').replace('\x00', '')
    lines = text.strip().split('\n')
    
    if lines and lines[0].startswith('series_id'):
        lines = lines[1:]
    
    series_map = {}
    for line in lines:
        if not line.strip():
            continue
        parts = line.split('\t')
        if len(parts) >= 3:
            series_id = parts[0].strip()
            area_code = parts[1].strip() if len(parts) > 1 else None
            item_code = parts[2].strip() if len(parts) > 2 else None
            seasonal = parts[3].strip() if len(parts) > 3 else None
            
            if series_id and area_code and item_code:
                series_map[series_id] = {
                    'area_code': area_code,
                    'item_code': item_code,
                    'seasonal_code': seasonal
                }
    
    print(f"    Loaded {len(series_map):,} series mappings")
    return series_map

def load_fact_table(data_bytes: bytes, table: str, series_map: dict = None) -> int:
    print(f"  Loading fact table {table}...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute(f"TRUNCATE TABLE {table}")
    conn.commit()
    
    text = data_bytes.decode('utf-8', errors='replace').replace('\x00', '')
    lines = text.strip().split('\n')
    
    if lines and lines[0].startswith('series_id'):
        lines = lines[1:]
    
    if series_map:
        columns = ["series_id", "area_code", "item_code", "seasonal_code", "year", "period", "value"]
    else:
        columns = ["series_id", "year", "period", "value"]
    col_str = ", ".join(columns)
    
    print(f"  Parsing {len(lines):,} rows...")
    
    BATCH_SIZE = 10000
    rows_loaded = 0
    skipped = 0
    batch = []
    
    for line in lines:
        if not line.strip():
            continue
        
        parts = line.split('\t')
        if len(parts) >= 4:
            series_id = parts[0].strip()
            year = parts[1].strip()
            period = parts[2].strip()
            value = parts[3].strip()
            
            if value == '' or value == '-':
                value = None
            
            if series_map:
                mapping = series_map.get(series_id)
                if not mapping:
                    skipped += 1
                    continue
                
                values = [
                    series_id,
                    mapping['area_code'],
                    mapping['item_code'],
                    mapping['seasonal_code'],
                    int(year) if year else None,
                    period,
                    float(value) if value else None
                ]
            else:
                values = [series_id, int(year) if year else None, period, float(value) if value else None]
            
            batch.append(tuple(values))
            
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, f"INSERT INTO {table} ({col_str}) VALUES %s", batch)
                conn.commit()
                rows_loaded += len(batch)
                if rows_loaded % 100000 == 0:
                    print(f"    Loaded {rows_loaded:,} rows...")
                batch = []
    
    if batch:
        execute_values(cur, f"INSERT INTO {table} ({col_str}) VALUES %s", batch)
        conn.commit()
        rows_loaded += len(batch)
    
    cur.close()
    conn.close()
    
    if skipped > 0:
        print(f"  Skipped {skipped:,} rows (no series mapping)")
    print(f"  Loaded {rows_loaded:,} rows into {table}")
    return rows_loaded

def process_star_dataset(dataset_key: str, client, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing {dataset_key} (star schema)...")
    
    config = BLS_DATASETS[dataset_key]
    base_url = config['base_url']
    total_rows = 0
    series_map = None
    
    try:
        for dim_key, dim_config in config['dimensions'].items():
            print(f"\n  Dimension: {dim_key}")
            url = f"{base_url}/{dim_config['file']}"
            gcs_path = f"raw/{dataset_key}/{dim_config['file']}"
            
            etag, last_modified = get_etag_and_last_modified(url)
            
            if not force and not should_reload(f"{dataset_key}_{dim_key}", 'all', etag):
                print(f"    ETag unchanged, skipping...")
                continue
            
            reload_dim = force or should_reload(f"{dataset_key}_{dim_key}", 'all', etag)
            data_bytes = ensure_gcs_file(url, gcs_path, client, force=force, reload=reload_dim)
            columns = dim_config.get('columns')
            rows = load_dimension_table(data_bytes, dim_config['table'], dim_key, columns)
            total_rows += rows
            
            update_load_log(f"{dataset_key}_{dim_key}", 'all', etag, last_modified, url, rows, 'success')
        
        if config.get('series_file'):
            print(f"\n  Loading series file for mapping...")
            series_url = f"{base_url}/{config['series_file']}"
            series_gcs_path = f"raw/{dataset_key}/{config['series_file']}"
            
            series_bytes = ensure_gcs_file(series_url, series_gcs_path, client, force=force, reload=force)
            series_map = load_series_mapping(series_bytes)
        
        print(f"\n  Fact table: {config['fact_table']}")
        data_url = f"{base_url}/{config['data_file']}"
        gcs_path = f"raw/{dataset_key}/{config['data_file']}"
        
        etag, last_modified = get_etag_and_last_modified(data_url)
        
        reload_fact = force or should_reload(f"{dataset_key}_fact", 'all', etag)
        data_bytes = ensure_gcs_file(data_url, gcs_path, client, force=force, reload=reload_fact)
        rows = load_fact_table(data_bytes, config['fact_table'], series_map)
        total_rows += rows
        
        if not validate_row_count(f"{dataset_key}_fact", 'all', rows):
            update_load_log(f"{dataset_key}_fact", 'all', etag, last_modified, data_url, rows, 'failed', f'Row count validation failed: {rows:,} rows is less than 50% of previous load')
            return False
        
        update_load_log(f"{dataset_key}_fact", 'all', etag, last_modified, data_url, rows, 'success')
        
        print(f"\n  {dataset_key} complete! Total rows: {total_rows:,}")
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_bulk_dataset(dataset_key: str, client, force: bool = False) -> bool:
    print(f"\n{'='*50}")
    print(f"Processing {dataset_key}...")
    
    config = BLS_DATASETS[dataset_key]
    url = config['url']
    table = config['table']
    columns = config['columns']
    gcs_path = f"raw/{dataset_key}/data.tsv"
    etag: Optional[str] = None
    last_modified: Optional[datetime] = None
    
    try:
        etag, last_modified = get_etag_and_last_modified(url)
        
        if not force and not should_reload(dataset_key, 'all', etag):
            print(f"  ETag unchanged ({etag}), skipping...")
            return True
        
        print(f"  ETag: {etag}")
        print(f"  Last-Modified: {last_modified}")
        
        reload_data = force or should_reload(dataset_key, 'all', etag)
        data_bytes = ensure_gcs_file(url, gcs_path, client, force=force, reload=reload_data)
        print(f"  Downloaded from GCS: {len(data_bytes):,} bytes")
        
        rows = load_bulk_data(data_bytes, table, columns)
        
        if not validate_row_count(dataset_key, 'all', rows):
            update_load_log(dataset_key, 'all', etag, last_modified, url, rows, 'failed', f'Row count validation failed: {rows:,} rows is less than 50% of previous load')
            return False
        
        update_load_log(dataset_key, 'all', etag, last_modified, url, rows, 'success')
        
        print(f"  {dataset_key} complete! {rows:,} rows")
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        update_load_log(dataset_key, 'all', etag or '', last_modified, url, 0, 'failed', str(e))
        return False

def get_table_count(table: str) -> int:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def main():
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("Set them in .env file or export them before running.")
        return
    
    import argparse
    parser = argparse.ArgumentParser(description="BLS Unified Data Pipeline")
    parser.add_argument("--datasets", "-d", help="Comma-separated datasets to process (default: all)")
    parser.add_argument("--force", "-f", action="store_true", help="Force re-download")
    parser.add_argument("--status", "-s", action="store_true", help="Show status only")
    parser.add_argument("--workers", "-w", type=int, default=4, help="Parallel workers (default: 4)")
    args = parser.parse_args()
    
    print("BLS Unified Data Pipeline")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    if args.status:
        conn = get_db_connection()
        cur = conn.cursor()
        
        print("\nDataset Counts:")
        for key, cfg in BLS_DATASETS.items():
            if cfg['type'] in ('bulk', 'denormalized'):
                cur.execute(f"SELECT reltuples::bigint FROM pg_class WHERE relname='{cfg['table']}'")
                row = cur.fetchone()
                est = row[0] if row and row[0] >= 0 else 0
                print(f"  {key} ({cfg['table']}): ~{est:,} rows (estimated)")
        
        print("\nLoad Log (last 10):")
        cur.execute("SELECT dataset, data_key, etag, status, rows_loaded, load_timestamp FROM bls_load_log ORDER BY load_timestamp DESC LIMIT 10")
        for row in cur.fetchall():
            print(f"  {row[0]}/{row[1]}: {row[2] or 'N/A'} | {row[3]} | {row[4]:,} rows | {row[5]}")
        cur.close()
        conn.close()
        return
    
    client = get_gcs_client()
    
    if args.datasets:
        bulk_datasets = [d.strip().lower() for d in args.datasets.split(',') if d.strip().lower() in BLS_DATASETS]
    else:
        bulk_datasets = [k for k, v in BLS_DATASETS.items() if v['type'] in ('bulk', 'star', 'denormalized')]
    
    print(f"\nDatasets to process: {bulk_datasets} (workers: {args.workers})")
    
    def get_load_func(dataset_key):
        load_funcs = {
            'cpi': load_cpi_denormalized,
            'ppi': load_ppi_denormalized,
            'employment': load_employment_denormalized,
            'unemployment': load_la_denormalized,
            'jolts': load_jt_denormalized,
            'state_employment': load_sa_denormalized,
            'occupational': load_oe_denormalized,
            'compensation': load_ci_denormalized,
            'productivity': load_mp_denormalized,
            'state_metro': load_sm_denormalized,
        }
        return load_funcs.get(dataset_key)
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for dataset_key in bulk_datasets:
            cfg = BLS_DATASETS.get(dataset_key)
            if cfg and cfg.get('type') == 'denormalized':
                load_func = get_load_func(dataset_key)
                if load_func:
                    futures[executor.submit(load_func, client, cfg, args.force)] = dataset_key
            elif cfg and cfg.get('type') == 'star':
                futures[executor.submit(process_star_dataset, dataset_key, client, args.force)] = dataset_key
            elif cfg and cfg.get('type') == 'bulk':
                futures[executor.submit(process_bulk_dataset, dataset_key, client, args.force)] = dataset_key
        
        for future in as_completed(futures):
            dataset_key = futures[future]
            try:
                future.result()
                print(f"  {dataset_key}: complete")
            except Exception as e:
                print(f"  {dataset_key}: FAILED - {e}")
    
    print("\n" + "=" * 50)
    print("Pipeline complete!")

if __name__ == "__main__":
    main()
