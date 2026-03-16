import os
import requests
import zipfile
import io
import hashlib
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlparse
from email.utils import parsedate_to_datetime
from .config import BLS_DATASETS, DB_CONFIG
import psycopg2

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def get_etag_and_last_modified(url: str) -> Tuple[Optional[str], Optional[datetime]]:
    try:
        resp = requests.head(url, timeout=30, allow_redirects=True, headers=HEADERS)
        if resp.status_code == 200:
            etag = resp.headers.get("ETag", "").strip('"')
            last_modified_str = resp.headers.get("Last-Modified")
            last_modified = None
            if last_modified_str:
                last_modified = parsedate_to_datetime(last_modified_str)
            return etag, last_modified
    except Exception as e:
        print(f"  Warning: Could not fetch headers: {e}")
    return None, None

def get_last_modified(url):
    _, last_modified = get_etag_and_last_modified(url)
    return last_modified

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_load_log_entry(dataset: str, data_key: str) -> Optional[Dict[str, Any]]:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT etag, last_modified, rows_loaded, load_timestamp, status FROM bls_load_log WHERE dataset = %s AND data_key = %s",
            (dataset, data_key)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return {
                'etag': row[0],
                'last_modified': row[1],
                'rows_loaded': row[2],
                'load_timestamp': row[3],
                'status': row[4]
            }
    except Exception as e:
        print(f"  Warning: Could not check load log: {e}")
    return None

def update_load_log(dataset: str, data_key: str, etag: Optional[str], last_modified: Optional[datetime],
                    source_url: str, rows_loaded: int, status: str = 'success', error_message: Optional[str] = None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
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
        cur.close()
        conn.close()
    except Exception as e:
        print(f"  Warning: Could not update load log: {e}")

def should_download(dataset: str, data_key: str, current_etag: Optional[str]) -> bool:
    if not current_etag:
        return True
    entry = get_load_log_entry(dataset, data_key)
    if not entry:
        return True
    if entry['status'] == 'failed':
        return True
    return entry['etag'] != current_etag

def compute_checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def download_file(url, dest_path, dataset_key, data_key=None):
    print(f"  Downloading {dataset_key} from {url}")
    
    etag, last_modified = get_etag_and_last_modified(url)
    data_key = data_key or dataset_key
    
    if etag:
        print(f"  ETag: {etag}")
    if last_modified:
        print(f"  Last-Modified: {last_modified}")
    
    if not should_download(dataset_key, data_key, etag):
        print(f"  ETag unchanged, skipping download")
        return None, None, None
    
    resp = requests.get(url, stream=True, timeout=300, headers=HEADERS)
    resp.raise_for_status()
    
    etag = resp.headers.get("ETag", "").strip('"') or etag
    last_modified_str = resp.headers.get("Last-Modified")
    if last_modified_str:
        last_modified = parsedate_to_datetime(last_modified_str)
    
    total_size = int(resp.headers.get("content-length", 0))
    downloaded = 0
    
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = (downloaded / total_size) * 100
                if int(pct) % 10 == 0:
                    print(f"  Progress: {pct:.1f}%")
    
    print(f"  Saved to {dest_path} ({downloaded:,} bytes)")
    return dest_path, etag, last_modified

def download_qcew_year(year, dest_dir, force=False):
    url = BLS_DATASETS["qcew"]["url"].format(year=year)
    dest_path = os.path.join(dest_dir, f"qcew_{year}.zip")
    
    print(f"  Downloading QCEW {year}...")
    
    etag, last_modified = get_etag_and_last_modified(url)
    if etag:
        print(f"  ETag: {etag}")
    if last_modified:
        print(f"  Last-Modified: {last_modified}")
    
    if not force and not should_download("qcew", str(year), etag):
        print(f"  ETag unchanged, skipping QCEW {year}")
        return None, None, None
    
    try:
        resp = requests.get(url, stream=True, timeout=600, headers=HEADERS)
        resp.raise_for_status()
        
        etag = resp.headers.get("ETag", "").strip('"') or etag
        last_modified_str = resp.headers.get("Last-Modified")
        if last_modified_str:
            last_modified = parsedate_to_datetime(last_modified_str)
        
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"  Saved {dest_path}")
        return dest_path, etag, last_modified
    except Exception as e:
        print(f"  Error downloading QCEW {year}: {e}")
        return None, None, None

def extract_qcew_zip(zip_path, dest_dir):
    print(f"  Extracting {zip_path}...")
    
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        for csv_file in csv_files:
            zf.extract(csv_file, dest_dir)
            print(f"  Extracted {csv_file}")
    
    return csv_files

def download_dataset(dataset_key, force=False):
    ensure_data_dir()
    
    if dataset_key not in BLS_DATASETS:
        print(f"Unknown dataset: {dataset_key}")
        return None
    
    dataset = BLS_DATASETS[dataset_key]
    print(f"\n{'='*60}")
    print(f"Dataset: {dataset['name']} ({dataset_key})")
    print(f"{'='*60}")
    
    result = {
        "files": [],
        "etags": [],
        "last_modifieds": [],
        "urls": []
    }
    
    if dataset_key == "qcew":
        year_range = dataset.get("year_range", range(2020, 2025))
        qcew_dir = os.path.join(DATA_DIR, "qcew")
        os.makedirs(qcew_dir, exist_ok=True)
        
        for year in year_range:
            zip_path = os.path.join(qcew_dir, f"{year}_qtrly_singlefile.zip")
            url = BLS_DATASETS["qcew"]["url"].format(year=year)
            
            if os.path.exists(zip_path) and not force:
                entry = get_load_log_entry("qcew", str(year))
                if entry and entry['status'] == 'success':
                    print(f"  QCEW {year} already exists and loaded, skipping")
                    result["files"].append(zip_path)
                    result["etags"].append(entry['etag'])
                    result["last_modifieds"].append(entry['last_modified'])
                    result["urls"].append(url)
                    continue
            
            file_path, etag, last_modified = download_qcew_year(year, qcew_dir, force=force)
            if file_path:
                result["files"].append(file_path)
                result["etags"].append(etag)
                result["last_modifieds"].append(last_modified)
                result["urls"].append(url)
        
        return result
    elif "urls" in dataset:
        for i, url in enumerate(dataset["urls"]):
            suffix = f"_{i+1}" if len(dataset["urls"]) > 1 else ""
            data_key = f"part{i+1}" if len(dataset["urls"]) > 1 else "all"
            dest_path = os.path.join(DATA_DIR, f"{dataset['file_prefix']}_data{suffix}.csv")
            
            if os.path.exists(dest_path) and not force:
                entry = get_load_log_entry(dataset_key, data_key)
                if entry and entry['status'] == 'success':
                    print(f"  File exists and loaded: {dest_path}")
                    result["files"].append(dest_path)
                    result["etags"].append(entry['etag'])
                    result["last_modifieds"].append(entry['last_modified'])
                    result["urls"].append(url)
                    continue
            
            file_path, etag, last_modified = download_file(url, dest_path, dataset_key, data_key)
            if file_path:
                result["files"].append(file_path)
                result["etags"].append(etag)
                result["last_modifieds"].append(last_modified)
                result["urls"].append(url)
        
        return result
    else:
        url = dataset["url"]
        dest_path = os.path.join(DATA_DIR, f"{dataset['file_prefix']}_data.csv")
        
        if os.path.exists(dest_path) and not force:
            entry = get_load_log_entry(dataset_key, "all")
            if entry and entry['status'] == 'success':
                print(f"  File exists and loaded: {dest_path}")
                print(f"  Use force=True to re-download")
                result["files"].append(dest_path)
                result["etags"].append(entry['etag'])
                result["last_modifieds"].append(entry['last_modified'])
                result["urls"].append(url)
                return result
        
        file_path, etag, last_modified = download_file(url, dest_path, dataset_key, "all")
        if file_path:
            result["files"].append(file_path)
            result["etags"].append(etag)
            result["last_modifieds"].append(last_modified)
            result["urls"].append(url)
        
        return result

def download_dimension_files(dataset_key, force=False):
    ensure_data_dir()
    
    if dataset_key not in BLS_DATASETS:
        print(f"Unknown dataset: {dataset_key}")
        return None
    
    dataset = BLS_DATASETS[dataset_key]
    dimensions = dataset.get("dimensions", [])
    
    if not dimensions:
        print(f"  No dimension files defined for {dataset_key}")
        return []
    
    dim_dir = os.path.join(DATA_DIR, "dimensions")
    os.makedirs(dim_dir, exist_ok=True)
    
    downloaded = []
    for dim_file in dimensions:
        code = dim_file.split(".")[0]
        url = f"https://download.bls.gov/pub/time.series/{code}/{dim_file}"
        dest_path = os.path.join(dim_dir, f"{dim_file.replace('.', '_')}.csv")
        
        if os.path.exists(dest_path) and not force:
            print(f"  Dimension exists: {dim_file}")
            downloaded.append(dest_path)
        else:
            print(f"  Downloading dimension: {dim_file}")
            try:
                file_path, _, _ = download_file(url, dest_path, dim_file, f"dim_{dim_file}")
                if file_path:
                    downloaded.append(file_path)
            except Exception as e:
                print(f"  Error downloading {dim_file}: {e}")
    
    return downloaded

def download_all(force=False):
    results = {}
    for key in BLS_DATASETS:
        results[key] = download_dataset(key, force=force)
    return results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download BLS datasets")
    parser.add_argument("--dataset", "-d", help="Download specific dataset")
    parser.add_argument("--all", "-a", action="store_true", help="Download all datasets")
    parser.add_argument("--dimensions", action="store_true", help="Download dimension files")
    parser.add_argument("--force", "-f", action="store_true", help="Force re-download")
    args = parser.parse_args()
    
    if args.dataset:
        download_dataset(args.dataset, force=args.force)
        if args.dimensions:
            download_dimension_files(args.dataset, force=args.force)
    elif args.all:
        download_all(force=args.force)
        if args.dimensions:
            for key in BLS_DATASETS:
                download_dimension_files(key, force=args.force)
    else:
        parser.print_help()
