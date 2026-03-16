import os
import sys
import argparse
from datetime import datetime
from .download import download_dataset, download_all, update_load_log, get_load_log_entry
from .gcs_upload import upload_file, upload_directory
from .loader import load_dataset, load_all_qcew
from .config import BLS_DATASETS, TABLE_SCHEMAS
import psycopg2
from .config import DB_CONFIG

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

def ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)

def ensure_load_log_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(TABLE_SCHEMAS["bls_load_log"])
    conn.commit()
    cur.close()
    conn.close()

def log_message(msg, log_file=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    if log_file:
        with open(log_file, "a") as f:
            f.write(line + "\n")

def run_pipeline(datasets=None, skip_download=False, skip_gcs=False, skip_load=False, force_download=False):
    ensure_log_dir()
    ensure_load_log_table()
    log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    log_message("=" * 60, log_file)
    log_message("BLS Data Pipeline Started", log_file)
    log_message("=" * 60, log_file)
    
    if datasets is None:
        datasets = list(BLS_DATASETS.keys())
    
    results = {"downloaded": [], "uploaded": [], "loaded": [], "errors": []}
    
    for dataset_key in datasets:
        log_message(f"\nProcessing: {dataset_key}", log_file)
        
        try:
            download_result = None
            if not skip_download:
                log_message(f"  Downloading {dataset_key}...", log_file)
                download_result = download_dataset(dataset_key, force=force_download)
                if download_result and download_result.get("files"):
                    results["downloaded"].append(dataset_key)
                else:
                    log_message(f"  No files downloaded for {dataset_key}", log_file)
            
            if not skip_gcs:
                log_message(f"  Uploading to GCS...", log_file)
                data_dir = os.path.join(os.path.dirname(__file__), "data")
                
                if dataset_key == "qcew":
                    qcew_dir = os.path.join(data_dir, "qcew")
                    if os.path.exists(qcew_dir):
                        uploaded = upload_directory(qcew_dir, gcs_prefix=f"raw/{dataset_key}")
                        results["uploaded"].extend(uploaded)
                elif download_result and download_result.get("files"):
                    for file_path in download_result["files"]:
                        if os.path.exists(file_path):
                            gcs_uri = upload_file(file_path, gcs_prefix=f"raw/{dataset_key}")
                            results["uploaded"].append(gcs_uri)
            
            if not skip_load:
                skip_load_flag = False
                if download_result and not force_download:
                    data_key = "all"
                    entry = get_load_log_entry(dataset_key, data_key)
                    if entry and entry['status'] == 'success':
                        skip_load_flag = True
                        log_message(f"  Already loaded (ETag unchanged), skipping load", log_file)
                        results["loaded"].append(dataset_key)
                
                if not skip_load_flag:
                    log_message(f"  Loading to PostgreSQL...", log_file)
                    if dataset_key == "qcew":
                        rows = load_all_qcew()
                        if download_result:
                            for i, url in enumerate(download_result.get("urls", [])):
                                year = url.split("/")[-2] if "/" in url else str(i)
                                etag = download_result["etags"][i] if i < len(download_result.get("etags", [])) else None
                                lm = download_result["last_modifieds"][i] if i < len(download_result.get("last_modifieds", [])) else None
                                update_load_log("qcew", year, etag, lm, url, rows, "success")
                    else:
                        rows = load_dataset(dataset_key)
                        if download_result:
                            for i, (file_path, url) in enumerate(zip(download_result["files"], download_result["urls"])):
                                data_key = f"part{i+1}" if len(download_result["files"]) > 1 else "all"
                                etag = download_result["etags"][i] if i < len(download_result.get("etags", [])) else None
                                lm = download_result["last_modifieds"][i] if i < len(download_result.get("last_modifieds", [])) else None
                                update_load_log(dataset_key, data_key, etag, lm, url, rows, "success")
                    
                    if rows:
                        results["loaded"].append(dataset_key)
                        log_message(f"  Loaded {rows:,} rows", log_file)
        
        except Exception as e:
            log_message(f"  ERROR: {e}", log_file)
            results["errors"].append({"dataset": dataset_key, "error": str(e)})
    
    log_message("\n" + "=" * 60, log_file)
    log_message("Pipeline Summary", log_file)
    log_message("=" * 60, log_file)
    log_message(f"Downloaded: {len(results['downloaded'])} datasets", log_file)
    log_message(f"Uploaded: {len(results['uploaded'])} files", log_file)
    log_message(f"Loaded: {len(results['loaded'])} datasets", log_file)
    log_message(f"Errors: {len(results['errors'])}", log_file)
    
    if results["errors"]:
        log_message("\nErrors:", log_file)
        for err in results["errors"]:
            log_message(f"  {err['dataset']}: {err['error']}", log_file)
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BLS Data Pipeline")
    parser.add_argument("--datasets", "-d", nargs="+", help="Specific datasets to process")
    parser.add_argument("--all", "-a", action="store_true", help="Process all datasets")
    parser.add_argument("--skip-download", action="store_true", help="Skip download step")
    parser.add_argument("--skip-gcs", action="store_true", help="Skip GCS upload")
    parser.add_argument("--skip-load", action="store_true", help="Skip PostgreSQL load")
    parser.add_argument("--force-download", "-f", action="store_true", help="Force re-download")
    args = parser.parse_args()
    
    if args.all:
        datasets = list(BLS_DATASETS.keys())
    elif args.datasets:
        datasets = args.datasets
    else:
        parser.print_help()
        sys.exit(1)
    
    run_pipeline(
        datasets=datasets,
        skip_download=args.skip_download,
        skip_gcs=args.skip_gcs,
        skip_load=args.skip_load,
        force_download=args.force_download
    )
