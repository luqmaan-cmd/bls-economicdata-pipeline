import os
import csv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Optional, List
from multiprocessing import Pool, Manager
from .config import DB_CONFIG, BLS_DATASETS, TABLE_SCHEMAS

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

BATCH_SIZE = 50000
NUM_WORKERS = 4

COLUMN_MAPPINGS = {
    "bls_cpi": [
        "series_id", "year", "period", "value", "seasonal", "base_period",
        "series_title", "begin_year", "begin_period", "end_year", "end_period",
        "aspect_type", "aspect_value", "area_name", "base_name", "item_name",
        "period_abbr", "period_name", "periodicity_name", "date"
    ],
    "bls_employment": [
        "series_id", "year", "period", "value", "footnote_codes"
    ],
    "bls_qcew": [
        "area_fips", "industry_code", "own_code", "agglvl_code", "size_code",
        "year", "qtr", "disclosure_code", "qtrly_estabs", "month1_emplvl",
        "month2_emplvl", "month3_emplvl", "total_qtrly_wages", "taxable_qtrly_wages",
        "qtrly_contributions", "avg_wkly_wage", "lq_disclosure_code", "lq_qtrly_estabs",
        "lq_month1_emplvl", "lq_month2_emplvl", "lq_month3_emplvl", "lq_total_qtrly_wages",
        "lq_taxable_qtrly_wages", "lq_qtrly_contributions", "lq_avg_wkly_wage",
        "oty_disclosure_code", "oty_qtrly_estabs_chg", "oty_qtrly_estabs_pct_chg",
        "oty_month1_emplvl_chg", "oty_month1_emplvl_pct_chg", "oty_month2_emplvl_chg",
        "oty_month2_emplvl_pct_chg", "oty_month3_emplvl_chg", "oty_month3_emplvl_pct_chg",
        "oty_total_qtrly_wages_chg", "oty_total_qtrly_wages_pct_chg", "oty_taxable_qtrly_wages_chg",
        "oty_taxable_qtrly_wages_pct_chg", "oty_qtrly_contributions_chg", "oty_qtrly_contributions_pct_chg",
        "oty_avg_wkly_wage_chg", "oty_avg_wkly_wage_pct_chg", "date", "industry_title",
        "ind_level", "naics_2d", "sector", "vintage_start", "vintage_end",
        "area_title", "area_type", "stfips", "specified_region"
    ],
    "bls_unemployment": [
        "series_id", "year", "period", "value", "footnote_codes"
    ]
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def ensure_table_exists(table_name):
    if table_name not in TABLE_SCHEMAS:
        print(f"  Warning: No schema defined for {table_name}")
        return
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(TABLE_SCHEMAS[table_name])
    conn.commit()
    cur.close()
    conn.close()

def truncate_table(table_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    conn.commit()
    cur.close()
    conn.close()
    print(f"Truncated table: {table_name}")

def parse_value(value, col_name=None):
    if value is None or value == "" or value == "NA" or value == "NaN":
        return None
    
    if col_name and "date" in col_name.lower():
        try:
            if "-" in str(value):
                return str(value)
            return None
        except:
            return None
    
    try:
        if "." in str(value):
            return float(value)
        return int(value)
    except (ValueError, TypeError):
        return str(value).strip('"')

def insert_batch(args):
    batch, table_name, col_str = args
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    execute_values(cur, f"INSERT INTO {table_name} ({col_str}) VALUES %s", batch)
    conn.commit()
    cur.close()
    conn.close()
    return len(batch)

def detect_delimiter(csv_path):
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        first_line = f.readline()
        if '\t' in first_line:
            return '\t'
        return ','

def load_csv_to_table(csv_path, table_name, truncate=True):
    print(f"\nLoading {csv_path} -> {table_name}")
    
    ensure_table_exists(table_name)
    
    if truncate:
        truncate_table(table_name)
    
    delimiter = detect_delimiter(csv_path)
    
    columns = COLUMN_MAPPINGS.get(table_name)
    if not columns:
        print(f"  No column mapping for {table_name}, reading from CSV header")
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            columns = [c.strip().strip('"').lower() for c in next(reader)]
    
    col_str = ", ".join(columns)
    
    batches = []
    batch = []
    
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        
        for row in reader:
            values = []
            for col in columns:
                col_lower = col.lower()
                val = None
                for key in row.keys():
                    if key and key.lower().strip('"') == col_lower:
                        val = row[key]
                        break
                values.append(parse_value(val, col))
            
            batch.append(tuple(values))
            
            if len(batch) >= BATCH_SIZE:
                batches.append((batch, table_name, col_str))
                batch = []
    
    if batch:
        batches.append((batch, table_name, col_str))
    
    print(f"  Inserting {len(batches)} batches with {NUM_WORKERS} workers...")
    
    total_rows = 0
    with Pool(NUM_WORKERS) as pool:
        results = pool.map(insert_batch, batches)
        total_rows = sum(results)
    
    print(f"  Complete: {total_rows:,} rows loaded into {table_name}")
    return total_rows

def load_dataset(dataset_key, csv_paths: Optional[List[str]] = None, truncate=True):
    if dataset_key not in BLS_DATASETS:
        print(f"Unknown dataset: {dataset_key}")
        return 0
    
    dataset = BLS_DATASETS[dataset_key]
    table_name = dataset["table"]
    
    if csv_paths is None:
        file_prefix = dataset['file_prefix']
        single_path = os.path.join(DATA_DIR, f"{file_prefix}_data.csv")
        multi_paths = [
            os.path.join(DATA_DIR, f"{file_prefix}_data_{i+1}.csv")
            for i in range(10)
            if os.path.exists(os.path.join(DATA_DIR, f"{file_prefix}_data_{i+1}.csv"))
        ]
        
        if multi_paths:
            csv_paths = multi_paths
        elif os.path.exists(single_path):
            csv_paths = [single_path]
        else:
            print(f"CSV not found for {dataset_key}")
            return 0
    
    total_rows = 0
    for i, csv_path in enumerate(csv_paths):
        if not os.path.exists(csv_path):
            print(f"CSV not found: {csv_path}")
            continue
        
        do_truncate = truncate and (i == 0)
        rows = load_csv_to_table(csv_path, table_name, truncate=do_truncate)
        total_rows += rows
    
    return total_rows

def load_all_qcew():
    qcew_dir = os.path.join(DATA_DIR, "qcew")
    total = 0
    
    for year in range(1990, 2025):
        csv_pattern = os.path.join(qcew_dir, f"{year}.q1-q4.singlefile.csv")
        alt_pattern = os.path.join(qcew_dir, f"{year}_qtrly_singlefile.csv")
        
        csv_path = None
        for pattern in [csv_pattern, alt_pattern]:
            if os.path.exists(pattern):
                csv_path = pattern
                break
        
        if csv_path:
            rows = load_csv_to_table(csv_path, "bls_qcew", truncate=(year == 1990))
            total += rows
    
    print(f"\nTotal QCEW rows loaded: {total:,}")
    return total

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load BLS data to PostgreSQL")
    parser.add_argument("--dataset", "-d", help="Load specific dataset")
    parser.add_argument("--csv", "-c", help="CSV file path")
    parser.add_argument("--table", "-t", help="Target table name")
    parser.add_argument("--all", "-a", action="store_true", help="Load all datasets")
    parser.add_argument("--qcew", "-q", action="store_true", help="Load all QCEW years")
    args = parser.parse_args()
    
    if args.csv and args.table:
        load_csv_to_table(args.csv, args.table)
    elif args.dataset:
        load_dataset(args.dataset, args.csv)
    elif args.qcew:
        load_all_qcew()
    elif args.all:
        for key in BLS_DATASETS:
            if key == "qcew":
                load_all_qcew()
            else:
                load_dataset(key)
    else:
        parser.print_help()
