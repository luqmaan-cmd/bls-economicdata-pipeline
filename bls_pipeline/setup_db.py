import psycopg2
from .config import DB_CONFIG, TABLE_SCHEMAS

def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    for table_name, schema_sql in TABLE_SCHEMAS.items():
        print(f"Creating table: {table_name}")
        cur.execute(schema_sql)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Created {len(TABLE_SCHEMAS)} tables")

if __name__ == "__main__":
    create_tables()
