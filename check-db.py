import duckdb
import os

db_path = 'farming_granular.duckdb'
if not os.path.exists(db_path):
    print(f"File not found: {db_path}")
    exit(1)

try:
    # read_only=True to avoid locking issues
    con = duckdb.connect(db_path, read_only=True)
    
    print("--- Table Schema ---")
    print(con.execute("DESCRIBE farming").fetchall())
    
    print("\n--- Indexes ---")
    try:
        # Check for FTS and VSS indexes
        indices = con.execute("SELECT index_name, table_name FROM duckdb_indexes").fetchall()
        for idx in indices:
            print(idx)
    except Exception as e:
        print(f"Error checking indexes: {e}")
        
    print("\n--- Data Count ---")
    print(con.execute("SELECT count(*) FROM farming").fetchall())
    
    con.close()
except Exception as e:
    print(f"Error connecting to DB: {e}")