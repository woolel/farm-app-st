import duckdb
try:
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE farm_info AS SELECT * FROM 'weekly_farming.parquet'")
    
    print("Schema:")
    schema = con.execute("DESCRIBE farm_info").fetchall()
    for col in schema:
        print(f"{col[0]}: {col[1]}")
        
    print("\nRow Count:")
    count = con.execute("SELECT COUNT(*) FROM farm_info").fetchall()[0][0]
    print(count)
    
except Exception as e:
    print(f"Error: {e}")
