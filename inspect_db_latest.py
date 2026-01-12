import duckdb

try:
    con = duckdb.connect('farming_granular.duckdb')
    print("Tables:")
    tables = con.execute("SHOW TABLES").fetchall()
    print(tables)
    
    for table_name in tables:
        t_name = table_name[0]
        print(f"\nSchema for {t_name}:")
        schema = con.execute(f"DESCRIBE {t_name}").fetchall()
        for col in schema:
            print(col)
            
    print("\nSample Data from farm_info (first 1 row):")
    try:
        sample = con.execute("SELECT * FROM farm_info LIMIT 1").fetchall()
        print(sample)
    except Exception as e:
        print(f"Could not read farm_info: {e}")

except Exception as e:
    print(f"Error inspecting DB: {e}")
