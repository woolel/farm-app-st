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
            print(f"{col[0]} {col[1]}") # Print just name and type

except Exception as e:
    print(f"Error inspecting DB: {e}")
