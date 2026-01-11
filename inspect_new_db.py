import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    print("Tables:", con.execute("SHOW TABLES").fetchall())
    print("\nDescribe farm_info:")
    print(con.execute("DESCRIBE farm_info").fetchall())
    print("\nSample Data:")
    print(con.execute("SELECT * FROM farm_info LIMIT 1").fetchall())
except Exception as e:
    print(e)
