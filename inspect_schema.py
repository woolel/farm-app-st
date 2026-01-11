import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    print(con.execute("DESCRIBE farm_info").fetchall())
except Exception as e:
    print(e)
