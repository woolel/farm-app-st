import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    con.execute("INSTALL fts; LOAD fts;")
    print(con.execute("SELECT index_name FROM duckdb_indexes").fetchall())
except Exception as e:
    print(e)
