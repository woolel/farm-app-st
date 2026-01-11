import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    print(con.execute("SELECT title, tags_env FROM farm_info WHERE len(tags_env) > 0 LIMIT 5").fetchall())
except Exception as e:
    print(e)
