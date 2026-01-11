import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    print(con.execute("SELECT id, year, month, title, tags_crop, content_md FROM farm_info LIMIT 5").fetchall())
except Exception as e:
    print(e)
