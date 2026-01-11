import duckdb
try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    # Filter for non-empty tags_crop
    print(con.execute("SELECT title, tags_crop FROM farm_info WHERE len(tags_crop) > 0 LIMIT 5").fetchall())
except Exception as e:
    print(e)
