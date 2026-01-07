import duckdb

con = duckdb.connect('farming_granular.duckdb', read_only=True)
con.execute("INSTALL fts; LOAD fts;")

query = "꿀벌"
try:
    sql = f"""
    SELECT 
        fts_main_farming.match_bm25(pk, ?) AS fts_score,
        category, content
    FROM farming
    WHERE fts_score > 0
    LIMIT 5;
    """
    res = con.execute(sql, [query]).fetchall()
    print(f"Results for '{query}':")
    for row in res:
        print(row)
except Exception as e:
    print(f"Error: {e}")

con.close()
