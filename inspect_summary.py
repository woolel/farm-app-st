import duckdb
con = duckdb.connect('farming_granular.duckdb', read_only=True)
res = con.execute("SELECT id, category, content FROM farming WHERE category = '요약' LIMIT 5").fetchall()
if not res:
    res = con.execute("SELECT id, category, content FROM farming WHERE category LIKE '%요약%' LIMIT 5").fetchall()
for rid, cat, cont in res:
    print(f"ID: {rid} | CAT: {cat}")
    print("--- CONTENT ---")
    print(cont)
    print("----------------\n")
con.close()
