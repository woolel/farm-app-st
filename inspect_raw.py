import duckdb

con = duckdb.connect('farming_granular.duckdb', read_only=True)
res = con.execute("SELECT id, content FROM farming WHERE category='요약' AND content LIKE '%|%' LIMIT 3").fetchall()
for id, content in res:
    print(f"===ID:{id}===")
    print(content.__repr__())
    print("\n----------------\n")
con.close()
