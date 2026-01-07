
import duckdb

con = duckdb.connect('farming_granular.duckdb', read_only=True)

print("=== IDs for '요약' Category for Month 1 ===")
rows = con.execute("SELECT id, year, content FROM farming WHERE month=1 AND category='요약' LIMIT 10").fetchall()
for r in rows:
    print(f"ID: {r[0]}, Year: {r[1]}")
