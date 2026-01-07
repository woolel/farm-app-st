
import duckdb

con = duckdb.connect('farming_granular.duckdb', read_only=True)

print("=== Full Content of '요약' Category for Month 1 ===")
# Get a few sample years to see variation
rows = con.execute("SELECT year, content FROM farming WHERE month=1 AND category='요약' LIMIT 5").fetchall()
for year, content in rows:
    print(f"--- Year {year} ---")
    print(content[:300]) # First 300 chars should hopefully contain the date
    print("-------------------")
