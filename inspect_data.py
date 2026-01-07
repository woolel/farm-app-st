
import duckdb
import pandas as pd

con = duckdb.connect('farming_granular.duckdb', read_only=True)

# 1. Check distinct categories
print("=== Distinct Categories ===")
cats = con.execute("SELECT DISTINCT category FROM farming ORDER BY category").fetchall()
for c in cats:
    print(c[0])

# 2. Check content for 'Summary' like categories for Month 1 (January)
print("\n=== Sample Content for 'Summary' related categories (Month 1, limit 5) ===")
summary_samples = con.execute("SELECT category, content FROM farming WHERE month=1 AND (category LIKE '%요약%' OR content LIKE '%요약%') LIMIT 5").fetchall()
for cat, content in summary_samples:
    print(f"[{cat}] {content[:100]}...")

# 3. Check for 'Empty' content candidates
print("\n=== Short Content (< 20 chars) Candidates ===")
short_samples = con.execute("SELECT category, content FROM farming WHERE month=1 AND length(content) < 20 LIMIT 10").fetchall()
for cat, content in short_samples:
    print(f"[{cat}] '{content}'")
