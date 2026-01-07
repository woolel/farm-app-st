import duckdb
import os

db_path = 'farming_granular.duckdb'
con = duckdb.connect(db_path, read_only=True)
con.execute("INSTALL fts; LOAD fts;")

print("Checking for FTS tables...")
tables = con.execute("SELECT table_name FROM duckdb_tables").fetchall()
fts_tables = [t[0] for t in tables if 'fts_main_farming' in t[0]]
print(f"FTS Tables found: {fts_tables}")

print("\nChecking for match_bm25 function...")
funcs = con.execute("SELECT function_name FROM duckdb_functions").fetchall()
bm25_funcs = [f[0] for f in funcs if 'match_bm25' in f[0]]
print(f"BM25 Functions found: {bm25_funcs}")

print("\nChecking duckdb_indexes...")
indexes = con.execute("SELECT * FROM duckdb_indexes").fetchall()
for idx in indexes:
    print(idx)

con.close()
