import duckdb
import os

db_name = 'test_fts_v2.duckdb'
if os.path.exists(db_name):
    os.remove(db_name)

con = duckdb.connect(db_name)
con.execute("INSTALL fts; LOAD fts;")
con.execute("CREATE TABLE items (pk BIGINT PRIMARY KEY, content TEXT)")
con.execute("INSERT INTO items VALUES (1, 'hello world'), (2, 'farming is fun')")
con.execute("PRAGMA create_fts_index('items', 'pk', 'content')")
con.close()

print("--- Checking again with prefix ---")
con = duckdb.connect(db_name)
con.execute("INSTALL fts; LOAD fts;")
try:
    # Try the prefix version
    res = con.execute("SELECT fts_main_items.match_bm25(pk, 'hello') as score FROM items").fetchall()
    print(f"Prefix test: {res}")
except Exception as e:
    print(f"Prefix test failed: {e}")

try:
    # Try searching for the macro in different ways
    res = con.execute("SELECT * FROM duckdb_functions() WHERE function_name LIKE '%match%'").fetchall()
    print(f"Functions found: {res}")
except Exception as e:
    print(f"Function check failed: {e}")

con.close()
