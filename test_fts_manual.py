import duckdb
import os

DB_PATH = 'farming_granular.duckdb'

if not os.path.exists(DB_PATH):
    print(f"File not found: {DB_PATH}")
    exit(1)

con = duckdb.connect(DB_PATH, read_only=True)

print("Attempting to create FTS index...")
try:
    con.execute("INSTALL fts; LOAD fts;")
    # pk를 식별자로 사용하여 FTS 인덱스 생성
    # content와 category 컬럼에 대해 인덱스 생산
    con.execute("PRAGMA create_fts_index('farming', 'pk', 'content', 'category');")
    print("✅ FTS 인덱스 생성 완료")
except Exception as e:
    print(f"❌ FTS 인덱스 생성 실패: {e}")

# Check schemas
schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
print(f"Schemas: {schemas}")
fts_exists = any('fts_main_farming' in str(row) for row in schemas)
print(f"FTS Schema exists: {fts_exists}")

con.close()
