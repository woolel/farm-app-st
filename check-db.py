# check_db.py
import duckdb

con = duckdb.connect('farming_granular.duckdb')

print("=== 1. 전체 데이터 개수 ===")
print(con.execute("SELECT COUNT(*) FROM farming").fetchone()[0])

print("\n=== 2. 월별 데이터 분포 ===")
# 월별로 데이터가 몇 개씩 있는지 확인
print(con.execute("SELECT month, COUNT(*) FROM farming GROUP BY month ORDER BY month").fetchall())

print("\n=== 3. 1월 데이터 샘플 (5개) ===")
# 1월 데이터가 있다면 어떻게 생겼는지 확인
print(con.execute("SELECT year, category, content FROM farming WHERE month=1 LIMIT 5").fetchall())

con.close()