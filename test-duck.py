import duckdb
from sentence_transformers import SentenceTransformer

# ==========================================
# 1. 설정 및 연결
# ==========================================
DB_PATH = 'farming_granular.duckdb'
MODEL_NAME = 'jhgan/ko-sroberta-multitask'

print(f"🔌 데이터베이스({DB_PATH}) 연결 및 모델 로드 중...")
con = duckdb.connect(DB_PATH)
con.execute("INSTALL vss; LOAD vss;") # 벡터 확장 로드

model = SentenceTransformer(MODEL_NAME)

# ==========================================
# 2. 데이터 기본 점검 (쪼개기 확인)
# ==========================================
print("\n📊 [1. 데이터 통계 점검]")
total_count = con.execute("SELECT COUNT(*) FROM farming").fetchone()[0]
print(f"   -> 총 데이터 행(Row) 수: {total_count}개")

print("   -> 카테고리별 분포 (Top 5):")
cat_stats = con.execute("SELECT category, COUNT(*) as cnt FROM farming GROUP BY category ORDER BY cnt DESC LIMIT 5").fetchall()
for cat, cnt in cat_stats:
    print(f"      - {cat}: {cnt}개")

# ==========================================
# 3. 특수문자 보존 확인 (기호 확인)
# ==========================================
print("\n🔣 [2. 특수문자(%, ~) 보존 확인]")
# 내용에 %나 ~가 포함된 데이터 하나만 뽑아보기
sample = con.execute("SELECT category, content FROM farming WHERE content LIKE '%~%' OR content LIKE '%\\%%' LIMIT 1").fetchone()

if sample:
    print(f"   -> 카테고리: {sample[0]}")
    print(f"   -> 내용(일부): {sample[1][:80]}...") 
    print("   ✅ 기호가 정상적으로 보입니다.")
else:
    print("   ⚠️ 기호가 포함된 데이터를 찾지 못했습니다 (데이터 특성일 수 있음).")

# ==========================================
# 4. 하이브리드 검색 성능 테스트 (핵심!)
# ==========================================
print("\n🔎 [3. 하이브리드 검색 테스트 (VSS + FTS)]")
query = "겨울철 꿀벌 관리할 때 주의할 점은?"
print(f"   ❓ 질문: {query}")
print("-" * 60)

# 1) 질문 임베딩
query_vector = model.encode(query).tolist()

# 2) 하이브리드 검색 실행
sql = f"""
SELECT final_score, score, fts_score, category, year, month, content
FROM (
    SELECT 
        (0.7 * score + 0.3 * fts_score) as final_score,
        score, fts_score, category, year, month, content
    FROM (
        SELECT 
            array_cosine_similarity(embedding, ?::FLOAT[768]) AS score,
            fts_main_farming.match_bm25(pk, ?) AS fts_score,
            *
        FROM farming
    )
) 
WHERE final_score IS NOT NULL
ORDER BY final_score DESC 
LIMIT 3;
"""

results = con.execute(sql, [query_vector, query]).fetchall()

# 3) 결과 출력
for i, row in enumerate(results):
    f_score, v_score, fts_score, category, year, mon, content = row
    
    date_info = f"{year}년 {mon}월"
    clean_content = content.replace('\n', ' ').replace('\r', '')
    
    print(f"{i+1}위. [{category}] {date_info} (합산: {f_score:.4f} | 벡터: {v_score:.4f} | FTS: {fts_score:.4f})")
    print(f"   내용: {clean_content[:120]}...")
    print("-" * 60)

con.close()