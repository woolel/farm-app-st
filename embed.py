import duckdb
import os
import json
import pandas as pd
from sentence_transformers import SentenceTransformer
from kiwipiepy import Kiwi

# ==========================================
# 1. 설정 (파일 경로 및 모델)
# ==========================================
INPUT_FILE = 'optimized_farming_data_v2.jsonl'  # 원본 데이터
DB_PATH = 'farming_granular.duckdb'             # 생성될 DB 이름
MODEL_NAME = 'jhgan/ko-sroberta-multitask'      # 한국어 특화 임베딩 모델
VECTOR_DIM = 768                                # 모델의 벡터 차원 수

# ==========================================
# 1.5. Kiwi 형태소 분석기 및 전처리 함수
# ==========================================
kiwi = Kiwi()

def extract_keywords(text):
    """명사(N), 동사/형용사(V), 수칭/수치(SN)만 추출하여 텍스트 정규화"""
    if not text: return ""
    result = kiwi.tokenize(text)
    # N(명사), V(동사/형용사 어근), SN(숫자/수량) 추출
    keywords = [t.form for t in result if t.tag.startswith('N') or t.tag.startswith('V') or t.tag == 'SN']
    return " ".join(keywords) if keywords else text

# ==========================================
# 2. AI 모델 로드
# ==========================================
print(f"🚀 [1/5] AI 모델 로드 중 ({MODEL_NAME})...")
print("   (처음 실행 시 모델 다운로드에 시간이 소요될 수 있습니다. 잠시만 기다려주세요.)")
model = SentenceTransformer(MODEL_NAME)

# ==========================================
# 3. DuckDB 초기화 및 테이블 생성
# ==========================================
print(f"🚀 [2/5] 데이터베이스 초기화 중...")

# 기존 DB 파일이 있다면 삭제 (깨끗한 상태로 시작)
if os.path.exists(DB_PATH):
    try:
        os.remove(DB_PATH)
    except PermissionError:
        print("❌ 오류: DB 파일이 열려있어 삭제할 수 없습니다. DB 연결을 해제해주세요.")
        exit()

con = duckdb.connect(DB_PATH)

# 벡터 검색 확장 기능(VSS) 로드
try:
    con.execute("INSTALL vss; LOAD vss;")
except Exception as e:
    print(f"⚠️ 확장 로드 경고 (이미 설치된 경우 무시): {e}")

# 테이블 스키마 정의 (카테고리별로 쪼개진 구조)
con.execute(f"""
    CREATE TABLE farming (
        pk BIGINT PRIMARY KEY, -- 고유 프라이머리 키 (FTS 필수)
        id TEXT,
        year TEXT,
        month INTEGER,
        category TEXT,     -- '양봉', '기상', '벼' 등 구분
        content TEXT,      -- 실제 내용 (기호, 특수문자 보존됨)
        embedding FLOAT[{VECTOR_DIM}]
    )
""")

# ==========================================
# 4. 데이터 읽기 및 전처리 (Flattening)
# ==========================================
print(f"🚀 [3/5] JSONL 파일 읽기 및 데이터 세분화...")

processed_rows = []
texts_to_embed = []

try:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f):
            if not line.strip(): continue
            
            entry = json.loads(line)
            
            # 메타데이터 추출
            week_id = entry.get('id')
            year = entry.get('year')
            month = entry.get('month')
            
            # 데이터 구조 파악 (평탄화 여부에 따라 처리)
            if 'content' in entry and isinstance(entry['content'], dict):
                target_dict = entry['content']
            else:
                target_dict = entry

            # 각 카테고리별로 데이터 분리 (Granular Split)
            for key, val in target_dict.items():
                # 메타데이터 키는 건너뜀
                if key in ['id', 'year', 'month', 'week_range', 'start_date', 'end_date']:
                    continue
                
                # 유효한 데이터인지 확인 (너무 짧거나 비어있으면 제외)
                if not val or not isinstance(val, str) or len(val.strip()) < 5:
                    continue

                clean_content = val.strip()
                processed_rows.append({
                    "id": week_id,
                    "year": year,
                    "month": month,
                    "category": key,
                    "content": clean_content
                })

except FileNotFoundError:
    print(f"❌ 오류: 입력 파일({INPUT_FILE})을 찾을 수 없습니다.")
    exit()

print(f"   -> 총 {len(processed_rows)}개의 세부 데이터 분석 완료.")

# ==========================================
# 4.5. 키워드 추출 (안정적인 tokenize 방식)
# ==========================================
if processed_rows:
    print(f"🚀 [3.5/5] Kiwi 형태소 분석기 가동 중 (키워드 추출)...")
    
    keyword_texts = []
    total = len(processed_rows)
    
    for i, row in enumerate(processed_rows):
        # N(명사), V(동사/형용사 어근), SN(숫자/수량) 추출
        result = kiwi.tokenize(row['content'])
        keywords = [t.form for t in result if t.tag.startswith('N') or t.tag.startswith('V') or t.tag == 'SN']
        keyword_texts.append(" ".join(keywords) if keywords else row['content'])
        
        # 500개 단위로 진행 상황 표시
        if (i + 1) % 500 == 0 or (i + 1) == total:
            print(f"   -> 키워드 추출 진행 중: {i + 1}/{total} ({(i + 1)/total*100:.1f}%)")

    # 임베딩용 텍스트 최종 생성
    for i, row in enumerate(processed_rows):
        cat = row['category']
        embedding_text = f"{cat}: {keyword_texts[i]}"
        texts_to_embed.append(embedding_text)

# ==========================================
# 5. 임베딩 생성 및 DB 저장 (Pandas 고속 모드)
# ==========================================
if texts_to_embed:
    print(f"🚀 [4/5] 임베딩 생성 및 고속 저장 시작 ({len(texts_to_embed)}건)...")
    
    # 1) 임베딩 생성 (Batch Processing)
    vectors = model.encode(texts_to_embed, batch_size=64, show_progress_bar=True, normalize_embeddings=True)
    
    # 2) Pandas DataFrame 생성 (병목 해결의 핵심)
    df = pd.DataFrame(processed_rows)
    df['embedding'] = list(vectors) # 벡터 컬럼 추가
    
    # 3) DuckDB에 통째로 입력 (SQL Injection 방지 및 속도 최적화)
    # df의 컬럼 순서가 테이블과 다를 수 있으므로 명시적으로 매핑하거나 순서를 맞춤
    # 여기서는 DataFrame 키 순서와 테이블 정의가 거의 같으므로 바로 삽입 시도
    # 안전하게 컬럼 순서 재배열:
    # pk 고유값 할당 및 컬럼 순서 재배열
    df['pk'] = range(len(df))
    df = df[['pk', 'id', 'year', 'month', 'category', 'content', 'embedding']]
    
    # 3) DuckDB에 통째로 입력
    print("   -> DB에 데이터 입력 중 (Bulk Insert)...")
    con.execute("INSERT INTO farming SELECT * FROM df")
    
    # 4) 인덱스 생성
    # [극대화 1] HNSW 파라미터 튜닝 (정밀도 향상)
    # M: 클수록 정밀하지만 메모리 사용량 증가 (기본 16, 추천 32)
    # ef_construction: 인덱스 생성 시 탐색 범위 (기본 100, 추천 200)
    print("🚀 [5/5] 검색 최적화 인덱스 생성 중...")
    try:
        print("   -> 벡터 인덱스(HNSW) 생성 (M=32, ef_c=200)...")
        con.execute("SET hnsw_enable_experimental_persistence = true;")
        con.execute("CREATE INDEX idx_vector ON farming USING HNSW (embedding) WITH (M=32, ef_construction=200);")
    except Exception as e:
        print(f"⚠️ 벡터 인덱스 생성 경고: {e}")

    # [극대화 2] 전문 검색(FTS) 인덱스 추가 (키워드 매칭 보완)
    print("   -> 전문 검색(FTS) 인덱스 구축 중...")
    try:
        con.execute("INSTALL fts; LOAD fts;")
        # pk를 식별자로 사용하여 FTS 인덱스 생성
        con.execute("PRAGMA create_fts_index('farming', 'pk', 'content', 'category');")
        print("   ✅ FTS 인덱스 생성 완료")
    except Exception as e:
        print(f"⚠️ FTS 인덱스 생성 경고: {e}")

else:
    print("⚠️ 처리할 데이터가 없습니다.")

# ==========================================
# 6. 마무리
# ==========================================
con.execute("CHECKPOINT;") # 모든 변경사항을 디스크에 강제 기록
con.close()
print("="*50)
print(f"✅ 모든 작업이 완료되었습니다!")
print(f"📂 생성된 파일: {os.path.abspath(DB_PATH)}")
print("="*50)