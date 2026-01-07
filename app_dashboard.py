import streamlit as st
import duckdb
from sentence_transformers import SentenceTransformer
from datetime import datetime
import os
import re

# ==========================================
# 1. 페이지 설정 및 디자인
# ==========================================
st.set_page_config(
    page_title="스마트 농업 대시보드", 
    page_icon="🚜", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 스타일 커스텀
st.markdown("""
    <style>
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; }
    
    /* 표 스타일 강제 적용 */
    table {
        width: 100% !important;
        border-collapse: collapse !important;
    }
    th, td {
        padding: 8px !important;
        border: 1px solid #ddd !important;
        text-align: left !important;
    }
    th {
        background-color: #f2f2f2 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'시스템 로딩 중... ({model_path})'):
        model = SentenceTransformer(model_path)
    
    if not os.path.exists('farming_granular.duckdb'):
        return None, None, "file_not_found"
        
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    fts_status = "ok"

    try:
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("INSTALL fts; LOAD fts;")
        
        # FTS 인덱스 존재 여부 확인 (진단용 - 독립된 try-except로 감쌈)
        try:
            # FTS 인덱스는 별도의 스키마(fts_main_farming)로 생성되므로 스키마 목록을 확인
            schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
            fts_exists = any('fts_main_farming' in str(row) for row in schemas)
            if not fts_exists:
                fts_status = "fts_missing"
        except Exception:
            pass # 진단 쿼리 자체가 실패할 경우 앱 실행을 방해하지 않음
            
    except Exception as e:
        st.warning(f"DuckDB 확장 로드 실패 (검색 기능이 제한될 수 있음): {e}")
        
    return model, con, fts_status

# [극대화 3] 데이터 조회 유틸리티 (캐싱 적용)
@st.cache_data(ttl=3600)
def get_monthly_trends(month, _con):
    """
    현재 월의 주요 키워드 트렌드 분석 (SQL 집계)
    """
    sql = """
        SELECT category, count(*) as cnt
        FROM farming
        WHERE month = ?
        GROUP BY category
        ORDER BY cnt DESC
    """
    return _con.execute(sql, [month]).fetchall()

model, con, db_status = load_resources()

if db_status == "file_not_found":
    st.error("❌ 'farming_granular.duckdb' 파일이 없습니다.")
    st.stop()

if db_status == "fts_missing":
    st.error("⚠️ 데이터베이스에 FTS 인덱스가 감지되지 않습니다. (최신 DB가 적용되지 않았을 수 있습니다)")
    if st.button("🔄 데이터베이스 연결 새로고침 (캐시 삭제)"):
        st.cache_resource.clear()
        st.rerun()

if con is None:
    st.error("❌ 데이터베이스 연결에 실패했습니다.")
    st.stop()

# ==========================================
# 3. 유틸리티 함수
# ==========================================
def format_content(text):
    r"""
    1. 취소선 방지: ~ -> \~
    2. 표 깨짐 방지: 표 앞뒤에 줄바꿈 추가
    """
    if not text: return ""
    
    # 취소선 방지
    text = text.replace('~', r'\~')
    
    # 마크다운 표가 붙어서 깨지는 것을 방지하기 위해 줄바꿈 보강
    text = text.replace('|', ' | ') # 파이프 간격 확보
    return f"\n{text}\n"

# ==========================================
# 4. 사이드바
# ==========================================
today = datetime.now()
current_month = today.month

with st.sidebar:
    st.header("🔍 검색 도우미")
    st.info(f"오늘은 {today.year}년 {today.month}월 {today.day}일 입니다.")
    
    st.markdown("### 📂 분야 선택")
    selected_cats = st.multiselect(
        "관심 분야:",
        ['기상', '양봉', '벼', '밭작물', '채소', '과수', '특용작물', '축산'],
        default=['양봉', '기상']
    )
    
    st.markdown(f"### 💡 {current_month}월 추천 키워드")
    if current_month in [12, 1, 2]:
        tags = ["월동 관리", "한파 대비", "전정", "화재 예방"]
    elif current_month in [3, 4, 5]:
        tags = ["파종 준비", "못자리", "봄벌 깨우기", "냉해 예방"]
    elif current_month in [6, 7, 8]:
        tags = ["장마 대비", "탄저병", "응애 방제", "배수로"]
    else: 
        tags = ["수확 시기", "건조 관리", "가을 걷이", "월동 준비"]

    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    for tag in tags:
        if st.button(f"#{tag}", use_container_width=True):
            st.session_state.search_query = tag

# ==========================================
# 5. 메인 화면: 오늘의 농사 브리핑
# ==========================================
st.title(f"📅 {current_month}월 {today.day}일, 농사 브리핑")

# [극대화 4] 이달의 트렌드 분석 (SQL 집계 활용)
with st.sidebar:
    st.divider()
    st.markdown(f"### 📈 {current_month}월 데이터 트렌드")
    trends = get_monthly_trends(current_month, con)
    if trends:
        for cat, count in trends[:5]:
            st.caption(f"**{cat}**: {count}건의 정보")
    st.divider()

with st.container():
    st.markdown("### 🌤️ 지난 3년, 오늘 이맘때 핵심 정보")
    
    # [SQL 수정] id 추가하여 주간 범위 파악
    history_sql = f"""
        SELECT id, year, category, content 
        FROM farming 
        WHERE month = ? 
        -- 기본적인 노이즈만 제거 (목차 점선, 명시적 목차 단어)
        AND content NOT LIKE '%····%'
        AND content NOT LIKE '%목 차%'
        AND category NOT IN ('목차')
        ORDER BY year DESC
        LIMIT 150 
    """
    history_data = con.execute(history_sql, [current_month]).fetchall()
    
    if history_data:
        history_by_year = {}
        
        # [우선순위 로직] 날짜 매칭되는 요약 > 기상/농업 > 나머지
        # '이번주'의 기준을 조금 넓게(+- 7일) 잡아서 가장 가까운 요약표를 찾음
        def get_priority(item, current_date):
            _id, _year, _cat, _content = item
            
            # 1. 날짜 매칭 요약정보 (최우선)
            if '요약' in _cat:
                try:
                    # ID 포맷: YYYY-MM-DD_YYYY-MM-DD
                    dates = _id.split('_')
                    if len(dates) == 2:
                        start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
                        end_dt = datetime.strptime(dates[1], "%Y-%m-%d")
                        
                        target_year = int(_year)
                        check_date = datetime(target_year, current_date.month, current_date.day)
                        
                        # [유연한 매칭] 해당 주간이거나, 주간 시작/끝에서 7일 이내면 허용
                        # 이렇게 하면 1월 7일인데 1월 8일 시작 데이터도 매칭됨
                        diff_start = abs((check_date - start_dt).days)
                        diff_end = abs((check_date - end_dt).days)
                        
                        if start_dt <= check_date <= end_dt:
                            return 0 # 정확히 포함
                        elif diff_start <= 7 or diff_end <= 7:
                            return 0 # 근처 1주일 이내면 허용
                except Exception:
                    pass
                return 100 # 날짜 안 맞는 요약은 아예 뒤로 보내거나 숨김
                
            if '기상' in _cat or '농업' in _cat: return 2
            return 99

        for row in history_data:
            row_id, year, cat, content = row
            
            if year not in history_by_year:
                history_by_year[year] = []
            
            # [필터링 1] 목차/차례 명시적 제거 (강력 필터)
            if '목 차' in content or '목차' in content:
                continue

            # [필터링 2] 내용 없는 껍데기 제거
            clean_text = content.replace('\n', '').replace('|', '').replace('-', '').strip()
            
            # "### 제 7장 제7장 특용작물" 같은 반복 헤더 제거
            # 헤더(###)만 있고 내용 길이가 짧거나, 의미있는 문장이 없는 경우
            is_header_only = False
            if '###' in content:
                 # 제x장 패턴이 있고 길이가 50자 미만이면 제낌
                 if ('제' in content and '장' in content) and len(clean_text) < 60:
                     is_header_only = True
                 # '기상' 같은 단일 헤더도 짧으면 제낌
                 elif len(clean_text) < 30:
                     is_header_only = True
            
            if is_header_only: continue

            # 중복 제거 (내용 기준)
            if any(item[2] == content for item in history_by_year[year]):
                continue
                
            priority = get_priority(row, today)
            
            # [특수 필터] 날짜 안 맞는 요약은 브리핑에서 아예 제외 (혼란 방지)
            if '요약' in cat and priority > 0:
                continue
            
            # 우선순위와 함께 저장 (Priority, Category, Content)
            history_by_year[year].append((priority, cat, content))

        # 연도별 출력
        available_years = sorted(history_by_year.keys(), reverse=True)
        
        if not available_years:
             st.warning("표시할 유효한 데이터가 없습니다.")
        else:
            for i, year in enumerate(available_years):
                if i >= 3: break 
                
                # 우선순위 정렬 (Prio 0 -> ... -> 99)
                items = sorted(history_by_year[year], key=lambda x: x[0])
                
                # 데이터가 없으면 스킵
                if not items: continue

                st.markdown(f"#### 📆 {year}년 {current_month}월")
                
                # 상위 5개 표출
                final_items = items[:5]
                
                for _, category, full_content in final_items:
                    safe_content = format_content(full_content)
                    
                    # 미리보기 텍스트
                    clean_one_line = full_content.replace('\n', ' ').replace('|', ' ').strip()
                    preview_text = clean_one_line[:40] + "..." if len(clean_one_line) > 40 else clean_one_line
                    
                    # 아이콘 설정
                    if '기상' in category or '농업' in category: icon = "☁️"
                    elif '요약' in category: icon = "📝"
                    else: icon = "📌"

                    with st.expander(f"{icon} **[{category}]** {preview_text}", expanded=False):
                        st.markdown(safe_content, unsafe_allow_html=True)
                
                st.markdown("---") 

    else:
        st.info("이맘때의 과거 데이터가 충분하지 않습니다.")

# ==========================================
# 6. 시맨틱 검색 엔진
# ==========================================
st.subheader("🔎 농사 지식 백과 검색")

query = st.text_input(
    "궁금한 내용을 입력하세요:", 
    value=st.session_state.search_query,
    placeholder="예: 겨울철 꿀벌 관리, 고추 탄저병 예방...",
    key="main_search"
)

@st.cache_data(ttl=600) # 검색 결과 10분간 캐싱
def search_farming(query, category_filter, _model, _con):
    # 1. 질문 벡터화
    query_vector = _model.encode(query).tolist()
    
    # 2. 하이브리드 검색 SQL (VSS + FTS)
    # fts_main_farming.match_bm25를 사용하여 키워드 점수 합산
    # 시맨틱 유사도(score)와 키워드 점수를 결합
    sql = f"""
    SELECT 
        (0.7 * score + 0.3 * fts_score) as final_score,
        category, year, month, content
    FROM (
        SELECT 
            array_cosine_similarity(embedding, ?::FLOAT[768]) AS score,
            fts_main_farming.match_bm25(pk, ?) AS fts_score,
            *
        FROM farming
    ) 
    WHERE (score > 0.5 OR fts_score > 2.0)
    {category_filter}
    AND content NOT LIKE '%····%'
    AND content NOT LIKE '%목 차%'
    AND category NOT IN ('목차')
    ORDER BY final_score DESC 
    LIMIT 5;
    """
    return _con.execute(sql, [query_vector, query]).fetchall()

if query:
    category_filter = ""
    if selected_cats:
        cats_str = "', '".join(selected_cats)
        category_filter = f"AND category IN ('{cats_str}')"

    with st.spinner(f"AI와 엔진이 '{query}' 관련 최적의 정보를 찾는 중..."):
        results = search_farming(query, category_filter, model, con)

    if not results:
        st.warning("조건에 맞는 정보를 찾지 못했습니다.")
    else:
        for row in results:
            score, cat, year, mon, content = row
            score_badge = "🟢 높음" if score > 0.6 else "🟡 보통"
            safe_content = format_content(content)
            
            with st.container():
                st.markdown(f"#### [{cat}] {year}년 {mon}월 정보 <small>({score_badge})</small>", unsafe_allow_html=True)
                
                # 검색어 하이라이팅 (마크다운 충돌 방지 위해 단순화)
                st.markdown(f"💡 **관련 검색어:** {query}")
                
                # [핵심 수정] st.info 대신 st.markdown 사용
                st.markdown(safe_content, unsafe_allow_html=True)
                st.caption("---")

# ==========================================
# 7. 푸터
# ==========================================
st.markdown("<br><div style='text-align: center; color: gray;'>데이터 출처: 농촌진흥청 주간농사정보 | Created with Streamlit & DuckDB</div>", unsafe_allow_html=True)