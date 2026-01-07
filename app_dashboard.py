import streamlit as st
import duckdb
from sentence_transformers import SentenceTransformer
from datetime import datetime
import os

# ==========================================
# 1. 페이지 설정 및 디자인
# ==========================================
st.set_page_config(
    page_title="스마트 농업 대시보드", 
    page_icon="🚜", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# 스타일 커스텀 (가독성 향상)
st.markdown("""
    <style>
    .big-font { font-size:18px !important; }
    .stAlert { padding-top: 10px; padding-bottom: 10px; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
# @st.cache_resource는 AI 모델과 DB 연결을 메모리에 저장해두고
# 새로고침할 때마다 다시 로드하지 않게 하여 속도를 획기적으로 높입니다.
@st.cache_resource
def load_resources():
    # 1. AI 모델 로드 (HuggingFace에서 자동 다운로드)
    # 클라우드 서버에는 로컬 모델이 없으므로 모델명을 직접 입력합니다.
    model_name = 'jhgan/ko-sroberta-multitask'
    model = SentenceTransformer(model_name)
    
    # 2. DuckDB 연결
    # read_only=True: 여러 사람이 동시에 접속해도 파일이 깨지지 않게 함
    if not os.path.exists('farming_granular.duckdb'):
        return None, None
        
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    
    # 벡터 검색 확장 기능 설치 및 로드
    try:
        con.execute("INSTALL vss; LOAD vss;")
    except Exception as e:
        # 이미 설치되어 있을 경우 무시
        pass
        
    return model, con

# 리소스 불러오기
with st.spinner('시스템을 가동 중입니다... (약 30초 소요)'):
    model, con = load_resources()

# DB 파일이 없을 경우 예외 처리
if con is None:
    st.error("❌ 데이터베이스 파일(farming_granular.duckdb)을 찾을 수 없습니다. GitHub에 파일을 올렸는지 확인해주세요.")
    st.stop()

# ==========================================
# 3. 사이드바: 검색 필터 및 추천 키워드
# ==========================================
today = datetime.now()
current_month = today.month

with st.sidebar:
    st.header("🔍 검색 도우미")
    
    # 1) 카테고리 필터
    st.markdown("### 📂 분야 선택")
    selected_cats = st.multiselect(
        "관심 분야만 골라보세요:",
        ['기상', '양봉', '벼', '밭작물', '채소', '과수', '특용작물', '축산'],
        default=['양봉', '기상'] # 기본 선택값
    )
    
    # 2) 월별 추천 키워드 (시즌성)
    st.markdown(f"### 💡 {current_month}월 추천 키워드")
    
    # 계절별 키워드 자동 변경 로직
    if current_month in [12, 1, 2]:
        tags = ["월동 관리", "한파 대비", "전정(가지치기)", "화재 예방", "시설 하우스"]
    elif current_month in [3, 4, 5]:
        tags = ["파종 준비", "못자리", "봄벌 깨우기", "냉해 예방", "꽃가루 매개"]
    elif current_month in [6, 7, 8]:
        tags = ["장마 대비", "탄저병 방제", "혹서기 가축관리", "응애 방제", "배수로 정비"]
    else: # 9, 10, 11
        tags = ["수확 시기", "건조 관리", "가을 걷이", "월동 준비", "김장 채소"]

    # 태그 버튼 생성 (세션 상태를 이용해 검색어 주입)
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    # 버튼을 누르면 검색창에 텍스트가 입력되게 함
    for tag in tags:
        if st.button(f"#{tag}", use_container_width=True):
            st.session_state.search_query = tag

# ==========================================
# 4. 메인 화면: 오늘의 브리핑 (자동 대시보드)
# ==========================================
st.title(f"📅 {today.month}월 {today.day}일, 농사 브리핑")

# '지난 3년의 오늘' 데이터를 자동으로 보여줌
with st.expander("🌤️ 지난 3년, 오늘 이맘때 기상과 핵심 정보 (자동 분석)", expanded=True):
    # SQL: 현재 월(Month)과 일치하고, '기상'이나 '요약' 카테고리만 조회
    history_sql = f"""
        SELECT year, category, content 
        FROM farming 
        WHERE month = ? 
        AND category IN ('기상', '요약')
        ORDER BY year DESC, category ASC
        LIMIT 10
    """
    history_data = con.execute(history_sql, [current_month]).fetchall()
    
    if history_data:
        # 데이터를 연도별로 그룹화
        history_by_year = {}
        for year, cat, content in history_data:
            if year not in history_by_year:
                history_by_year[year] = []
            # 내용이 너무 길면 자르기
            summary = content[:120] + "..." if len(content) > 120 else content
            history_by_year[year].append(f"**[{cat}]** {summary}")

        # 3단 컬럼 레이아웃
        cols = st.columns(3)
        years_list = sorted(history_by_year.keys(), reverse=True)[:3]

        for i, year in enumerate(years_list):
            with cols[i]:
                st.info(f"📆 {year}년 {current_month}월")
                for item in history_by_year[year][:2]: # 공간상 2개만 표시
                    st.markdown(item)
    else:
        st.info("이맘때의 과거 데이터가 충분하지 않습니다.")

# ==========================================
# 5. 시맨틱 검색 엔진
# ==========================================
st.divider()
st.subheader("🔎 농사 지식 백과 검색")

# 검색창
query = st.text_input(
    "궁금한 내용을 입력하세요 (또는 왼쪽 추천 키워드 클릭):", 
    value=st.session_state.search_query,
    placeholder="예: 겨울철 꿀벌 관리, 고추 탄저병 예방...",
    key="main_search"
)

# 검색 실행 로직
if query:
    # 1. 카테고리 필터링 SQL 생성
    category_filter = ""
    if selected_cats:
        cats_str = "', '".join(selected_cats)
        category_filter = f"AND category IN ('{cats_str}')"

    with st.spinner(f"'{query}' 관련 문서를 AI가 분석 중입니다..."):
        # 2. 질문 벡터화
        query_vector = model.encode(query).tolist()
        
        # 3. 벡터 유사도 검색 SQL
        sql = f"""
        SELECT score, category, year, month, content
        FROM (
            SELECT array_cosine_similarity(embedding, ?::FLOAT[768]) AS score, *
            FROM farming
        ) 
        WHERE score IS NOT NULL
        {category_filter} 
        ORDER BY score DESC 
        LIMIT 5;
        """
        
        results = con.execute(sql, [query_vector]).fetchall()

    # 4. 결과 출력
    if not results:
        st.warning("조건에 맞는 정보를 찾지 못했습니다. 카테고리 필터를 넓혀보세요.")
    else:
        for row in results:
            score, cat, year, mon, content = row
            
            # 유사도에 따른 색상 힌트
            score_badge = "🟢 높음" if score > 0.6 else "🟡 보통"
            
            with st.container():
                st.markdown(f"#### [{cat}] {year}년 {mon}월 정보 <small>({score_badge} / 유사도 {score:.2f})</small>", unsafe_allow_html=True)
                
                # 검색어 하이라이팅 (단순 텍스트 매칭)
                highlighted_content = content.replace(query, f":red[**{query}**]")
                st.info(highlighted_content)
                st.caption("---")

# ==========================================
# 6. 푸터
# ==========================================
st.markdown("---")
st.markdown("<div style='text-align: center; color: gray;'>데이터 출처: 농촌진흥청 주간농사정보 | Created with Streamlit & DuckDB</div>", unsafe_allow_html=True)