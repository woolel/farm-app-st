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

# 가독성을 위한 CSS 스타일
st.markdown("""
    <style>
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    # 1. AI 모델 로드
    # 로컬에 'local_model' 폴더가 있으면 오프라인 모드, 없으면 온라인(HuggingFace) 다운로드
    # (GitHub 배포 시에는 local_model 폴더를 올리지 않으므로 자동으로 다운로드 됩니다)
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'AI 모델을 불러오는 중입니다... ({model_path})'):
        model = SentenceTransformer(model_path)
    
    # 2. DuckDB 연결
    if not os.path.exists('farming_granular.duckdb'):
        return None, None
        
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    
    # VSS(벡터 검색) 확장 로드
    try:
        con.execute("INSTALL vss; LOAD vss;")
    except Exception:
        pass # 이미 설치된 경우 무시
        
    return model, con

# 리소스 로딩 실행
model, con = load_resources()

# DB 파일 누락 시 에러 처리
if con is None:
    st.error("❌ 'farming_granular.duckdb' 파일을 찾을 수 없습니다. GitHub에 DB 파일을 업로드했는지 확인해주세요.")
    st.stop()

# ==========================================
# 3. 사이드바: 날짜 확인 및 검색 도우미
# ==========================================
today = datetime.now()
current_month = today.month

with st.sidebar:
    st.header("🔍 검색 도우미")
    st.info(f"오늘은 {today.year}년 {today.month}월 {today.day}일 입니다.")
    
    # 1) 카테고리 필터
    st.markdown("### 📂 분야 선택")
    selected_cats = st.multiselect(
        "관심 분야만 골라보세요:",
        ['기상', '양봉', '벼', '밭작물', '채소', '과수', '특용작물', '축산'],
        default=['양봉', '기상'] # 기본 선택값
    )
    
    # 2) 월별 추천 키워드 (시즌성 자동 변경)
    st.markdown(f"### 💡 {current_month}월 추천 키워드")
    
    if current_month in [12, 1, 2]:
        tags = ["월동 관리", "한파 대비", "전정(가지치기)", "화재 예방", "시설 하우스"]
    elif current_month in [3, 4, 5]:
        tags = ["파종 준비", "못자리", "봄벌 깨우기", "냉해 예방", "꽃가루 매개"]
    elif current_month in [6, 7, 8]:
        tags = ["장마 대비", "탄저병 방제", "혹서기 가축관리", "응애 방제", "배수로 정비"]
    else: # 9, 10, 11
        tags = ["수확 시기", "건조 관리", "가을 걷이", "월동 준비", "김장 채소"]

    # 버튼 클릭 시 검색어 자동 입력
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    for tag in tags:
        if st.button(f"#{tag}", use_container_width=True):
            st.session_state.search_query = tag

# ==========================================
# 4. 메인 화면: 오늘의 농사 브리핑 (자동 분석)
# ==========================================
st.title(f"📅 {current_month}월 {today.day}일, 농사 브리핑")

with st.expander("🌤️ 지난 3년, 오늘 이맘때 기상과 핵심 정보 보기 (클릭)", expanded=True):
    
    # [SQL 개선] 목차 제거, LIMIT 증가, 노이즈 필터링
    history_sql = f"""
        SELECT year, category, content 
        FROM farming 
        WHERE month = ? 
        AND category IN ('기상', '요약', '농업정보') 
        AND content NOT LIKE '%····%'   -- 목차 점선 제거
        AND content NOT LIKE '%목 차%'  -- 목차 텍스트 제거
        AND content NOT LIKE '%|---%'   -- 표 구분선 제거
        ORDER BY year DESC, category ASC
        LIMIT 50 -- 데이터를 충분히 가져와서 Python에서 연도별 균형 맞춤
    """
    history_data = con.execute(history_sql, [current_month]).fetchall()
    
    if history_data:
        # 연도별 데이터 정리 (카테고리, 전체내용) 튜플로 저장
        history_by_year = {}
        
        for year, cat, content in history_data:
            if year not in history_by_year:
                history_by_year[year] = []
            
            # 연도별 최대 3개까지만 담기 (스크롤 폭주 방지)
            if len(history_by_year[year]) >= 3:
                continue
            
            # 튜플 형태로 저장 (나중에 전체 내용을 보여주기 위함)
            history_by_year[year].append((cat, content))

        # 화면 출력
        available_years = sorted(history_by_year.keys(), reverse=True)
        
        if not available_years:
             st.warning("표시할 유효한 데이터가 없습니다.")
        else:
            # 연도 개수에 맞춰 컬럼 생성 (최대 3개)
            cols = st.columns(len(available_years))

            for i, year in enumerate(available_years):
                if i >= 3: break # 최대 3년치만 표시
                
                with cols[i]:
                    st.subheader(f"📆 {year}년") 
                    
                    for category, full_content in history_by_year[year]:
                        # 미리보기용 요약 (25자)
                        clean_text = full_content.replace('\n', ' ').strip()
                        preview_text = clean_text[:25] + "..." if len(clean_text) > 25 else clean_text
                        
                        # [UI 개선] Expander를 사용하여 내용 잘림 방지
                        with st.expander(f"**[{category}]** {preview_text}", expanded=False):
                            st.info(full_content) # 전체 내용 표시
    else:
        st.info("이맘때의 과거 데이터가 충분하지 않습니다.")

# ==========================================
# 5. 시맨틱 검색 엔진 (심층 검색)
# ==========================================
st.divider()
st.subheader("🔎 농사 지식 백과 검색")

query = st.text_input(
    "궁금한 내용을 입력하세요:", 
    value=st.session_state.search_query,
    placeholder="예: 겨울철 꿀벌 관리, 고추 탄저병 예방...",
    key="main_search"
)

if query:
    # 1. 카테고리 필터 SQL
    category_filter = ""
    if selected_cats:
        cats_str = "', '".join(selected_cats)
        category_filter = f"AND category IN ('{cats_str}')"

    with st.spinner(f"'{query}' 관련 문서를 AI가 분석 중입니다..."):
        # 2. 질문 벡터화
        query_vector = model.encode(query).tolist()
        
        # 3. 벡터 검색 SQL (여기에도 노이즈 필터 적용)
        sql = f"""
        SELECT score, category, year, month, content
        FROM (
            SELECT array_cosine_similarity(embedding, ?::FLOAT[768]) AS score, *
            FROM farming
        ) 
        WHERE score IS NOT NULL
        {category_filter}
        AND content NOT LIKE '%····%'   -- 목차 제거
        AND content NOT LIKE '%목 차%'  -- 목차 제거
        ORDER BY score DESC 
        LIMIT 5;
        """
        
        results = con.execute(sql, [query_vector]).fetchall()

    # 4. 결과 출력
    if not results:
        st.warning("조건에 맞는 정보를 찾지 못했습니다. 왼쪽 사이드바에서 '분야 선택'을 전체로 변경해보세요.")
    else:
        for row in results:
            score, cat, year, mon, content = row
            
            # 유사도 배지
            score_badge = "🟢 높음" if score > 0.6 else "🟡 보통"
            
            with st.container():
                st.markdown(f"#### [{cat}] {year}년 {mon}월 정보 <small>({score_badge} / 유사도 {score:.2f})</small>", unsafe_allow_html=True)
                
                # 검색어 하이라이팅 (빨간색 강조)
                highlighted_content = content.replace(query, f":red[**{query}**]")
                
                st.info(highlighted_content)
                st.caption("---")

# ==========================================
# 6. 푸터
# ==========================================
st.markdown("---")
st.markdown("<div style='text-align: center; color: gray;'>데이터 출처: 농촌진흥청 주간농사정보 | Created with Streamlit & DuckDB</div>", unsafe_allow_html=True)