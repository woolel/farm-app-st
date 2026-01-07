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
    /* 표가 잘리지 않게 스타일 조정 */
    .stMarkdown table { width: 100% !important; display: table !important; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    # 1. AI 모델 로드
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'AI 모델 로딩 중... ({model_path})'):
        model = SentenceTransformer(model_path)
    
    # 2. DuckDB 연결
    if not os.path.exists('farming_granular.duckdb'):
        return None, None
        
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    try:
        con.execute("INSTALL vss; LOAD vss;")
    except Exception:
        pass 
        
    return model, con

model, con = load_resources()

if con is None:
    st.error("❌ 'farming_granular.duckdb' 파일이 없습니다. GitHub 업로드 여부를 확인하세요.")
    st.stop()

# ==========================================
# 3. 유틸리티 함수 (텍스트 정제)
# ==========================================
def clean_text_for_display(text):
    """
    화면에 출력할 때 마크다운 문법 충돌을 방지하는 함수
    1. 물결표(~)가 취소선(~~)으로 오인되지 않도록 이스케이프 처리
    2. 불필요한 연속 공백 제거
    """
    if not text: return ""
    # 마크다운에서 ~를 그냥 쓰면 취소선이 될 수 있으므로 \~로 변환하여 문자로 강제 인식
    safe_text = text.replace('~', '\~')
    return safe_text

# ==========================================
# 4. 사이드바 설정
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
        tags = ["월동 관리", "한파 대비", "전정", "화재 예방", "시설 하우스"]
    elif current_month in [3, 4, 5]:
        tags = ["파종 준비", "못자리", "봄벌 깨우기", "냉해 예방", "꽃가루"]
    elif current_month in [6, 7, 8]:
        tags = ["장마 대비", "탄저병", "혹서기", "응애 방제", "배수로"]
    else: 
        tags = ["수확 시기", "건조 관리", "가을 걷이", "월동 준비", "김장"]

    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    for tag in tags:
        if st.button(f"#{tag}", use_container_width=True):
            st.session_state.search_query = tag

# ==========================================
# 5. 메인 화면: 오늘의 농사 브리핑
# ==========================================
st.title(f"📅 {current_month}월 {today.day}일, 농사 브리핑")

with st.container():
    st.markdown("### 🌤️ 지난 3년, 오늘 이맘때 핵심 정보")
    
    # [SQL] 목차 제거 및 데이터 조회
    history_sql = f"""
        SELECT year, category, content 
        FROM farming 
        WHERE month = ? 
        AND content NOT LIKE '%····%'
        AND content NOT LIKE '%목 차%'
        AND content NOT LIKE '%제1장%'
        AND category NOT IN ('목차')
        ORDER BY year DESC
        LIMIT 100
    """
    history_data = con.execute(history_sql, [current_month]).fetchall()
    
    if history_data:
        history_by_year = {}
        
        # [우선순위 정렬 함수] 기상 > 요약 > 나머지
        def get_priority(cat_name):
            if '기상' in cat_name: return 0
            if '요약' in cat_name: return 1
            if '핵심' in cat_name: return 2
            return 99

        for year, cat, content in history_data:
            if year not in history_by_year:
                history_by_year[year] = []
            
            # [Python 필터링] 복잡한 목차 테이블 제거
            if content.count('|') > 5 and ('제1장' in content or '농업정보' in content):
                continue
            
            if len(history_by_year[year]) >= 5: continue
            
            # 중복 제거
            if any(item[1] == content for item in history_by_year[year]):
                continue

            history_by_year[year].append((cat, content))

        # 연도별 정렬 및 출력
        available_years = sorted(history_by_year.keys(), reverse=True)
        
        if not available_years:
             st.warning("표시할 유효한 데이터가 없습니다.")
        else:
            # [레이아웃] 세로 배치 (표 깨짐 방지)
            for i, year in enumerate(available_years):
                if i >= 3: break 
                
                st.markdown(f"#### 📆 {year}년 {current_month}월")
                
                # 기상 우선 정렬
                items = sorted(history_by_year[year], key=lambda x: get_priority(x[0]))
                
                for category, full_content in items:
                    # [텍스트 정제] 취소선 방지 적용
                    safe_content = clean_text_for_display(full_content)
                    
                    # 미리보기 텍스트
                    clean_text = safe_content.replace('\n', ' ').strip()
                    preview_text = clean_text[:40] + "..." if len(clean_text) > 40 else clean_text
                    
                    if '기상' in category: icon = "☁️"
                    elif '요약' in category: icon = "📝"
                    else: icon = "📌"

                    with st.expander(f"{icon} **[{category}]** {preview_text}", expanded=False):
                        st.markdown(safe_content)
                
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

if query:
    category_filter = ""
    if selected_cats:
        cats_str = "', '".join(selected_cats)
        category_filter = f"AND category IN ('{cats_str}')"

    with st.spinner(f"AI가 '{query}' 관련 문서를 분석 중입니다..."):
        query_vector = model.encode(query).tolist()
        
        sql = f"""
        SELECT score, category, year, month, content
        FROM (
            SELECT array_cosine_similarity(embedding, ?::FLOAT[768]) AS score, *
            FROM farming
        ) 
        WHERE score IS NOT NULL
        {category_filter}
        AND content NOT LIKE '%····%'
        AND content NOT LIKE '%목 차%'
        AND category NOT IN ('목차')
        ORDER BY score DESC 
        LIMIT 5;
        """
        results = con.execute(sql, [query_vector]).fetchall()

    if not results:
        st.warning("조건에 맞는 정보를 찾지 못했습니다.")
    else:
        for row in results:
            score, cat, year, mon, content = row
            score_badge = "🟢 높음" if score > 0.6 else "🟡 보통"
            
            # [텍스트 정제] 취소선 방지 적용
            safe_content = clean_text_for_display(content)
            
            with st.container():
                st.markdown(f"#### [{cat}] {year}년 {mon}월 정보 <small>({score_badge})</small>", unsafe_allow_html=True)
                
                # 검색어 하이라이팅
                highlighted_content = safe_content.replace(query, f":red[**{query}**]")
                st.info(highlighted_content)
                st.caption("---")

# ==========================================
# 7. 푸터
# ==========================================
st.markdown("<br><div style='text-align: center; color: gray;'>데이터 출처: 농촌진흥청 주간농사정보 | Created with Streamlit & DuckDB</div>", unsafe_allow_html=True)