import streamlit as st
import duckdb
import torch
from sentence_transformers import SentenceTransformer
from datetime import datetime
import re

# ==========================================
# 1. 페이지 설정 및 스타일
# ==========================================
st.set_page_config(
    page_title="스마트 농업 대시보드", 
    page_icon="🚜", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0');
    
    html, body, [class*="css"] {
        font-family: "Pretendard", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif !important;
    }
    
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; line-height: 1.6; }
    
    .highlight { 
        background-color: #e8f0fe; 
        padding: 2px 4px; 
        border-radius: 4px; 
        font-weight: bold; 
        color: #1a73e8;
    }
    
    .material-icon {
        vertical-align: middle;
        margin-right: 4px;
        line-height: 1;
    }
    
    .block-container {
        max-width: 900px;
        padding-top: 2rem;
        padding-bottom: 5rem;
        margin: 0 auto;
    }
    
    .score-badge {
        padding: 4px 8px;
        border-radius: 12px;
        color: white;
        font-weight: bold;
        font-size: 0.8em;
    }

    .filter-box {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

def material_icon(name, size=20, color=None):
    style = f"font-size:{size}px;"
    if color: style += f"color:{color};"
    return f"<span class='material-symbols-outlined material-icon' style='{style}'>{name}</span>"

# ==========================================
# 2. 리소스 로드
# ==========================================
@st.cache_resource
def load_resources():
    model_name = 'jhgan/ko-sroberta-multitask'
    
    with st.spinner("시스템 초기화 중..."):
        try:
            model = SentenceTransformer(model_name, device='cpu')
            con = duckdb.connect(
                'farming_granular.duckdb', 
                read_only=True, 
                config={'allow_unsigned_extensions': 'true'}
            )
            con.execute("INSTALL vss; LOAD vss;")
            return model, con, "ok"
        except Exception as e:
            return None, None, str(e)

model, con, status = load_resources()

if status != "ok":
    st.error(f"시스템 오류: {status}")
    st.stop()

# ==========================================
# 3. 유틸리티 및 데이터 함수
# ==========================================
def format_content(text):
    if not text: return ""
    text = text.replace('~', r'\~')
    return text

@st.cache_data(ttl=3600)
def get_week_list(year, month):
    try:
        # [수정] 정규식 패턴 앞에 r을 붙여 SyntaxWarning 해결
        sql = """
            SELECT DISTINCT regexp_extract(title, r'\[(.*?)\]', 1) as week_range 
            FROM farm_info 
            WHERE year = ? AND month = ? 
            ORDER BY week_range
        """
        return [row[0] for row in con.execute(sql, [int(year), int(month)]).fetchall() if row[0]]
    except:
        return []

@st.cache_data(ttl=3600)
def get_all_categories():
    try:
        sql = "SELECT DISTINCT unnest(tags_crop) FROM farm_info ORDER BY 1"
        rows = con.execute(sql).fetchall()
        return [r[0] for r in rows if r[0]]
    except:
        return []

# ==========================================
# 4. 상태 관리 (수정됨)
# ==========================================
today = datetime.now()
AVAILABLE_YEARS = [2023, 2024, 2025] # 데이터가 있는 연도 목록

if 'search_query' not in st.session_state:
    st.session_state.search_query = ""

# [수정] 현재 연도가 데이터 범위를 벗어나면 가장 최신 연도로 설정
if 'filter_year' not in st.session_state:
    if today.year in AVAILABLE_YEARS:
        st.session_state.filter_year = today.year
    else:
        st.session_state.filter_year = AVAILABLE_YEARS[-1] # 2025

if 'filter_month' not in st.session_state:
    st.session_state.filter_month = today.month
if 'selected_week_range' not in st.session_state:
    st.session_state.selected_week_range = None

# ==========================================
# 5. 상단 헤더 및 글로벌 필터
# ==========================================
st.markdown(f"## {material_icon('agriculture', size=36, color='#34a853')} 스마트 농업 대시보드", unsafe_allow_html=True)

# --- 필터 컨테이너 시작 ---
with st.container():
    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
    f_col1, f_col2 = st.columns(2)
    
    # [1] 아카이브 (날짜 선택)
    with f_col1:
        st.markdown(f"**{material_icon('calendar_month', color='#1a73e8')} 아카이브 (날짜 선택)**")
        c1, c2, c3 = st.columns([0.3, 0.3, 0.4])
        
        with c1:
            # index 계산 시 안전장치 확보
            try:
                default_idx = AVAILABLE_YEARS.index(st.session_state.filter_year)
            except ValueError:
                default_idx = len(AVAILABLE_YEARS) - 1 # 에러 발생 시 마지막 연도 선택
                
            sel_year = st.selectbox("연도", AVAILABLE_YEARS, 
                                  index=default_idx,
                                  key='sel_year_key', label_visibility="collapsed")
        with c2:
            sel_month = st.selectbox("월", range(1, 13), 
                                   index=st.session_state.filter_month-1, 
                                   key='sel_month_key', label_visibility="collapsed")
        
        weeks_list = get_week_list(sel_year, sel_month)
        weeks_options = ["전체 보기"] + weeks_list
        
        with c3:
            sel_week = st.selectbox("주간 선택", weeks_options, label_visibility="collapsed")
            if sel_week == "전체 보기":
                st.session_state.selected_week_range = None
            else:
                st.session_state.selected_week_range = sel_week

    # [2] 작목 선택 (필터)
    with f_col2:
        st.markdown(f"**{material_icon('filter_alt', color='#ea4335')} 작목 선택 (필터)**")
        all_tags = get_all_categories()
        selected_crops = st.multiselect(
            "작목을 선택하세요 (비어있으면 전체)", 
            all_tags, 
            placeholder="전체 (클릭하여 작목 선택)",
            label_visibility="collapsed"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
# --- 필터 컨테이너 끝 ---

# ==========================================
# 6. 중앙 대시보드 (필터링된 과거 기록)
# ==========================================
if st.session_state.selected_week_range:
    dashboard_title = f"{sel_year}년 {sel_month}월 ({st.session_state.selected_week_range})"
else:
    dashboard_title = f"{sel_year}년 {sel_month}월 전체"

st.caption(f"📌 현재 조회 중: **{dashboard_title}**")

with st.container(border=True):
    try:
        # 1. 기본 SQL 구성
        if st.session_state.selected_week_range:
            target_week = st.session_state.selected_week_range
            query_sql = """
                SELECT year, title, content_md, tags_crop 
                FROM farm_info 
                WHERE title LIKE ?
                AND title NOT LIKE '%요약%'
                ORDER BY title DESC
            """
            params = [f'%{target_week}%']
        else:
            query_sql = """
                SELECT year, title, content_md, tags_crop 
                FROM farm_info 
                WHERE year = ? AND month = ?
                AND title NOT LIKE '%요약%' 
                AND content_md NOT LIKE '%목 차%'
                ORDER BY title DESC
            """
            params = [sel_year, sel_month]

        # 2. 데이터 가져오기
        rows = con.execute(query_sql, params).fetchall()

        # 3. 작목 필터링
        filtered_rows = []
        if selected_crops:
            for r in rows:
                item_tags = r[3] if r[3] else []
                if any(crop in item_tags for crop in selected_crops):
                    filtered_rows.append(r)
        else:
            filtered_rows = rows

        # 4. 결과 출력
        if filtered_rows:
            cols = st.columns(2)
            for idx, item in enumerate(filtered_rows):
                yr, title, content, tags = item
                clean_title = title.split(']')[-1].strip() if ']' in title else title
                
                with cols[idx % 2]:
                    with st.popover(clean_title, use_container_width=True):
                        if tags:
                            st.caption(f"태그: {', '.join(tags)}")
                        st.markdown(format_content(content))
        else:
            st.info("조건에 맞는 데이터가 없습니다. 필터를 변경해보세요.")

    except Exception as e:
        st.error(f"데이터 로드 오류: {e}")

# ==========================================
# 7. 하단 전체 검색
# ==========================================
st.divider()
st.subheader("🔍 전체 검색")
st.caption("위의 필터와 상관없이 모든 데이터베이스를 검색합니다.")

with st.form("global_search_form", clear_on_submit=False):
    c1, c2 = st.columns([0.85, 0.15])
    with c1:
        query_input = st.text_input(
            "검색어 입력", 
            value=st.session_state.search_query,
            placeholder="질문을 입력하세요 (예: 봄배추 육묘, 고추 탄저병약)",
            label_visibility="collapsed"
        )
    with c2:
        search_btn = st.form_submit_button("검색")

if search_btn and query_input:
    with st.spinner("전체 데이터베이스 검색 중..."):
        try:
            query_vector = model.encode(query_input).tolist()
            
            sql = """
                SELECT 
                    year, month, title, content_md, 
                    array_cosine_similarity(embedding, ?::FLOAT[768]) as score
                FROM farm_info
                WHERE 1=1 
                ORDER BY score DESC
                LIMIT 10
            """
            
            results = con.execute(sql, [query_vector]).fetchall()
            
            # 커트라인 0.40
            valid_results = [r for r in results if r[4] >= 0.40]
            
            if not valid_results:
                st.warning("관련성이 높은 검색 결과가 없습니다.")
            else:
                st.success(f"'{query_input}' 검색 결과: {len(valid_results)}건")
                
                for row in valid_results[:5]:
                    yr, mn, title, content, score = row
                    
                    if score >= 0.65:
                        badge_color = "#34a853"
                        badge_text = "강력 추천"
                    elif score >= 0.50:
                        badge_color = "#f9ab00"
                        badge_text = "관련 있음"
                    else:
                        badge_color = "#9aa0a6"
                        badge_text = "참고용"
                    
                    clean_title = title.split(']')[-1].strip() if ']' in title else title
                    
                    with st.container(border=True):
                        st.markdown(f"""
                        <div style='display:flex; justify-content:space-between; align-items:center;'>
                            <span class='big-font'><b>{clean_title}</b></span>
                            <div style='background-color:{badge_color};' class='score-badge'>
                                {badge_text} ({score:.2f})
                            </div>
                        </div>
                        <div style='font-size:0.8em; color:gray; margin-top:4px;'>
                            {yr}년 {mn}월 자료
                        </div>
                        """, unsafe_allow_html=True)
                        
                        formatted_body = format_content(content)
                        for word in query_input.split():
                            if len(word) > 1:
                                formatted_body = formatted_body.replace(word, f"<span class='highlight'>{word}</span>")
                        
                        st.markdown(formatted_body, unsafe_allow_html=True)
                        
        except Exception as e:
            st.error(f"검색 오류: {e}")

st.markdown("---")
st.markdown("<div style='text-align:center; color:gray; font-size:0.8em;'>Data: 농촌진흥청 | Powered by DuckDB & Streamlit</div>", unsafe_allow_html=True)