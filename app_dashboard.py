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
@st.cache_resource(show_spinner="데이터베이스 및 AI 모델 로딩 중...")
def load_resources():
    model_name = 'jhgan/ko-sroberta-multitask'
    
    try:
        # 1. 모델 로드
        model = SentenceTransformer(model_name, device='cpu')
        
        # 2. In-Memory DuckDB 연결
        con = duckdb.connect(database=':memory:')
        
        # 3. Parquet 파일 로드
        # [Fix] HNSW 인덱스를 위해 embedding 컬럼을 FLOAT[768]로 명시적 변환
        con.execute("CREATE TABLE farm_info AS SELECT * REPLACE (list_transform(embedding, x -> x::FLOAT)::FLOAT[768] AS embedding) FROM 'weekly_farming.parquet'")
        
        # 4. 인덱스 생성 (Fast Startup)
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("CREATE INDEX embedding_idx ON farm_info USING HNSW (embedding);")
        
        con.execute("INSTALL fts; LOAD fts;")
        con.execute("PRAGMA create_fts_index('farm_info', 'id', 'title', 'content_md');")
        
        return model, con, "ok"
    except Exception as e:
        return None, None, str(e)

model, con, status = load_resources()

if status != "ok":
    st.error(f"시스템 오류: {status}")
    st.stop()

# ==========================================
# 3. 유틸리티 함수
# ==========================================
def format_content(text):
    if not text: return ""
    text = text.replace('~', r'\~')
    return text

@st.cache_data(ttl=3600)
def get_week_list(year, month):
    try:
        sql = """
            SELECT DISTINCT regexp_extract(title, '\\[(.*?)\\]', 1) as week_range 
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


def find_week_or_nearest(target_date):
    try:
        # 1. 모든 주차 정보 가져오기
        sql = "SELECT DISTINCT regexp_extract(title, '\\[(.*?)\\]', 1) as w_range FROM farm_info WHERE title LIKE '%[%]%'"
        rows = con.execute(sql).fetchall()
        all_ranges = [r[0] for r in rows if r[0] and '~' in r[0]]
        
        best_range = None
        min_diff = 99999
        
        # 2. 범위 검색
        for r_str in all_ranges:
            try:
                parts = r_str.split('~')
                if len(parts) != 2: continue
                s_date = datetime.strptime(parts[0].strip(), "%Y-%m-%d").date()
                e_date = datetime.strptime(parts[1].strip(), "%Y-%m-%d").date()
                
                # Case A: 날짜가 범위 내에 있음 (Exact Match)
                if s_date <= target_date <= e_date:
                    return r_str
                
                # Case B: 과거 데이터 중 가장 가까운 것 (Nearest Past)
                # target_date보다 이전에 끝난 주차들 중에서, 차이가 가장 적은 것
                if e_date < target_date:
                    diff = (target_date - e_date).days
                    if diff < min_diff:
                        min_diff = diff
                        best_range = r_str
            except:
                continue
                
        return best_range
    except Exception as e:
        return None

def get_year_month_from_range(w_range):
    try:
        if not w_range: return datetime.now().year, datetime.now().month
        date_str = w_range.split('~')[0]
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.year, dt.month
    except:
        return datetime.now().year, datetime.now().month


def organize_items_smartly(items, target_date_obj):
    if not items: return []

    weeks_group = {}
    for item in items:
        w_range = item[4]
        if not w_range: continue
        if w_range not in weeks_group: weeks_group[w_range] = []
        weeks_group[w_range].append(item)
    
    if not weeks_group: return []

    best_week = None
    min_diff_days = 9999
    
    for w_str in weeks_group.keys():
        try:
            start_str = w_str.split('~')[0]
            w_date = datetime.strptime(start_str, "%Y-%m-%d")
            w_date_adj = w_date.replace(year=target_date_obj.year)
            
            diff = abs((target_date_obj - w_date_adj).days)
            if diff < min_diff_days:
                min_diff_days = diff
                best_week = w_str
        except:
            continue
            
    if not best_week:
        best_week = list(weeks_group.keys())[0]

    target_items = weeks_group[best_week]
    
    summary_list = []
    weather_list = []
    others_list = []
    
    for item in target_items:
        title = item[1]
        if '요약' in title or '요 약' in title:
            summary_list.append(item)
        elif '기상' in title:
            weather_list.append(item)
        else:
            others_list.append(item)
            
    final_list = summary_list[:1] + weather_list[:1] + others_list
    return final_list[:4]

# ==========================================
# 4. 상태 관리
# ==========================================
today = datetime.now()
AVAILABLE_YEARS = [2023, 2024, 2025]

if 'search_query' not in st.session_state:
    st.session_state.search_query = ""

if 'filter_year' not in st.session_state:
    if today.year in AVAILABLE_YEARS:
        st.session_state.filter_year = today.year
    else:
        st.session_state.filter_year = AVAILABLE_YEARS[-1]

if 'filter_month' not in st.session_state:
    st.session_state.filter_month = today.month
if 'selected_week_range' not in st.session_state:
    st.session_state.selected_week_range = None

# ==========================================
# 5. 상단 헤더 및 글로벌 필터
# ==========================================
st.markdown(f"## {material_icon('agriculture', size=36, color='#34a853')} 스마트 농업 대시보드", unsafe_allow_html=True)

with st.container():

    f_col1, f_col2 = st.columns(2)
    
    # [1] 아카이브 (날짜 선택)
    # [1] 아카이브 (캘린더로 변경)
    with f_col1:
        st.markdown(f"**{material_icon('calendar_month', color='#1a73e8')} 날짜 선택 (아카이브)**", unsafe_allow_html=True)
        
        # 날짜 선택 위젯
        picked_date = st.date_input(
            "날짜를 선택하세요",
            value=datetime.today(),
            label_visibility="collapsed"
        )
        
        # 선택된 날짜에 맞는 주차 검색 (Exact or Nearest Past)
        found_range = find_week_or_nearest(picked_date)
        
        if found_range:
            st.session_state.selected_week_range = found_range
            sel_year, sel_month = get_year_month_from_range(found_range)
        else:
            # 데이터가 아예 없는 경우
            st.warning("데이터가 없는 구간입니다.")
            st.session_state.selected_week_range = None
            sel_year, sel_month = picked_date.year, picked_date.month


    # [2] 작목 선택 (필터) - 수정됨
    with f_col2:
        st.markdown(f"**{material_icon('filter_alt', color='#ea4335')} 작목 선택 (필터)**", unsafe_allow_html=True)
        all_tags = get_all_categories()
        # [수정] default를 비워두어 깔끔하게 보이게 함 (Logic에서 비어있으면 전체로 처리)
        selected_crops = st.multiselect(
            "작목을 선택하세요", 
            all_tags,
            default=[], # 초기 상태 비움
            placeholder="전체 (특정 작목을 보려면 클릭하세요)", # 안내 문구 변경
            label_visibility="collapsed"
        )


# ==========================================
# 6. 중앙 대시보드
# ==========================================
if st.session_state.selected_week_range:
    target_date_str = st.session_state.selected_week_range.split('~')[0]
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    dashboard_title = f"{sel_year}년 {sel_month}월 ({st.session_state.selected_week_range})"
    st.caption(f"📌 선택된 기간: **{dashboard_title}**")
else:
    target_date = datetime.now()
    dashboard_title = f"{sel_year}년 {sel_month}월 (오늘 날짜 기준 비교)"
    st.caption(f"📌 **오늘: {target_date.year}년 {target_date.month}월 {target_date.day}일** 기준, 지난 3년의 가장 유사한 시기 기록입니다.")

with st.container(border=True):
    try:
        if st.session_state.selected_week_range:
            query_sql = """
                SELECT year, title, content_md, tags_crop, regexp_extract(title, '\\[(.*?)\\]', 1) as w_range
                FROM farm_info 
                WHERE title LIKE ?
                ORDER BY year DESC
            """
            params = [f'%{st.session_state.selected_week_range}%']
        else:
            query_sql = """
                SELECT year, title, content_md, tags_crop, regexp_extract(title, '\\[(.*?)\\]', 1) as w_range
                FROM farm_info 
                WHERE month = ?
                AND content_md NOT LIKE '%목 차%'
                ORDER BY year DESC
            """
            params = [sel_month]

        rows = con.execute(query_sql, params).fetchall()

        # [수정] 작목 필터링 로직 변경
        filtered_rows = []
        
        # 사용자가 작목을 하나라도 선택했다면 -> 그 작목들만 필터링
        if selected_crops:
            for r in rows:
                item_tags = r[3] if r[3] else []
                if any(crop in item_tags for crop in selected_crops):
                    filtered_rows.append(r)
        # 선택하지 않았다면(비어있으면) -> 전체 데이터 표시 (All)
        else:
            filtered_rows = rows

        if filtered_rows:
            grouped_by_year = {2025: [], 2024: [], 2023: []}
            for item in filtered_rows:
                y = item[0]
                if y in grouped_by_year:
                    grouped_by_year[y].append(item)
            
            for year in [2025, 2024, 2023]:
                items = grouped_by_year[year]
                
                if items:
                    st.markdown(f"##### {material_icon('calendar_today', color='#5f6368')} {year}년 기록", unsafe_allow_html=True)
                    
                    display_items = organize_items_smartly(items, target_date)
                    
                    if not display_items:
                        st.caption("해당 시기의 데이터가 부족합니다.")
                        st.divider()
                        continue

                    cols = st.columns(2)
                    for idx, item in enumerate(display_items):
                        yr, title, content, tags, w_range = item
                        clean_title = title.split(']')[-1].strip() if ']' in title else title
                        
                        icon = "📄"
                        if '요약' in title or '요 약' in title:
                            icon = "⭐"
                            clean_title = f"<b>{clean_title}</b>"
                        elif '기상' in title:
                            icon = "⛅"

                        with cols[idx % 2]:
                            with st.popover(f"{icon} {clean_title}", use_container_width=True):
                                if tags:
                                    st.caption(f"태그: {', '.join(tags)}")
                                st.markdown(format_content(content), unsafe_allow_html=True)
                    
                    st.divider()
        else:
            st.info("조건에 맞는 데이터가 없습니다. 작목 필터를 변경해보세요.")

    except Exception as e:
        st.error(f"데이터 로드 오류: {e}")

# ==========================================
# 7. 하단 전체 검색
# ==========================================
st.subheader("🔍 전체 검색")
st.caption("위의 필터와 상관없이 모든 데이터베이스를 검색합니다.")

with st.form("global_search_form", clear_on_submit=False):
    c1, c2 = st.columns([0.85, 0.15])
    with c1:
        query_input = st.text_input("검색어 입력", value=st.session_state.search_query, placeholder="예: 봄배추 육묘", label_visibility="collapsed")
    with c2:
        search_btn = st.form_submit_button("검색")

if search_btn and query_input:
    with st.spinner("검색 중..."):
        try:
            query_vector = model.encode(query_input).tolist()
            sql = """
                SELECT year, month, title, content_md, array_cosine_similarity(embedding, ?::FLOAT[768]) as score
                FROM farm_info WHERE 1=1 ORDER BY score DESC LIMIT 10
            """
            results = con.execute(sql, [query_vector]).fetchall()
            valid_results = [r for r in results if r[4] >= 0.40]
            
            if not valid_results:
                st.warning("결과 없음")
            else:
                st.success(f"{len(valid_results)}건 발견")
                for row in valid_results[:5]:
                    yr, mn, title, content, score = row
                    
                    badge, color = "참고용", "#9aa0a6"
                    if score >= 0.65: badge, color = "강력 추천", "#34a853"
                    elif score >= 0.50: badge, color = "관련 있음", "#f9ab00"
                    
                    clean_title = title.split(']')[-1].strip()
                    with st.container(border=True):
                        st.markdown(f"""
                        <div style='display:flex; justify-content:space-between;'>
                            <span class='big-font'><b>{clean_title}</b></span>
                            <div style='background-color:{color};' class='score-badge'>{badge} ({score:.2f})</div>
                        </div>
                        <div style='font-size:0.8em; color:gray;'>{yr}년 {mn}월</div>
                        """, unsafe_allow_html=True)
                        
                        hl_content = format_content(content)
                        for w in query_input.split():
                            if len(w)>1: hl_content = hl_content.replace(w, f"<span class='highlight'>{w}</span>")
                        st.markdown(hl_content, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"오류: {e}")

st.markdown("---")
st.markdown("<div style='text-align:center; color:gray; font-size:0.8em;'>Data: 농촌진흥청 | Powered by DuckDB & Streamlit</div>", unsafe_allow_html=True)