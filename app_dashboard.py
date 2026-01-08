import streamlit as st
import duckdb
from sentence_transformers import SentenceTransformer
from datetime import datetime
import os
import re
from kiwipiepy import Kiwi

# ==========================================
# 1. 페이지 설정 및 디자인
# ==========================================
st.set_page_config(
    page_title="스마트 농업 대시보드", 
    page_icon="🚜", 
    layout="wide",
    initial_sidebar_state="collapsed" # 햄버거 메뉴를 위해 기본 접힘
)

# CSS 스타일 커스텀
st.markdown("""
    <style>
    /* 0. Material Symbols CDN 로드 */
    @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0');

    /* 1. 한글 폰트 강제 적용 */
    html, body, [class*="css"] {
        font-family: "Pretendard", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif !important;
    }

    /* 2. 텍스트 크기 조정 */
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; line-height: 1.6; }
    
    /* 3. 테이블 스타일 */
    table { 
        width: 100% !important; 
        border-collapse: collapse !important; 
        margin-bottom: 1rem !important; 
        display: block; 
        overflow-x: auto;
    }
    th, td { 
        padding: 8px 12px !important; 
        border: 1px solid #ddd !important; 
        text-align: left !important; 
        font-size: 15px !important; 
        white-space: pre-wrap; 
    }
    th { 
        background-color: #f8f9fa !important; 
        font-weight: bold; 
        color: #202124;
    }
    
    /* 4. 검색어 하이라이트 스타일 (Google Blue) */
    .highlight { 
        background-color: #e8f0fe; 
        padding: 2px 4px; 
        border-radius: 4px; 
        font-weight: bold; 
        color: #1a73e8;
    }

    /* 5. Material Icon 스타일 */
    .material-icon {
        vertical-align: middle;
        margin-right: 4px;
        line-height: 1;
    }

    /* 6. 메인 컨테이너 폭 조정 및 중앙 정렬 (데스크탑) */
    .block-container {
        max-width: 900px;
        padding-top: 2rem;
        padding-bottom: 2rem;
        margin: 0 auto;
    /* 7. 하단 팝오버 상단 전개 설정 */
    div[data-testid="stPopoverBody"] {
        bottom: 50px !important;
        top: auto !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'AI 모델 및 데이터베이스 로딩 중... ({model_path})'):
        try:
            model = SentenceTransformer(model_path)
        except Exception as e:
            return None, None, f"model_error: {e}"
    
    if not os.path.exists('farming_granular.duckdb'):
        return None, None, "file_not_found"
        
    try:
        con = duckdb.connect('farming_granular.duckdb', read_only=True)
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("INSTALL fts; LOAD fts;")
        
        schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
        fts_status = "ok"
        if not any('fts_main_farming' in str(row) for row in schemas):
            fts_status = "fts_missing"
            
    except Exception as e:
        return None, None, f"db_error: {e}"
        
    return model, con, fts_status

@st.cache_data(ttl=3600)
def get_monthly_trends(month, _con):
    try:
        sql = """
            SELECT category, count(*) as cnt
            FROM farming
            WHERE month = ?
            GROUP BY category
            ORDER BY cnt DESC
        """
        return _con.execute(sql, [month]).fetchall()
    except:
        return []

@st.cache_data(ttl=3600)
def get_week_list(year, month, _con):
    """특정 연도/월의 주간 정보(id) 목록 조회"""
    try:
        sql = "SELECT DISTINCT id FROM farming WHERE year = ? AND month = ? ORDER BY id"
        return [row[0] for row in _con.execute(sql, [int(year), int(month)]).fetchall()]
    except:
        return []

model, con, status = load_resources()

if isinstance(status, str) and "error" in status:
    st.error(f"시스템 초기화 오류: {status}")
    st.stop()

if status == "file_not_found":
    st.markdown(f"""
        <div style="padding:15px; border-radius:5px; background-color:#f8d7da; color:#721c24; border:1px solid #f5c6cb;">
            {material_icon('error', color='#ea4335')} 'farming_granular.duckdb' 데이터베이스 파일이 없습니다.
        </div>
    """, unsafe_allow_html=True)
    st.stop()

if status == "fts_missing":
    st.markdown(f"""
        <div style="padding:15px; border-radius:5px; background-color:#fff3cd; color:#856404; border:1px solid #ffeeba; margin-bottom:20px;">
            {material_icon('warning', color='#fbbc04')} 검색 인덱스(FTS)가 감지되지 않아 키워드 검색 성능이 저하될 수 있습니다.
        </div>
    """, unsafe_allow_html=True)

# ==========================================
# 3. 유틸리티 함수
# ==========================================
@st.cache_resource
def get_kiwi():
    """Kiwi 객체 캐싱 (성능 최적화)"""
    return Kiwi()

def extract_keywords(text):
    """명사, 동사 어근, 숫자만 추출하여 AI 검색 품질 향상"""
    if not text: return ""
    kiwi = get_kiwi()
    result = kiwi.tokenize(text)
    keywords = [t.form for t in result if t.tag.startswith('N') or t.tag.startswith('V') or t.tag == 'SN']
    return " ".join(keywords) if keywords else text
def material_icon(name, size=20, color=None, font_weight=400):
    """Material Symbols 아이콘을 반환하는 헬퍼 함수"""
    style = f"font-size:{size}px; font-weight:{font_weight};"
    if color:
        style += f"color:{color};"
    return f"<span class='material-symbols-outlined material-icon' style='{style}'>{name}</span>"

def format_content(text):
    """
    텍스트 포맷팅 함수
    - 기본적으로 Streamlit 마크다운 렌더링 사용
    - 마크다운 테이블이 깨지는 경우(구분선 누락 등)를 대비한 최소한의 보정 로직 적용
    """
    if not text: return ""
    text = text.replace('~', r'\~') # 물결표 이스케이프
    
    lines = text.splitlines()
    output = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        clean_line = line.strip()
        
        # 테이블 헤더 감지 (파이프가 있고 내용이 있는 첫 줄)
        if '|' in clean_line and any(c.isalnum() for c in clean_line):
            # 다음 줄이 구분선(|---|)인지 확인
            is_table_start = False
            if i + 1 < len(lines):
                next_line = lines[i+1].strip()
                if '|' in next_line and '-' in next_line and not any(c.isalnum() for c in next_line):
                    is_table_start = True
            
            # 구분선이 없는데 파이프가 많은 경우 -> 테이블로 간주하고 구분선 강제 삽입
            if not is_table_start and clean_line.count('|') >= 2:
                # 현재 줄 출력 (헤더)
                output.append(line)
                
                # 가상 구분선 생성 (헤더의 파이프 개수에 맞춰)
                col_count = clean_line.count('|') - 1
                if col_count < 1: col_count = 1
                separator = "|" + " --- |" * col_count
                output.append(separator)
                
                i += 1
                continue

        output.append(line)
        i += 1
            
    return '\n'.join(output)

# ==========================================
# 4. 앱 상태 관리 및 상수
# ==========================================
today = datetime.now()
current_month = today.month

if 'search_query' not in st.session_state:
    st.session_state.search_query = ""
if 'selected_week_id' not in st.session_state:
    st.session_state.selected_week_id = None

# 추천 검색어 로직 이동
keywords_map = {
    (12, 1, 2): ["월동 관리", "한파", "전정", "화재 예방"],
    (3, 4, 5): ["파종", "육묘", "냉해", "꽃가루 매개"],
    (6, 7, 8): ["장마", "탄저병", "침수", "고온"],
    (9, 10, 11): ["수확", "건조", "가을 파종", "단풍"]
}
recommendations = []
for months, tags in keywords_map.items():
    if current_month in months:
        recommendations = tags
        break

# ==========================================
# 5. 메인 레이아웃 및 과거 데이터
# ==========================================
# 메인 헤더
header_col1, header_col2 = st.columns([0.1, 0.9])
with header_col2:
    st.markdown(f"## {material_icon('agriculture', size=36, color='#34a853')} 스마트 농업 대시보드", unsafe_allow_html=True)

# 오늘 날짜를 기준으로 제목 동적 생성
title_date = today.strftime("%m월 %d일")
st.markdown(f"### {material_icon('calendar_month', size=28, color='#1a73e8')} {title_date}의 과거 농사 기록 (최근 3년)", unsafe_allow_html=True)

# 과거 기록 데이터 조회 및 섹션 구성
with st.container(border=True):
    history_sql = """
        SELECT id, year, category, content 
        FROM farming 
        WHERE month = ? 
        AND content NOT LIKE '%목 차%' 
        AND category != '목차'
        ORDER BY year DESC, category
    """
    try:
        # 아카이브로 특정 주간을 선택한 경우 해당 데이터만 조회, 아니면 오늘 날짜 기준
        if st.session_state.selected_week_id:
            rows = con.execute("SELECT id, year, category, content FROM farming WHERE id = ? AND category != '목차'", [st.session_state.selected_week_id]).fetchall()
            valid_items = rows
        else:
            rows = con.execute(history_sql, [current_month]).fetchall()
            valid_items = []
            seen_contents = set()

            for r in rows:
                rid, ryear, rcat, rcontent = r
                content_sig = re.sub(r'\s+', '', rcontent)[:50]
                if content_sig in seen_contents: continue
                seen_contents.add(content_sig)

                try:
                    start_str, end_str = rid.split('~')
                    s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(year=today.year)
                    e_date = datetime.strptime(end_str, "%Y-%m-%d").replace(year=today.year)
                    
                    if s_date <= today <= e_date:
                        is_match = True
                    else:
                        days_diff = min(abs((today - s_date).days), abs((today - e_date).days))
                        is_match = days_diff <= 3
                    
                    if is_match:
                        valid_items.append(r)
                except:
                    continue

        if valid_items:
            grouped = {}
            for item in valid_items:
                y = item[1]
                if y not in grouped: grouped[y] = []
                grouped[y].append(item)
            
            # 연도별 세로 전개
            for y in sorted(grouped.keys(), reverse=True)[:3]:
                st.markdown(f"**{material_icon('push_pin', color='#ea4335')} {y}년 기록**", unsafe_allow_html=True)
                
                # 내용 2단 2행 (최대 4개) 그리드 배치
                cols = st.columns(2)
                for idx, item in enumerate(grouped[y][:4]): 
                    cat, content = item[2], item[3]
                    cat_prefix = f"[{cat}] " if cat and cat != 'content' else ""
                    short_content = content.split('\n')[0][:30] + "..."
                    with cols[idx % 2]:
                        with st.popover(f"{cat_prefix}{short_content}", use_container_width=True):
                            st.markdown(format_content(content), unsafe_allow_html=True)
                st.divider()
        else:
            st.info("해당 기간의 과거 정보가 없습니다.")
            
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")

# ==========================================
# 6. 하단 통합 검색 바 (필터 | 검색 | 아카이브)
# ==========================================
st.divider()
bar1, bar2, bar3 = st.columns([0.15, 0.7, 0.15])

with bar1:
    with st.popover("🔍 작목 선택", use_container_width=True):
        selected_cats = st.multiselect(
            "필터링할 작목:",
            ['기상', '벼', '밭작물', '채소', '과수', '특용작물', '축산', '양봉'],
            default=['기상', '과수']
        )

with bar2:
    with st.form("search_form", clear_on_submit=False):
        c1, c2 = st.columns([0.85, 0.15])
        with c1:
            query_input = st.text_input(
                "질문", 
                value=st.session_state.search_query,
                placeholder="예: 사과 탄저병 방제 시기는?",
                label_visibility="collapsed"
            )
        with c2:
            search_btn = st.form_submit_button("🔍")

with bar3:
    with st.popover("📅 아카이브", use_container_width=True):
        # segmented_control은 Streamlit 1.40+ 에서 st.segmented_control 로 사용 가능
        # 지원되지 않는 환경이라면 st.radio로 대체 (여기선 요청대로 구현)
        try:
            arch_year = st.segmented_control("연도", ["2023", "2024", "2025"], default="2025")
        except:
            arch_year = st.radio("연도", ["2023", "2024", "2025"], horizontal=True)
            
        arch_month = st.selectbox("월", [m for m in range(1, 13)], format_func=lambda x: f"{x}월", index=current_month-1)
        
        weeks = get_week_list(arch_year, arch_month, con)
        if weeks:
            st.caption(f"{arch_year}년 {arch_month}월의 주간 목록:")
            for w_id in weeks:
                if st.button(f"{w_id}", key=f"week_{w_id}", use_container_width=True):
                    st.session_state.selected_week_id = w_id
                    st.rerun()
        else:
            st.caption("해당 기간의 데이터가 없습니다.")
        
        if st.button("🔄 오늘 날짜로 초기화", use_container_width=True):
            st.session_state.selected_week_id = None
            st.rerun()

# 추천 검색어 칩 (Streamlit 버튼 방식)
if recommendations:
    st.caption("✨ 추천 검색어:")
    # n+1 컬럼 생성 (간격 조절용 첫 컬럼 포함)
    n_tags = len(recommendations)
    chip_cols = st.columns([0.1] + [0.9/n_tags] * n_tags)
    for i, tag in enumerate(recommendations):
        if chip_cols[i+1].button(f"#{tag}", key=f"chip_{tag}", use_container_width=True):
            st.session_state.search_query = tag
            st.rerun()
else:
    st.caption("현재 추천 검색어가 없습니다.")

if search_btn and query_input:
    cat_filter_sql = ""
    if selected_cats:
        cat_list_str = "', '".join(selected_cats)
        cat_filter_sql = f"AND category IN ('{cat_list_str}')"

    with st.spinner("AI가 문서를 분석 중입니다..."):
        # 검색어 정규화 (명사/동사/숫자 추출)
        clean_query = extract_keywords(query_input)
        query_vector = model.encode(clean_query).tolist()
        
        # 하이브리드 검색 SQL (Semantic 1.5배 + FTS 0.5배 가중치 결합)
        search_sql = f"""
        SELECT 
            vector_score,
            fts_score,
            category, year, month, content
        FROM (
            SELECT 
                array_cosine_similarity(embedding, ?::FLOAT[768]) AS vector_score,
                fts_main_farming.match_bm25(pk, ?) AS fts_score,
                category, year, month, content
            FROM farming
            WHERE 1=1 {cat_filter_sql}
        ) sub
        WHERE vector_score > 0.40 -- 최소 관련성 필터 완화 (전처리 후엔 점수 편차가 커질 수 있음)
        ORDER BY (vector_score * 1.5 + fts_score * 0.5) DESC -- 가중치 기반 하이브리드 정렬
        LIMIT 5
        """
        
        try:
            results = con.execute(search_sql, [query_vector, query_input]).fetchall()
            
            if not results:
                st.markdown(f"""
                    <div style="padding:15px; border-radius:5px; background-color:#fff3cd; color:#856404; border:1px solid #ffeeba;">
                        {material_icon('sentiment_dissatisfied', color='#fbbc04')} 검색 결과가 없습니다. 질문을 구체적으로 바꾸거나 필터를 해제해보세요.
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.success(f"총 {len(results)}건의 관련 정보를 찾았습니다.")
                
                for row in results:
                    v_score, f_score, cat, yr, mn, body = row
                    
                    # [핵심 수정] NoneType 에러 방지용 안전장치
                    if v_score is None: v_score = 0.0
                    if f_score is None: f_score = 0.0
                    
                    # 뱃지 로직
                    badge_color = "#34a853" if v_score > 0.65 else "#fbbc04"
                    match_type = "AI+키워드" if f_score > 0 else "AI추론"
                    
                    # 'content' 카테고리는 표시하지 않음
                    cat_display = f"<b>[{cat}]</b> " if cat and cat != 'content' else ""

                    with st.container(border=True):
                        st.markdown(f"""
                        <div style='display:flex; justify-content:space-between; align-items:center;'>
                            <span class='big-font'>{cat_display}{yr}년 {mn}월 자료</span>
                            <span style='color:{badge_color}; font-weight:bold; font-size:0.9em;'>
                                유사도 {v_score:.2f} ({match_type})
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        highlighted_body = format_content(body)
                        for word in query_input.split():
                            if len(word) > 1:
                                highlighted_body = highlighted_body.replace(word, f"<span class='highlight'>{word}</span>")
                        
                        st.markdown(highlighted_body, unsafe_allow_html=True)
                        
        except Exception as e:
            st.error(f"검색 처리 중 오류가 발생했습니다: {e}")

st.markdown("---")
st.markdown("<div style='text-align:center; color:gray; font-size:0.8em;'>데이터 출처: 농촌진흥청 주간농사정보 | Powered by DuckDB & Streamlit</div>", unsafe_allow_html=True)