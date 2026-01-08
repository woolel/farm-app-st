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
    initial_sidebar_state="expanded"
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

model, con, status = load_resources()

if isinstance(status, str) and "error" in status:
    st.error(f"시스템 초기화 오류: {status}")
    st.stop()

if status == "file_not_found":
    st.error(f"{material_icon('error', color='#ea4335')} 'farming_granular.duckdb' 데이터베이스 파일이 없습니다.", unsafe_allow_html=True)
    st.stop()

if status == "fts_missing":
    st.warning(f"{material_icon('warning', color='#fbbc04')} 검색 인덱스(FTS)가 감지되지 않아 키워드 검색 성능이 저하될 수 있습니다.", unsafe_allow_html=True)

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
    if not text: return ""
    text = text.replace('~', r'\~') 
    lines = text.split('\n')
    formatted_lines = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        if '|' in line and len(line) > 3:
            processed_line = line.replace('|', ' | ')
            processed_line = re.sub(r'\s+\|\s+', ' | ', processed_line) 
            if i > 0 and '|' not in lines[i-1]:
                formatted_lines.append("") 
            formatted_lines.append(processed_line)
        else:
            formatted_lines.append(line)
    return '\n'.join(formatted_lines)

# ==========================================
# 4. 사이드바 UI
# ==========================================
today = datetime.now()
current_month = today.month

with st.sidebar:
    st.markdown(f"## {material_icon('agriculture', size=32, color='#34a853')} 스마트 농업 봇", unsafe_allow_html=True)
    st.info(f"오늘 날짜: {today.year}년 {today.month}월 {today.day}일")
    
    st.markdown(f"### {material_icon('sell', color='#1a73e8')} 관심 분야 설정", unsafe_allow_html=True)
    selected_cats = st.multiselect(
        "필터링할 작목/분야:",
        ['기상', '벼', '밭작물', '채소', '과수', '특용작물', '축산', '양봉'],
        default=['기상', '과수']
    )
    
    st.divider()
    
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
            
    st.markdown(f"### {material_icon('lightbulb', color='#fbbc04')} {current_month}월 추천 검색어", unsafe_allow_html=True)
    
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    cols = st.columns(2)
    for i, tag in enumerate(recommendations):
        if cols[i % 2].button(f"#{tag}", key=f"btn_{tag}", use_container_width=True):
            st.session_state.search_query = tag

    st.divider()
    st.markdown(f"{material_icon('bar_chart', color='#1a73e8')} **이달의 데이터 분포**", unsafe_allow_html=True)
    trends = get_monthly_trends(current_month, con)
    if trends:
        trend_df = {row[0]: row[1] for row in trends[:5]}
        st.bar_chart(trend_df, height=150, color='#1a73e8')
    else:
        st.caption("데이터 집계 중...")

# ==========================================
# 5. 메인: 과거 데이터 (History)
# ==========================================
st.markdown(f"### {material_icon('calendar_month', size=28, color='#1a73e8')} {current_month}월의 과거 농사 기록 (최근 3년)", unsafe_allow_html=True)

with st.expander(f"지난 3년간 오늘 이맘때의 주요 정보 보기", expanded=True):
    history_sql = """
        SELECT id, year, category, content 
        FROM farming 
        WHERE month = ? 
        AND content NOT LIKE '%목 차%' 
        AND category != '목차'
        ORDER BY year DESC, category
    """
    try:
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
                target_date = today
                
                if s_date <= target_date <= e_date:
                    is_match = True
                else:
                    days_diff = min(abs((target_date - s_date).days), abs((target_date - e_date).days))
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
            
            for y in sorted(grouped.keys(), reverse=True)[:3]:
                st.markdown(f"**{material_icon('push_pin', color='#ea4335')} {y}년 기록**", unsafe_allow_html=True)
                cols = st.columns(2)
                for idx, item in enumerate(grouped[y][:4]): 
                    cat, content = item[2], item[3]
                    short_content = content.split('\n')[0][:30] + "..."
                    with cols[idx % 2]:
                        with st.popover(f"[{cat}] {short_content}"):
                            st.markdown(format_content(content), unsafe_allow_html=True)
        else:
            st.info("이맘때와 정확히 일치하는 과거 주간 정보가 없습니다.")
            
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")

st.divider()

# ==========================================
# 6. 시맨틱 하이브리드 검색
# ==========================================
st.markdown(f"## {material_icon('search', size=32, color='#1a73e8')} 농업 지식 검색", unsafe_allow_html=True)

with st.form("search_form"):
    col1, col2 = st.columns([4, 1])
    with col1:
        query_input = st.text_input(
            "질문", 
            value=st.session_state.search_query,
            placeholder="예: 사과 탄저병 방제 시기는?",
            label_visibility="collapsed"
        )
    with col2:
        search_btn = st.form_submit_button("검색 🚀", use_container_width=True) # Streamlit 버튼 내부엔 HTML 주입이 어려우므로 🚀 유지 혹은 텍스트만

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
                st.warning(f"{material_icon('sentiment_dissatisfied', color='#fbbc04')} 검색 결과가 없습니다. 질문을 구체적으로 바꾸거나 필터를 해제해보세요.", unsafe_allow_html=True)
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
                    
                    with st.container(border=True):
                        st.markdown(f"""
                        <div style='display:flex; justify-content:space-between; align-items:center;'>
                            <span class='big-font'><b>[{cat}]</b> {yr}년 {mn}월 자료</span>
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