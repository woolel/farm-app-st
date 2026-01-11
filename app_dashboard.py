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
        con = duckdb.connect('farming_granular.duckdb', read_only=False) # FTS 생성을 위해 Write 모드 필요할 수 있음
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("INSTALL fts; LOAD fts;")
        
        # FTS 인덱스 확인 및 생성
        schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
        fts_status = "ok"
        if not any('fts_main_farm_info' in str(row) for row in schemas):
            try:
                # PK(id)가 존재하므로 이를 이용해 인덱스 생성
                con.execute("PRAGMA create_fts_index('farm_info', 'id', 'content_md', 'title', 'tags_crop');")
            except Exception as e:
                fts_status = "fts_missing"
            
    except Exception as e:
        return None, None, f"db_error: {e}"
        
    return model, con, fts_status

@st.cache_data(ttl=3600)
def get_monthly_trends(month, _con):
    try:
        # 태그별 통계 (unnest 사용)
        sql = """
            SELECT unnest(tags_crop) as category, count(*) as cnt
            FROM farm_info
            WHERE month = ?
            GROUP BY category
            ORDER BY cnt DESC
            LIMIT 10
        """
        rows = _con.execute(sql, [month]).fetchall()
        if not rows:
            return []
        return rows
    except:
        return []

@st.cache_data(ttl=3600)
def get_week_list(year, month, _con):
    """특정 연도/월의 주간 정보(주차 문자열) 목록 조회"""
    try:
        # title에서 [YYYY-MM-DD~YYYY-MM-DD] 패턴 추출
        sql = """
            SELECT DISTINCT regexp_extract(title, '\[(.*?)\]', 1) as week_range 
            FROM farm_info 
            WHERE year = ? AND month = ? 
            AND week_range IS NOT NULL
            ORDER BY week_range
        """
        return [row[0] for row in _con.execute(sql, [int(year), int(month)]).fetchall() if row[0]]
    except:
        return []

@st.cache_data(ttl=3600)
def get_all_categories(_con):
    """DB에 존재하는 모든 작목 태그 조회"""
    try:
        sql = "SELECT DISTINCT unnest(tags_crop) FROM farm_info ORDER BY 1"
        rows = _con.execute(sql).fetchall()
        return [r[0] for r in rows if r[0]]
    except:
        return ['벼', '밭작물', '채소', '과수', '특용작물', '축산', '양봉'] # Fallback

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
            
            # 테이블 시작 조건: (1)구분선이 있거나, (2)구분선이 없지만 파이프가 2개 이상일 때
            if is_table_start or (clean_line.count('|') >= 2):
                
                # 테이블 앞 빈 줄 확보
                if output and output[-1].strip():
                    output.append("")
                
                # 테이블 블록 수집 시작
                table_lines = []
                table_lines.append(clean_line) # 헤더 추가
                
                if not is_table_start:
                    # 구분선 강제 생성 (없을 경우)
                    col_count = clean_line.count('|') - 1
                    if col_count < 1: col_count = 1
                    separator = "|" + " --- |" * col_count
                    table_lines.append(separator)
                else:
                    # 구분선이 있으면 다음 줄(구분선)도 추가하고 인덱스 증가
                    table_lines.append(lines[i+1].strip())
                    i += 1
                
                # 이어지는 테이블 행 수집 (빈 줄 무시하고 합침)
                i += 1
                crossed_blank = False # 빈 줄을 건너뛰었는지 여부 체크
                
                while i < len(lines):
                    next_content_line = lines[i].strip()
                    
                    if not next_content_line:
                        # 빈 줄 발견 -> 플래그 세우고 계속 진행
                        crossed_blank = True
                        i += 1
                        continue
                    
                    # 내용이 있는 줄
                    is_table_row = False
                    
                    # 1. 빈 줄을 건너뛴 후라면 -> 반드시 '|'로 시작해야 테이블로 인정 (엄격)
                    if crossed_blank:
                        if next_content_line.startswith('|'):
                            is_table_row = True
                    # 2. 연속된 줄이라면 -> '|'가 포함되기만 해도 인정 (관대)
                    else:
                        if '|' in next_content_line:
                            is_table_row = True
                            
                    if is_table_row:
                        table_lines.append(next_content_line)
                        crossed_blank = False # 유효 행 찾았으므로 플래그 초기화
                        i += 1
                    else:
                        # 테이블 아님 -> 종료
                        break
                
                # 수집된 테이블 전체 출력
                output.extend(table_lines)
                continue

        # 테이블이 아닌 일반 라인
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
# 과거 기록 데이터 조회 및 섹션 구성
with st.container(border=True):
    # Farm Info 테이블 조회 (content_md, tags_crop 등)
    history_sql = """
        SELECT regexp_extract(title, '\[(.*?)\]', 1) as week_range, year, tags_crop, content_md, title 
        FROM farm_info 
        WHERE month = ? 
        AND content_md NOT LIKE '%목 차%' 
        ORDER BY year DESC, week_range DESC
    """
    try:
        # 아카이브로 특정 주간을 선택한 경우 해당 데이터만 조회
        if st.session_state.selected_week_id:
            # selected_week_id는 '2023-01-01~2023-01-07' 형태
            rows = con.execute("""
                SELECT regexp_extract(title, '\[(.*?)\]', 1) as week_range, year, tags_crop, content_md, title 
                FROM farm_info 
                WHERE title LIKE ?
            """, [f'%{st.session_state.selected_week_id}%']).fetchall()
            valid_items = rows
        else:
            rows = con.execute(history_sql, [current_month]).fetchall()
            valid_items = []
            
            for r in rows:
                w_range, ryear, rtags, rcontent, rtitle = r
                if not w_range: continue
                
                try:
                    start_str, end_str = w_range.split('~')
                    # 과거 연도의 날짜를 현재 연도로 치환하여 비교
                    s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(year=today.year)
                    e_date = datetime.strptime(end_str, "%Y-%m-%d").replace(year=today.year)
                    
                    # 현재 날짜가 해당 주간 범위 내(혹은 근사)에 있는지 확인
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
                
                # 정렬: '요약' 또는 '요 약'이 포함된 항목을 최상단으로
                sorted_items = sorted(grouped[y], key=lambda x: (
                    0 if '요약' in x[4] or '요 약' in x[4] else 1, 
                    x[4] # title
                ))
                
                for idx, item in enumerate(sorted_items[:4]): 
                    w_range, ryear, rtags, rcontent, rtitle = item
                    
                    # 제목에서 날짜([]) 제거하고 깨끗하게 보여주기
                    clean_title = rtitle.split(']')[-1].strip() if ']' in rtitle else rtitle
                    
                    # 태그 표시는 제거 (사용자 요청)
                    display_text = clean_title
                    
                    with cols[idx % 2]:
                        with st.popover(display_text, use_container_width=True):
                            st.markdown(format_content(rcontent), unsafe_allow_html=True)
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
    # DB에서 동적으로 태그 가져오기
    available_tags = get_all_categories(con)
    with st.popover("🔍 작목 선택", use_container_width=True):
        selected_cats = st.multiselect(
            "필터링할 작목:",
            available_tags,
            default=available_tags[:2] if available_tags else []
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
        # list_has_any (하나라도 포함되면 매칭)
        # duckdb list query: list_contains(tags_crop, 'ITEM') ... OR ...
        # 간단하게: array filtering
        # 그러나 SQL 파라미터 바인딩이 복잡하므로 문자열 포맷팅 사용 (주의)
        # category IN (...) 대신 list logic
        # OR logic: list_has_any(tags_crop, [selected...]) -> list_has_any는 최신 duckdb 필요할수도
        # 안전하게 unnest 후 IN
        pass 
        
        # NOTE: DuckDB Python client passing list for IN clause is tricky with arrays
        # Constructing dynamic WHERE clause
        # WHERE len(list_filter(tags_crop, x -> x IN (...))) > 0
        cat_list_str = ", ".join([f"'{c}'" for c in selected_cats])
        cat_filter_sql = f"AND len(list_filter(tags_crop, x -> x IN ({cat_list_str}))) > 0"

    with st.spinner("AI가 문서를 분석 중입니다..."):
        # 1. 검색어 정규화 (FTS용: 명사/동사/숫자만 추출)
        clean_query = extract_keywords(query_input)
        
        # 2. 임베딩 생성 (Vector용: 문맥 유지를 위해 원본 문장 사용)
        # SBERT 모델은 문장 전체의 의미를 파악하는에 유리함
        query_vector = model.encode(query_input).tolist()
        
        # 하이브리드 검색 SQL (Semantic 1.5배 + FTS 0.5배 가중치 결합)
        # farm_info 테이블 사용
        # [수정] '요약'이 포함된 제목은 상세 정보 파악에 방해되므로 제외
        search_sql = f"""
        SELECT 
            vector_score,
            fts_score,
            tags_crop, year, month, content_md, title
        FROM (
            SELECT 
                array_cosine_similarity(embedding, ?::FLOAT[768]) AS vector_score,
                fts_main_farm_info.match_bm25(id, ?) AS fts_score,
                tags_crop, year, month, content_md, title
            FROM farm_info
            WHERE 1=1 
                {cat_filter_sql} 
                AND title NOT LIKE '%요약%' 
                AND title NOT LIKE '%요 약%'
        ) sub
        WHERE vector_score > 0.40
        ORDER BY (vector_score * 1.5 + fts_score * 0.5) DESC
        LIMIT 5
        """
        
        try:
            # FTS에는 키워드만 전달하여 정확도 향상
            results = con.execute(search_sql, [query_vector, clean_query]).fetchall()
            
            if not results:
                st.markdown(f"""
                    <div style="padding:15px; border-radius:5px; background-color:#fff3cd; color:#856404; border:1px solid #ffeeba;">
                        {material_icon('sentiment_dissatisfied', color='#fbbc04')} 검색 결과가 없습니다. 질문을 구체적으로 바꾸거나 필터를 해제해보세요.
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.success(f"총 {len(results)}건의 관련 정보를 찾았습니다.")
                
                for row in results:
                    v_score, f_score, tags, yr, mn, body, rtitle = row
                    
                    # [핵심 수정] NoneType 에러 방지용 안전장치
                    if v_score is None: v_score = 0.0
                    if f_score is None: f_score = 0.0
                    
                    # 뱃지 로직
                    badge_color = "#34a853" if v_score > 0.65 else "#fbbc04"
                    match_type = "AI+키워드" if f_score > 0 else "AI추론"
                    
                    # 태그 표시
                    cat_display = ""
                    if tags:
                        cat_display = " ".join([f"<b>[{t}]</b>" for t in tags[:3]]) + " "
                    elif "기상" in rtitle:
                        cat_display = "<b>[기상]</b> "

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