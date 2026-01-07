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

# CSS 스타일 커스텀 (폰트 및 테이블 스타일 강제 적용)
st.markdown("""
    <style>
    /* 1. 한글 폰트 강제 적용 (깨짐 방지 보완) */
    html, body, [class*="css"] {
        font-family: "Pretendard", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif !important;
    }

    /* 2. 텍스트 크기 조정 */
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; line-height: 1.6; }
    
    /* 3. 테이블 스타일 (깨짐 방지 및 가독성) */
    table { 
        width: 100% !important; 
        border-collapse: collapse !important; 
        margin-bottom: 1rem !important; 
        display: block; /* 가로 스크롤 허용 */
        overflow-x: auto;
    }
    th, td { 
        padding: 8px 12px !important; 
        border: 1px solid #ddd !important; 
        text-align: left !important; 
        font-size: 15px !important; 
        white-space: pre-wrap; /* 줄바꿈 허용 */
    }
    th { 
        background-color: #f8f9fa !important; 
        font-weight: bold; 
        color: #333;
    }
    
    /* 4. 검색어 하이라이트 스타일 */
    .highlight { 
        background-color: #fff9c4; 
        padding: 2px 4px; 
        border-radius: 4px; 
        font-weight: bold; 
        color: #d32f2f;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    # 모델 로드 (로컬 경로 우선, 없으면 HuggingFace 다운로드)
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'AI 모델 및 데이터베이스 로딩 중... ({model_path})'):
        try:
            model = SentenceTransformer(model_path)
        except Exception as e:
            return None, None, f"model_error: {e}"
    
    if not os.path.exists('farming_granular.duckdb'):
        return None, None, "file_not_found"
        
    try:
        # read_only=True로 설정하여 동시성 문제 예방
        con = duckdb.connect('farming_granular.duckdb', read_only=True)
        
        # 확장 기능 로드
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("INSTALL fts; LOAD fts;")
        
        # FTS 인덱스 상태 확인
        schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
        fts_status = "ok"
        if not any('fts_main_farming' in str(row) for row in schemas):
            fts_status = "fts_missing"
            
    except Exception as e:
        return None, None, f"db_error: {e}"
        
    return model, con, fts_status

# 데이터 조회 함수 (캐싱)
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

# 리소스 초기화 실행
model, con, status = load_resources()

# 초기화 에러 핸들링
if isinstance(status, str) and "error" in status:
    st.error(f"시스템 초기화 오류: {status}")
    st.stop()

if status == "file_not_found":
    st.error("❌ 'farming_granular.duckdb' 데이터베이스 파일이 없습니다. DB 생성 코드를 먼저 실행해주세요.")
    st.stop()

if status == "fts_missing":
    st.warning("⚠️ 검색 인덱스(FTS)가 감지되지 않아 키워드 검색 성능이 저하될 수 있습니다.")

# ==========================================
# 3. 유틸리티 함수 (테이블 깨짐 수정 로직)
# ==========================================
def format_content(text):
    """
    마크다운 렌더링을 위한 텍스트 전처리
    - 테이블 구조가 깨지지 않도록 줄바꿈 및 파이프(|) 보정
    """
    if not text: return ""
    
    # 1. 취소선 방지 (~ -> \~)
    text = text.replace('~', r'\~') 
    
    # 2. 테이블 감지 및 포맷팅 강화
    lines = text.split('\n')
    formatted_lines = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # 테이블 행으로 추정되는 경우 (파이프가 있고 길이가 충분함)
        if '|' in line and len(line) > 3:
            # 파이프 앞뒤에 공백 강제 추가 (마크다운 파서 인식 도움)
            # 기존 파이프를 ' | '로 치환하되, 중복 공백은 정리
            processed_line = line.replace('|', ' | ')
            processed_line = re.sub(r'\s+\|\s+', ' | ', processed_line) 
            
            # 테이블의 시작이거나(헤더), 이전 줄이 일반 텍스트였다면 빈 줄 추가하여 분리
            if i > 0 and '|' not in lines[i-1]:
                formatted_lines.append("") 
            
            formatted_lines.append(processed_line)
        else:
            # 일반 텍스트
            formatted_lines.append(line)
            
    return '\n'.join(formatted_lines)

# ==========================================
# 4. 사이드바 UI
# ==========================================
today = datetime.now()
current_month = today.month

with st.sidebar:
    st.title("🚜 스마트 농업 봇")
    st.info(f"오늘 날짜: {today.year}년 {today.month}월 {today.day}일")
    
    st.markdown("### 🏷️ 관심 분야 설정")
    selected_cats = st.multiselect(
        "필터링할 작목/분야:",
        ['기상', '벼', '밭작물', '채소', '과수', '특용작물', '축산', '양봉'],
        default=['기상', '과수']
    )
    
    st.divider()
    
    # 월별 추천 키워드
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
            
    st.markdown(f"### 💡 {current_month}월 추천 검색어")
    
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    cols = st.columns(2)
    for i, tag in enumerate(recommendations):
        # 버튼 클릭 시 세션 스테이트에 검색어 저장
        if cols[i % 2].button(f"#{tag}", key=f"btn_{tag}", use_container_width=True):
            st.session_state.search_query = tag

    st.divider()
    st.markdown("📊 **이달의 데이터 분포**")
    trends = get_monthly_trends(current_month, con)
    if trends:
        trend_df = {row[0]: row[1] for row in trends[:5]}
        st.bar_chart(trend_df, height=150)
    else:
        st.caption("데이터 집계 중...")

# ==========================================
# 5. 메인: 과거 데이터 (History)
# ==========================================
st.subheader(f"📅 {current_month}월의 과거 농사 기록 (최근 3년)")

with st.expander("🔻 지난 3년간 오늘 이맘때의 주요 정보 보기", expanded=True):
    # SQL: 기본적인 월별 데이터 조회
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
            
            # 내용 중복 제거 (공백 제거 후 앞부분 비교)
            content_sig = re.sub(r'\s+', '', rcontent)[:50]
            if content_sig in seen_contents: continue
            seen_contents.add(content_sig)

            # 날짜 정밀 비교 (오늘 날짜 기준 ±3일 포함 여부)
            try:
                start_str, end_str = rid.split('~')
                # 연도는 무시하고 월/일 비교를 위해 현재 연도로 치환
                s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(year=today.year)
                e_date = datetime.strptime(end_str, "%Y-%m-%d").replace(year=today.year)
                
                target_date = today
                
                # 기간 내 포함되거나, 기간과 3일 이내로 가까운지 확인
                if s_date <= target_date <= e_date:
                    is_match = True
                else:
                    days_diff = min(abs((target_date - s_date).days), abs((target_date - e_date).days))
                    is_match = days_diff <= 3
                
                if is_match:
                    valid_items.append(r)
            except:
                continue # 날짜 포맷 에러 시 스킵

        if valid_items:
            # 연도별로 그룹화하여 출력
            grouped = {}
            for item in valid_items:
                y = item[1]
                if y not in grouped: grouped[y] = []
                grouped[y].append(item)
            
            # 최신 연도순, 최대 3개 연도만 표시
            for y in sorted(grouped.keys(), reverse=True)[:3]:
                st.markdown(f"**📌 {y}년 기록**")
                cols = st.columns(2)
                for idx, item in enumerate(grouped[y][:4]): # 연도별 최대 4개
                    cat, content = item[2], item[3]
                    short_content = content.split('\n')[0][:30] + "..."
                    
                    with cols[idx % 2]:
                        with st.popover(f"[{cat}] {short_content}"):
                            # [핵심] 테이블 깨짐 방지 함수 적용
                            st.markdown(format_content(content), unsafe_allow_html=True)
        else:
            st.info("이맘때와 정확히 일치하는 과거 주간 정보가 없습니다.")
            
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")

st.divider()

# ==========================================
# 6. 시맨틱 하이브리드 검색 (오류 수정됨)
# ==========================================
st.header("🔍 농업 지식 검색")

# 검색 폼 (엔터 키 리로드 방지)
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
        search_btn = st.form_submit_button("검색 🚀", use_container_width=True)

if search_btn and query_input:
    # 카테고리 필터 SQL 생성
    cat_filter_sql = ""
    if selected_cats:
        cat_list_str = "', '".join(selected_cats)
        cat_filter_sql = f"AND category IN ('{cat_list_str}')"

    with st.spinner("AI가 문서를 분석 중입니다..."):
        # 질문 임베딩 생성
        query_vector = model.encode(query_input).tolist()
        
        # ------------------------------------------------------------------
        # [수정된 SQL] Binder Error 해결을 위한 Nested Query 구조
        # 안쪽(sub)에서 점수를 계산하고, 바깥쪽에서 정렬(ORDER BY)합니다.
        # ------------------------------------------------------------------
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
        WHERE vector_score > 0.45 -- 최소 관련성 필터
        ORDER BY (vector_score * 10 + ln(fts_score + 1)) DESC
        LIMIT 5
        """
        
        try:
            results = con.execute(search_sql, [query_vector, query_input]).fetchall()
            
            if not results:
                st.warning("🤔 검색 결과가 없습니다. 질문을 구체적으로 바꾸거나 필터를 해제해보세요.")
            else:
                st.success(f"총 {len(results)}건의 관련 정보를 찾았습니다.")
                
                for row in results:
                    v_score, f_score, cat, yr, mn, body = row
                    
                    # 뱃지 색상 및 타입 결정
                    badge_color = "#4CAF50" if v_score > 0.65 else "#FF9800" # Green vs Orange
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
                        
                        # 내용 포맷팅 및 하이라이트
                        highlighted_body = format_content(body)
                        for word in query_input.split():
                            if len(word) > 1:
                                highlighted_body = highlighted_body.replace(word, f"<span class='highlight'>{word}</span>")
                        
                        st.markdown(highlighted_body, unsafe_allow_html=True)
                        
        except Exception as e:
            st.error(f"검색 처리 중 오류가 발생했습니다: {e}")

# ==========================================
# 7. 푸터
# ==========================================
st.markdown("---")
st.markdown("<div style='text-align:center; color:gray; font-size:0.8em;'>데이터 출처: 농촌진흥청 주간농사정보 | Powered by DuckDB & Streamlit</div>", unsafe_allow_html=True)