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

st.markdown("""
    <style>
    .big-font { font-size:18px !important; }
    .stExpander p { font-size: 16px; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
    
    /* 테이블 스타일 보정 */
    table { width: 100% !important; border-collapse: collapse !important; }
    th, td { padding: 8px !important; border: 1px solid #ddd !important; text-align: left !important; }
    th { background-color: #f9f9f9 !important; font-weight: bold; }
    
    /* 하이라이트 스타일 */
    .highlight { background-color: #fff9c4; padding: 2px 4px; border-radius: 4px; font-weight: bold; }
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
        # 'farming' 테이블에 대한 FTS 인덱스 스키마 이름은 보통 'fts_main_farming'
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

# 리소스 초기화
model, con, status = load_resources()

# 에러 핸들링
if isinstance(status, str) and "error" in status:
    st.error(f"시스템 초기화 오류: {status}")
    st.stop()

if status == "file_not_found":
    st.error("❌ 'farming_granular.duckdb' 데이터베이스 파일이 없습니다. 실행 경로를 확인해주세요.")
    st.stop()

if status == "fts_missing":
    st.warning("⚠️ 검색 인덱스(FTS)가 감지되지 않아 키워드 검색 성능이 저하될 수 있습니다.")

# ==========================================
# 3. 유틸리티 함수
# ==========================================
def format_content(text):
    """마크다운 렌더링을 위한 텍스트 전처리"""
    if not text: return ""
    text = text.replace('~', r'\~') # 취소선 방지
    # 표가 문장에 붙어 나올 때 강제 줄바꿈
    text = text.replace('.|', '.\n|').replace(':|', ':\n|')
    text = text.replace('|', ' | ') # 파이프 간격 조정
    return text

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
    
    # 추천 키워드 로직
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
    
    # 세션 상태 초기화
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    # 버튼 클릭 시 검색어 입력창에 값 주입
    cols = st.columns(2)
    for i, tag in enumerate(recommendations):
        if cols[i % 2].button(f"#{tag}", key=f"btn_{tag}", use_container_width=True):
            st.session_state.search_query = tag

    # 트렌드 위젯
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
    # SQL: 날짜 매칭을 위해 ID도 가져옴
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
        
        # Python 레벨에서 날짜 정밀 필터링 (오늘 날짜와 가까운 주차만 선별)
        valid_items = []
        seen_contents = set()

        for r in rows:
            rid, ryear, rcat, rcontent = r
            
            # 내용 중복 제거 (약간의 전처리 후 해시 비교)
            content_sig = re.sub(r'\s+', '', rcontent)[:50]
            if content_sig in seen_contents: continue
            seen_contents.add(content_sig)

            # 날짜 파싱 (ID: YYYY-MM-DD~YYYY-MM-DD)
            try:
                start_str, end_str = rid.split('~')
                s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(year=today.year)
                e_date = datetime.strptime(end_str, "%Y-%m-%d").replace(year=today.year)
                
                # 오늘 날짜가 기간 내에 있거나, 앞뒤 3일 이내인 경우
                # (연도 무시하고 월/일만 비교하기 위해 year를 통일)
                target_date = today
                
                # 기간 겹침 확인
                if s_date <= target_date <= e_date:
                    is_match = True
                else:
                    # 근접 날짜 확인 (오차 3일 허용)
                    days_diff = min(abs((target_date - s_date).days), abs((target_date - e_date).days))
                    is_match = days_diff <= 3
                
                if is_match:
                    valid_items.append(r)
            except:
                continue # 날짜 형식이 안 맞으면 패스

        if valid_items:
            # 연도별 그룹화
            grouped = {}
            for item in valid_items:
                y = item[1]
                if y not in grouped: grouped[y] = []
                grouped[y].append(item)
            
            # 최신 연도순 출력 (최대 3개 연도)
            for y in sorted(grouped.keys(), reverse=True)[:3]:
                st.markdown(f"**📌 {y}년 기록**")
                cols = st.columns(2)
                # 연도별 최대 4개 항목만 노출
                for idx, item in enumerate(grouped[y][:4]):
                    cat, content = item[2], item[3]
                    short_content = content.split('\n')[0][:30] + "..."
                    with cols[idx % 2]:
                        with st.popover(f"[{cat}] {short_content}"):
                            st.markdown(format_content(content))
        else:
            st.info("이맘때와 정확히 일치하는 과거 주간 정보가 없습니다.")
            
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {e}")

st.divider()

# ==========================================
# 6. 시맨틱 하이브리드 검색
# ==========================================
st.header("🔍 농업 지식 검색")

# 검색 폼 (Enter 키 리로드 방지 및 UX 개선)
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
    # 1. 카테고리 필터 SQL 생성
    cat_filter_sql = ""
    if selected_cats:
        cat_list_str = "', '".join(selected_cats)
        cat_filter_sql = f"AND category IN ('{cat_list_str}')"

    # 2. 임베딩 생성
    with st.spinner("AI가 문서를 분석 중입니다..."):
        query_vector = model.encode(query_input).tolist()
        
        # 3. 하이브리드 검색 쿼리 (점수 로직 개선)
        # score (Vector): 0.0 ~ 1.0
        # fts_score (BM25): 0.0 ~ N (보통 10~50 사이가 나옴)
        # -> 벡터 유사도 0.5 이상인 것 중에서, 키워드 매칭 점수를 로그 스케일로 더해서 정렬
        search_sql = f"""
        SELECT 
            score, 
            fts_main_farming.match_bm25(pk, ?) as fts_score,
            category, year, month, content
        FROM (
            SELECT 
                array_cosine_similarity(embedding, ?::FLOAT[768]) AS score,
                pk, category, year, month, content
            FROM farming
            WHERE 1=1 {cat_filter_sql}
        ) 
        WHERE score > 0.45 -- 최소 관련성 필터
        ORDER BY (score * 10 + ln(fts_score + 1)) DESC
        LIMIT 5
        """
        
        try:
            results = con.execute(search_sql, [query_input, query_vector]).fetchall()
            
            if not results:
                st.warning("🤔 검색 결과가 없습니다. 질문을 구체적으로 바꾸거나 필터를 해제해보세요.")
            else:
                st.success(f"총 {len(results)}건의 관련 정보를 찾았습니다.")
                
                for row in results:
                    v_score, f_score, cat, yr, mn, body = row
                    
                    # 관련도 배지 표시
                    badge_color = "green" if v_score > 0.65 else "orange"
                    match_type = "AI+키워드" if f_score > 0 else "AI추론"
                    
                    with st.container(border=True):
                        st.markdown(f"""
                        <div style='display:flex; justify-content:space-between; align-items:center;'>
                            <span class='big-font'><b>[{cat}]</b> {yr}년 {mn}월 자료</span>
                            <span style='color:{badge_color}; font-size:0.8em;'>
                                유사도 {v_score:.2f} ({match_type})
                            </span>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # 검색어 하이라이트 (단순 문자열 치환)
                        highlighted_body = format_content(body)
                        # 원본 쿼리 단어들로 하이라이팅 시도
                        for word in query_input.split():
                            if len(word) > 1:
                                highlighted_body = highlighted_body.replace(word, f"<span class='highlight'>{word}</span>")
                        
                        st.markdown(highlighted_body, unsafe_allow_html=True)
                        
        except Exception as e:
            st.error(f"검색 처리 중 오류가 발생했습니다: {e}")
            # 디버깅용: st.write(e)

# ==========================================
# 7. 푸터
# ==========================================
st.markdown("---")
st.markdown("<div style='text-align:center; color:gray; font-size:0.8em;'>데이터 출처: 농촌진흥청 주간농사정보 | Powered by DuckDB & Streamlit</div>", unsafe_allow_html=True)