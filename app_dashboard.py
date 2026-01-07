import streamlit as st
import duckdb
from sentence_transformers import SentenceTransformer
from datetime import datetime
import os
import re
import pandas as pd

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
    
    /* 표 스타일 강제 적용 */
    table {
        width: 100% !important;
        border-collapse: collapse !important;
    }
    th, td {
        padding: 8px !important;
        border: 1px solid #ddd !important;
        text-align: left !important;
    }
    th {
        background-color: #f2f2f2 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. 리소스 로드 (캐싱 적용)
# ==========================================
@st.cache_resource
def load_resources():
    model_path = './local_model' if os.path.exists('./local_model') else 'jhgan/ko-sroberta-multitask'
    
    with st.spinner(f'시스템 로딩 중... ({model_path})'):
        model = SentenceTransformer(model_path)
    
    if not os.path.exists('farming_granular.duckdb'):
        return None, None, "file_not_found"
        
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    fts_status = "ok"

    try:
        con.execute("INSTALL vss; LOAD vss;")
        con.execute("INSTALL fts; LOAD fts;")
        
        # FTS 인덱스 존재 여부 확인 (진단용 - 독립된 try-except로 감쌈)
        try:
            # FTS 인덱스는 별도의 스키마(fts_main_farming)로 생성되므로 스키마 목록을 확인
            schemas = con.execute("SELECT schema_name FROM duckdb_schemas;").fetchall()
            fts_exists = any('fts_main_farming' in str(row) for row in schemas)
            if not fts_exists:
                fts_status = "fts_missing"
        except Exception:
            pass # 진단 쿼리 자체가 실패할 경우 앱 실행을 방해하지 않음
            
    except Exception as e:
        st.warning(f"DuckDB 확장 로드 실패 (검색 기능이 제한될 수 있음): {e}")
        
    return model, con, fts_status

# [극대화 3] 데이터 조회 유틸리티 (캐싱 적용)
@st.cache_data(ttl=3600)
def get_monthly_trends(month, _con):
    """
    현재 월의 주요 키워드 트렌드 분석 (SQL 집계)
    """
    sql = """
        SELECT category, count(*) as cnt
        FROM farming
        WHERE month = ?
        GROUP BY category
        ORDER BY cnt DESC
    """
    return _con.execute(sql, [month]).fetchall()

model, con, db_status = load_resources()

if db_status == "file_not_found":
    st.error("❌ 'farming_granular.duckdb' 파일이 없습니다.")
    st.stop()

if db_status == "fts_missing":
    st.error("⚠️ 데이터베이스에 FTS 인덱스가 감지되지 않습니다. (최신 DB가 적용되지 않았을 수 있습니다)")
    if st.button("🔄 데이터베이스 연결 새로고침 (캐시 삭제)"):
        st.cache_resource.clear()
        st.rerun()

if con is None:
    st.error("❌ 데이터베이스 연결에 실패했습니다.")
    st.stop()

# ==========================================
# 3. 유틸리티 함수
# ==========================================
def format_content(text):
    r"""
    1. 취소선 방지: ~ -> \~
    2. 표 깨짐 방지: 표 앞뒤/행간 줄바꿈 보강 (안전한 방식)
    """
    if not text: return ""
    
    # 1. 취소선 방지
    text = text.replace('~', r'\~')
    
    # 2. 텍스트와 표가 붙어있는 경우 (안전한 패턴만 적용)
    # 문장 끝(.)이나 괄호 끝()) 뒤에 파이프가 오면 줄바꿈
    text = text.replace('.|', '.\n|').replace('. |', '.\n|')
    text = text.replace(')|', ')\n|').replace(') |', ')\n|')
    
    # 3. 표의 헤더와 구분선이 붙어있는 경우 강제 줄바꿈
    # 예: |제목||---| -> |제목|\n|---|
    # 구분선 패턴: |-...| 또는 |:...|
    text = re.sub(r'\|\s*(\|\s*:?-+:?\s*\|)', r'|\n\1', text)
    
    # 4. 일반적인 행 분리 (파이프 간격 확보)
    text = text.replace('|', ' | ') 
    
    return f"\n{text}\n"

def render_weather_chart(content):
    """
    마크다운 표 데이터를 추출하여 그래프로 시각화
    """
    try:
        lines = content.split('\n')
        table_lines = [l.strip() for l in lines if '|' in l]
        if len(table_lines) < 3: return # 헤더, 구분선, 최소 1행 필요
        
        # 구분선 인덱스 찾기
        sep_idx = -1
        for i, line in enumerate(table_lines):
            if '---' in line:
                sep_idx = i
                break
        
        if sep_idx <= 0: return
        
        # 헤더와 데이터 분리
        header_line = table_lines[sep_idx - 1]
        headers = [h.strip() for h in header_line.split('|') if h.strip()]
        
        # 기상관련 표인지 확인 (공백 제거 후 비교)
        headers_combined = "".join(headers).replace(" ", "")
        if not any(k in headers_combined for k in ['기온', '강수', '온도', '습도']):
            return
            
        data = []
        for line in table_lines[sep_idx + 1:]:
            cols = [c.strip() for c in line.split('|') if c.strip()]
            if len(cols) >= 2:
                data.append(cols[:len(headers)])
        
        if not data: return
        
        df = pd.DataFrame(data, columns=headers[:len(data[0])])
        
        # 수치 추출 함수 (범위인 경우 평균값)
        def extract_num(text):
            # 괄호 안의 데이터(비율 등)는 우선 제거
            pure_text = re.sub(r'\(.*?\)', '', text)
            nums = re.findall(r"[-+]?\d*\.\d+|\d+", pure_text)
            if not nums: return None
            return sum(float(n) for n in nums) / len(nums)

        # 수치형 변환
        val_cols = []
        for col in headers[1:]: # 첫 컬럼은 대개 '구분'
            col_clean = col.replace(" ", "")
            if any(k in col_clean for k in ['기온', '강수', '온도', '습도']):
                df[f'{col}_val'] = df[col].apply(extract_num)
                val_cols.append(col)
        
        # 유효 데이터 확인
        df_plot = df.dropna(subset=[f'{c}_val' for c in val_cols])
        if df_plot.empty: return

        # 그래프 그리기
        st.markdown("---")
        st.caption("📊 **표 데이터를 기반으로 자동 생성된 그래프**")
        
        x_col = headers[0]
        
        # 탭을 사용하여 기온과 강수량 분리 시각화
        tab_names = [f"📈 {c}" for c in val_cols]
        tabs = st.tabs(tab_names)
        
        for i, col in enumerate(val_cols):
            with tabs[i]:
                if '기온' in col or '온도' in col:
                    st.area_chart(df_plot.set_index(x_col)[f'{col}_val'], color="#ff4b4b")
                else:
                    st.bar_chart(df_plot.set_index(x_col)[f'{col}_val'], color="#0068c9")
        st.markdown("---")
        
    except Exception as e:
        # 에러 발생 시 그래프만 생략하고 본문은 보여줌
        pass

# ==========================================
# 4. 사이드바
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
        tags = ["월동 관리", "한파 대비", "전정", "화재 예방"]
    elif current_month in [3, 4, 5]:
        tags = ["파종 준비", "못자리", "봄벌 깨우기", "냉해 예방"]
    elif current_month in [6, 7, 8]:
        tags = ["장마 대비", "탄저병", "응애 방제", "배수로"]
    else: 
        tags = ["수확 시기", "건조 관리", "가을 걷이", "월동 준비"]

    if 'search_query' not in st.session_state:
        st.session_state.search_query = ""

    for tag in tags:
        if st.button(f"#{tag}", use_container_width=True):
            st.session_state.search_query = tag

# ==========================================
# 5. 메인 화면: 오늘의 농사 브리핑
# ==========================================
st.title(f"📅 {current_month}월 {today.day}일, 농사 브리핑")

# [극대화 4] 이달의 트렌드 분석 (SQL 집계 활용)
with st.sidebar:
    st.divider()
    st.markdown(f"### 📈 {current_month}월 데이터 트렌드")
    trends = get_monthly_trends(current_month, con)
    if trends:
        for cat, count in trends[:5]:
            st.caption(f"**{cat}**: {count}건의 정보")
    st.divider()

with st.container():
    st.markdown("### 🌤️ 지난 3년, 오늘 이맘때 핵심 정보")
    
    # [SQL 수정] id 추가하여 주간 범위 파악
    history_sql = f"""
        SELECT id, year, category, content 
        FROM farming 
        WHERE month = ? 
        -- 기본적인 노이즈만 제거 (목차 점선, 명시적 목차 단어)
        AND content NOT LIKE '%····%'
        AND content NOT LIKE '%목 차%'
        AND category NOT IN ('목차')
        ORDER BY year DESC
        LIMIT 150 
    """
    history_data = con.execute(history_sql, [current_month]).fetchall()
    
    if history_data:
        history_by_year = {}
        
        # [날짜 매칭 로직]
        # 모든 항목에 대해 '오늘 날짜'와 해당 데이터의 기간(ID)이 일치하는지 확인
        def get_date_score(item, current_date):
            _id, _year, _cat, _content = item
            try:
                dates = _id.split('_')
                if len(dates) == 2:
                    start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
                    end_dt = datetime.strptime(dates[1], "%Y-%m-%d")
                    
                    target_year = int(_year)
                    check_date = datetime(target_year, current_date.month, current_date.day)
                    
                    # 1. 정확히 포함 (0점)
                    if start_dt <= check_date <= end_dt:
                        return 0, 0
                    
                    # 2. 가까운 거리 (일수 차이)
                    diff_start = abs((check_date - start_dt).days)
                    diff_end = abs((check_date - end_dt).days)
                    min_diff = min(diff_start, diff_end)
                    
                    # 3. 7일 이내면 허용하되, 점수는 거리만큼 부여 (낮을수록 좋음)
                    if min_diff <= 7:
                        return 1, min_diff
            except:
                pass
            return 99, 99 # 매칭 실패

        for row in history_data:
            _, year, cat, content = row
            
            if year not in history_by_year:
                history_by_year[year] = []
            
            # [필터링 1] 목차/차례 제거 (공백 제거 후 확인)
            content_nospace = content.replace(' ', '').replace('\t', '')
            if '목차' in content_nospace or '차례' in content_nospace:
                continue

            # [필터링 2] 내용 없는 껍데기 & 헤더 제거
            clean_text = content_nospace.replace('\n', '').replace('|', '').replace('-', '').strip()
            
            # 헤더 필터
            if '###' in content:
                 # 제x장 패턴 + 짧음 -> 제거
                 if ('제' in content_nospace and '장' in content_nospace) and len(clean_text) < 60:
                     continue
                 # 그냥 너무 짧음 -> 제거
                 if len(clean_text) < 30:
                     continue
            
            # 내용 중복 제거
            if any(item['content'] == content for item in history_by_year[year]):
                continue

            score, distance = get_date_score(row, today)
            
            # [핵심 필터] 
            # 날짜가 매칭되지 않은(99점) 데이터는 과감히 숨김
            # -> "오늘 이맘때" 정보만 보여주기 위함
            if score >= 99:
                 continue

            history_by_year[year].append({'score': score, 'dist': distance, 'cat': cat, 'content': content})

        # 연도별 출력
        available_years = sorted(history_by_year.keys(), reverse=True)
        
        if not available_years:
             st.warning("표시할 유효한 데이터가 없습니다.")
        else:
            for i, year in enumerate(available_years):
                if i >= 3: break 
                
                items = history_by_year[year]
                if not items: continue

                # [중복 제거 - Best Pick]
                # 요약, 기상 등은 주간 단위로 하나만 나오므로, 점수가 가장 좋은 1개만 남김
                unique_categories = {} # cat -> item
                final_list = []
                
                # 점수(score) -> 거리(dist) 순 정렬
                items.sort(key=lambda x: (x['score'], x['dist']))
                
                for item in items:
                    c = item['cat']
                    # 요약, 기상은 연도별 1개만 (가장 날짜 잘 맞는 것)
                    if '요약' in c or '기상' in c or '농업' in c:
                        if c not in unique_categories:
                            unique_categories[c] = item
                            final_list.append(item)
                    else:
                        # 작목 정보는 여러 개일 수 있으나, 같은 작목 내에서는 가장 잘 맞는 것 1개만?
                        # 일단 작목은 다 보여주되 상위 N개 제한에 맡김
                        final_list.append(item)
                
                # 최종 정렬: 요약 -> 기상 -> 나머지
                def sort_key(x):
                    if '요약' in x['cat']: return 0
                    if '기상' in x['cat'] or '농업' in x['cat']: return 1
                    return 2
                
                final_list.sort(key=sort_key)
                
                # 상위 5개
                final_items = final_list[:5]
                
                st.markdown(f"#### 📆 {year}년 {current_month}월")

                for item in final_items:
                    category = item['cat']
                    full_content = item['content']
                    safe_content = format_content(full_content)
                    
                    clean_one_line = full_content.replace('\n', ' ').replace('|', ' ').strip()
                    preview_text = clean_one_line[:40] + "..." if len(clean_one_line) > 40 else clean_one_line
                    
                    if '기상' in category or '농업' in category: icon = "☁️"
                    elif '요약' in category: icon = "📝"
                    else: icon = "📌"

                    with st.expander(f"{icon} **[{category}]** {preview_text}", expanded=False):
                        # 기상 정보인 경우 그래프 시각화 시도
                        if '기상' in category:
                            render_weather_chart(full_content)
                        st.markdown(safe_content, unsafe_allow_html=True)
                
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

@st.cache_data(ttl=600) # 검색 결과 10분간 캐싱
def search_farming(query, category_filter, _model, _con):
    # 1. 질문 벡터화
    query_vector = _model.encode(query).tolist()
    
    # 2. 하이브리드 검색 SQL (VSS + FTS)
    # fts_main_farming.match_bm25를 사용하여 키워드 점수 합산
    # 시맨틱 유사도(score)와 키워드 점수를 결합
    sql = f"""
    SELECT 
        (0.7 * score + 0.3 * fts_score) as final_score,
        category, year, month, content
    FROM (
        SELECT 
            array_cosine_similarity(embedding, ?::FLOAT[768]) AS score,
            fts_main_farming.match_bm25(pk, ?) AS fts_score,
            *
        FROM farming
    ) 
    WHERE (score > 0.5 OR fts_score > 2.0)
    {category_filter}
    AND content NOT LIKE '%····%'
    AND content NOT LIKE '%목 차%'
    AND category NOT IN ('목차')
    ORDER BY final_score DESC 
    LIMIT 5;
    """
    return _con.execute(sql, [query_vector, query]).fetchall()

if query:
    category_filter = ""
    if selected_cats:
        cats_str = "', '".join(selected_cats)
        category_filter = f"AND category IN ('{cats_str}')"

    with st.spinner(f"AI와 엔진이 '{query}' 관련 최적의 정보를 찾는 중..."):
        results = search_farming(query, category_filter, model, con)

    if not results:
        st.warning("조건에 맞는 정보를 찾지 못했습니다.")
    else:
        for row in results:
            score, cat, year, mon, content = row
            score_badge = "🟢 높음" if score > 0.6 else "🟡 보통"
            safe_content = format_content(content)
            
            with st.container():
                st.markdown(f"#### [{cat}] {year}년 {mon}월 정보 <small>({score_badge})</small>", unsafe_allow_html=True)
                
                # 검색어 하이라이팅 (마크다운 충돌 방지 위해 단순화)
                st.markdown(f"💡 **관련 검색어:** {query}")
                
                # 기상 정보인 경우 그래프 시각화 시도
                if '기상' in cat:
                    render_weather_chart(content)
                    
                # [핵심 수정] st.info 대신 st.markdown 사용
                st.markdown(safe_content, unsafe_allow_html=True)
                st.caption("---")

# ==========================================
# 7. 푸터
# ==========================================
st.markdown("<br><div style='text-align: center; color: gray;'>데이터 출처: 농촌진흥청 주간농사정보 | Created with Streamlit & DuckDB</div>", unsafe_allow_html=True)