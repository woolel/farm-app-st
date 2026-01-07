
from datetime import datetime

# === LOGIC TO TEST ===
def get_priority(item, current_date):
    _id, _year, _cat, _content = item
    
    # 1. 날짜 매칭 요약정보 (최우선)
    if '요약' in _cat:
        try:
            # ID 포맷: YYYY-MM-DD_YYYY-MM-DD
            dates = _id.split('_')
            if len(dates) == 2:
                start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
                end_dt = datetime.strptime(dates[1], "%Y-%m-%d")
                
                # 데이터의 연도에 맞는 '이번 글'의 타겟 날짜 생성
                target_year = int(_year)
                # 현재 조회중인 날짜(current_date)의 월/일을 가져옴
                check_date = datetime(target_year, current_date.month, current_date.day)
                
                print(f"Checking ID {_id}: {start_dt.date()} <= {check_date.date()} <= {end_dt.date()}?")
                
                if start_dt <= check_date <= end_dt:
                    print("  -> MATCH!")
                    return 0 # 날짜 딱 맞는 주간 요약
                else:
                    print("  -> No match")
        except Exception as e:
            print(f"Error: {e}")
            pass
        return 1 # 날짜 안 맞아도 요약이면 차순위
        
    if '기상' in _cat or '농업' in _cat: return 2
    return 99

def filter_content(content):
    clean_text = content.replace('\n', '').replace('|', '').replace('-', '').strip()
    if len(clean_text) < 30: 
        if '###' in content:
            return False # Filter out
    return True # Keep

# === TEST DATA ===
# Mocking today as Jan 7
today = datetime(2025, 1, 7) 

test_items = [
    # 1. Correct Summary for Jan 7 (Start: Jan 6, End: Jan 12)
    ("2025-01-06_2025-01-12", "2025", "요약", "### 요약\n내용이 충분히 김..."),
    # 2. Wrong Summary (Start: Jan 13)
    ("2025-01-13_2025-01-19", "2025", "요약", "### 요약\n다른 주간..."),
    # 3. Weather (Prio 2)
    ("2025-01-06_2025-01-12", "2025", "기상", "기상 정보입니다."),
    # 4. Empty Header to filter
    ("2025-01-06_2025-01-12", "2025", "벼", "### 제1장 벼"),
]

print("=== PRIORITY TEST ===")
sorted_items = sorted(test_items, key=lambda x: get_priority(x, today))
for item in sorted_items:
    prio = get_priority(item, today)
    print(f"Prio {prio}: {item[2]} ({item[0]})")

print("\n=== FILTER TEST ===")
for item in test_items:
    keep = filter_content(item[3])
    print(f"'{item[3]}' -> Keep? {keep}")
