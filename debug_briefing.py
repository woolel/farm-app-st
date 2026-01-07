
import duckdb
from datetime import datetime

con = duckdb.connect('farming_granular.duckdb', read_only=True)
current_month = 1
# Mock today as Jan 7
today = datetime(2025, 1, 7)

print(f"=== Debugging for {today.date()} ===")

# Same SQL as app, but select ID
history_sql = f"""
    SELECT id, year, category, content 
    FROM farming 
    WHERE month = ? 
    ORDER BY year DESC
    LIMIT 150 
"""
history_data = con.execute(history_sql, [current_month]).fetchall()

print(f"Total rows fetched: {len(history_data)}")

history_by_year = {}

def get_priority(item, current_date):
    _id, _year, _cat, _content = item
    
    if '요약' in _cat:
        try:
            dates = _id.split('_')
            if len(dates) == 2:
                start_dt = datetime.strptime(dates[0], "%Y-%m-%d")
                end_dt = datetime.strptime(dates[1], "%Y-%m-%d")
                target_year = int(_year)
                # Check date using the Year of the Data?? 
                # Wait, if I want "This time of year", I should project 'Today' to 'Target Year' OR project 'Target Date' to 'Current Year'?
                # My logic was: check_date = datetime(target_year, current_date.month, current_date.day)
                # So for 2023 data, checking against Jan 7, 2023.
                # ID: 2023-01-09_2023-01-15.
                # Jan 7 is NOT in Jan 9-15.
                # So matching is strict!
                
                check_date = datetime(target_year, current_date.month, current_date.day)
                
                if start_dt <= check_date <= end_dt:
                    return 0, "MATCH"
                return 1, f"NO MATCH ({start_dt.date()}~{end_dt.date()} vs {check_date.date()})"
        except Exception as e:
            return 1, f"ERR {e}"
        return 1, "Format Err"
        
    if '기상' in _cat or '농업' in _cat: return 2, "Weather"
    return 99, "Other"

print("\n=== Checking 2025 Candidates ===")
found_summary_2025 = False
for row in history_data:
    _id, year, cat, content = row
    if year != '2025': continue
    
    prio, reason = get_priority(row, today)
    
    # Filter Logic Check
    clean_text = content.replace('\n', '').replace('|', '').replace('-', '').strip()
    is_filtered = False
    
    # Check for "Table of Contents" (User saw this)
    if '목 차' in content or '목차' in content: 
         # My SQL filter was: AND content NOT LIKE '%목 차%' AND category NOT IN ('목차')
         # If it appears, it means the content has '목 차' but SQL missed it (maybe whitespace) or category is different.
         pass

    if len(clean_text) < 40 and '###' in content:
        if ('제' in content and '장' in content) or len(clean_text) < 15:
            is_filtered = True

    print(f"[{cat}] ID:{_id} Prio:{prio} ({reason}) Filtered?{is_filtered}")
    print(f"Content Sample: {content[:30].replace(chr(10), ' ')}...")
    
    if '요약' in cat:
        found_summary_2025 = True

if not found_summary_2025:
    print("!!! NO SUMMARY FOUND FOR 2025 !!!")

print("\n=== Checking 2024 Candidates ===")
for row in history_data:
    _id, year, cat, content = row
    if year != '2024': continue
    if '요약' not in cat: continue
    prio, reason = get_priority(row, today)
    print(f"[{cat}] ID:{_id} Prio:{prio} ({reason})")
