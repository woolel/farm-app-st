
import duckdb
from datetime import datetime

con = duckdb.connect('farming_granular.duckdb', read_only=True)
current_month = 1
# Mock today as Jan 7
today = datetime(2025, 1, 7)

print(f"=== Debugging V2 for {today.date()} ===")

history_sql = f"""
    SELECT id, year, category, content 
    FROM farming 
    WHERE month = ? 
    ORDER BY year DESC
    LIMIT 150 
"""
history_data = con.execute(history_sql, [current_month]).fetchall()

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
                check_date = datetime(target_year, current_date.month, current_date.day)
                
                diff_start = abs((check_date - start_dt).days)
                diff_end = abs((check_date - end_dt).days)
                
                if start_dt <= check_date <= end_dt:
                    return 0, "MATCH"
                elif diff_start <= 7 or diff_end <= 7:
                    return 0, "NEAR MATCH (+-7)"
        except Exception as e:
            return 100, f"ERR {e}"
        return 100, "NO MATCH"
        
    if '기상' in _cat or '농업' in _cat: return 2, "Weather"
    return 99, "Other"

print("\n=== Checking 2025 Candidates ===")
for row in history_data:
    _id, year, cat, content = row
    if year != '2025': continue
    
    prio, reason = get_priority(row, today)
    
    # Filter Logic
    clean_text = content.replace('\n', '').replace('|', '').replace('-', '').strip()
    is_filtered = False
    
    if '목 차' in content or '목차' in content: 
         is_filtered = True
         reason += " [MOKCHA FILTER]"

    is_header_only = False
    if '###' in content:
            if ('제' in content and '장' in content) and len(clean_text) < 60:
                is_header_only = True
            elif len(clean_text) < 30:
                is_header_only = True
    
    if is_header_only:
        is_filtered = True
        reason += " [HEADER FILTER]"
        
    if '요약' in cat and prio > 0:
        is_filtered = True
        reason += " [NON-MATCH SUMMARY]"

    print(f"[{cat}] ID:{_id} Prio:{prio} ({reason}) Filtered?{is_filtered}")
    print(f"Content: {content[:40].replace(chr(10), ' ')}...")
