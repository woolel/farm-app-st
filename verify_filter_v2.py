
from datetime import datetime

def filter_content_v2(content):
    clean_text = content.replace('\n', '').replace('|', '').replace('-', '').strip()
    
    # "### 제1장 벼" 같은 헤더만 있는 경우 대략 10~20자 내외
    if len(clean_text) < 40: 
        if '###' in content:
             # 진짜 헤더인지 확인 (제x장 패턴)
             if ('제' in content and '장' in content) or len(clean_text) < 20:
                 return False # Filter OUT
    return True # Keep

test_cases = [
    ("### 요약\n내용이 충분히 김...", True), 
    ("### 제1장 벼", False), # Should filter
    ("### 제 7장 축산", False), # Should filter
    ("기상 정보입니다.", True), # Should keep (no ###)
    ("### 짧은 요약", False), # length < 20 -> Filter out (might be risky but safer for noise)
    ("### 비교적 긴 요약이지만 40자는 안되는 경우", True), # length ~25 > 20 -> Keep
]

print("=== FILTER V2 TEST ===")
for text, expected in test_cases:
    result = filter_content_v2(text)
    print(f"'{text}' -> Keep? {result} (Expected: {expected})")
