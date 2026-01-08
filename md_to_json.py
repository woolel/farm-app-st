import re
import json
import glob
import os
from collections import defaultdict

# 1. 키워드 맵 (목차 관련 키워드 제거됨, 공백 제거된 매칭 키워드로 수정)
CATEGORY_MAP = {
    '요약': ['요약', '핵심기술', '주간중점'], # [수정] '요 약'->'요약' (detect_category에서 공백 제거하므로)
    '기상': ['기상', '전망', '날씨', '저수율', '강수량', '농업정보', '농업정보'],
    '벼': ['벼', '볍씨', '모내기', '쌀', '식량작물', '이앙', '논'],
    '밭작물': ['밭작물', '콩', '감자', '고구마', '보리', '밀', '옥수수', '두류', '잡곡', '맥류'],
    '채소': ['채소', '고추', '마늘', '양파', '배추', '무', '시설하우스', '딸기', '수박', '오이', '토마토', '원예'],
    '과수': ['과수', '사과', '배', '포도', '복숭아', '감귤', '단감', '자두', '과원', '동해', '꽃눈'],
    '화훼': ['화훼', '국화', '장미', '프리지아', '카네이션', '꽃'],
    '특용작물': ['특용작물', '인삼', '오미자', '약용작물', '버섯', '느타리', '당귀'],
    '축산': ['축산', '한우', '돼지', '닭', 'AI', '구제역', '가축', '방역', '소', '젖소', '양돈', '가금', '돈사', '계사'],
    '양봉': ['양봉', '벌통', '꿀벌', '벌집', '봉군', '말벌', '응애', '월동벌', '장수말벌', '등검은말벌', '합봉', '사양기']
}

def detect_category(text):
    """텍스트에서 카테고리 키워드 찾기 (목차 필터링 포함)"""
    # 0. 목차 필터링 우선 적용
    if '목차' in text or '목 차' in text:
        return 'SKIP'
        
    clean_text = text.replace(" ", "")
    for category, keywords in CATEGORY_MAP.items():
        for keyword in keywords:
            if keyword in clean_text:
                return category
    return '기타'

def parse_md_to_jsonl_robust(directory_path):
    # json_con 폴더 내의 md 파일만 타겟팅 (유저 요청에 따라)
    target_path = os.path.join(directory_path, "json_con", "*.md")
    md_files = glob.glob(target_path)
    
    # 만약 json_con에 없으면 현재 디렉토리도 검색
    if not md_files:
        md_files = glob.glob(os.path.join(directory_path, "*.md"))

    all_weeks_data = []

    # 헤더 패턴: # [2024-01-01~2024-01-07] 제목
    header_pattern = re.compile(r'^#\s*\[(\d{4}-\d{2}-\d{2}~\d{4}-\d{2}-\d{2})\]\s*(.*)')

    print(f"📂 발견된 파일: {len(md_files)}개")

    for file_path in md_files:
        print(f"   -> 처리 중: {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"   ⚠️ 파일 읽기 오류: {e}")
            continue

        temp_storage = defaultdict(lambda: defaultdict(list))
        
        current_date_key = None
        current_category = '기타'
        skip_current_section = False

        for line in lines:
            stripped_line = line.strip()
            if not stripped_line: continue

            # 1. 헤더 라인인지 확인
            match = header_pattern.match(stripped_line)
            
            if match:
                date_range = match.group(1)
                title_content = match.group(2)
                
                current_date_key = date_range
                detected_cat = detect_category(title_content)
                
                if detected_cat == 'SKIP':
                    skip_current_section = True
                    current_category = None
                else:
                    skip_current_section = False
                    current_category = detected_cat
            
            else:
                # 2. 본문 라인
                if not skip_current_section and current_date_key and current_category:
                    temp_storage[current_date_key][current_category].append(line.strip())

        # 3. 임시 저장소를 리스트 구조로 반환 (JSONL용)
        for date_key, cat_data in temp_storage.items():
            start_date, end_date = date_key.split('~')
            
            week_entry = {
                "id": date_key,
                "year": start_date[:4],
                "month": int(start_date[5:7]),
                "week_range": date_key,
                "content": {}
            }
            
            for cat, texts in cat_data.items():
                week_entry["content"][cat] = "\n".join(texts)
                
            all_weeks_data.append(week_entry)

    # 날짜순 정렬
    all_weeks_data.sort(key=lambda x: x['id'])
    
    return all_weeks_data

if __name__ == "__main__":
    current_dir = "."
    print(f"🚀 MD -> JSONL 변환 시작 (TOC 제거 로직 적용)")
    
    data = parse_md_to_jsonl_robust(current_dir)
    
    # JSONL 파일로 저장 (embed.py 입력용)
    output_file = 'optimized_farming_data_v2.jsonl'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for entry in data:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
            
    print(f"✅ 변환 완료: {len(data)}주차 데이터 생성됨 -> {output_file}")
