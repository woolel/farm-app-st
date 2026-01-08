import re
import json
import glob
import os
from collections import defaultdict

# 1. 키워드 맵 (기타 카테고리 추가)
CATEGORY_MAP = {
    '요약': ['요 약', '핵심기술', '주간 중점', '목 차', '목차'], 
    '기상': ['기상', '전망', '날씨', '저수율', '강수량', '농업정보', '농업 정보'],
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
    """텍스트에서 카테고리 키워드 찾기"""
    clean_text = text.replace(" ", "")
    for category, keywords in CATEGORY_MAP.items():
        for keyword in keywords:
            if keyword in clean_text:
                return category
    return '기타' # [변경] 매칭 안 되면 '기타'로 분류 (데이터 유실 방지)

def parse_md_to_json_robust(directory_path):
    md_files = glob.glob(os.path.join(directory_path, "*.md"))
    all_weeks_data = []

    # 헤더 패턴: # [2024-01-01~2024-01-07] 제목
    header_pattern = re.compile(r'^#\s*\[(\d{4}-\d{2}-\d{2}~\d{4}-\d{2}-\d{2})\]\s*(.*)')

    print(f"📂 발견된 파일: {len(md_files)}개")

    for file_path in md_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # 데이터 임시 저장소
        # 구조: { "2024-01-01~2024-01-07": { "벼": ["내용1", "내용2"], "과수": [...] } }
        temp_storage = defaultdict(lambda: defaultdict(list))
        
        current_date_key = None
        current_category = '기타' # 기본값

        for line in lines:
            stripped_line = line.strip()
            if not stripped_line: continue

            # 1. 헤더 라인인지 확인
            match = header_pattern.match(stripped_line)
            
            if match:
                # 날짜와 제목 추출
                date_range = match.group(1) # 예: 2024-01-01~2024-01-07
                title_content = match.group(2) # 예: 제5장 과수
                
                # 날짜 키 갱신 (주차 변경 감지)
                current_date_key = date_range
                
                # 카테고리 감지
                detected_cat = detect_category(title_content)
                current_category = detected_cat
                
                # 헤더 자체도 내용에 포함할지 여부 (선택사항, 여기선 제목으로 씀)
                # temp_storage[current_date_key][current_category].append(f"### {title_content}")
            
            else:
                # 2. 본문 라인
                if current_date_key:
                    # 현재 날짜와 카테고리에 내용 추가
                    temp_storage[current_date_key][current_category].append(line.strip())
                else:
                    # 날짜가 아직 안 나왔는데 내용이 있는 경우 (파일 앞부분 서론 등)
                    # 무시하거나 첫 데이터가 나오기 전까지는 스킵
                    pass

        # 3. 임시 저장소를 JSON 구조로 변환
        for date_key, cat_data in temp_storage.items():
            start_date, end_date = date_key.split('~')
            
            # 리스트로 모인 텍스트들을 하나의 문자열로 합침 (\n 연결)
            final_content = {}
            for cat, texts in cat_data.items():
                final_content[cat] = "\n".join(texts)

            week_data = {
                "id": date_key,
                "year": start_date[:4],
                "month": int(start_date[5:7]),
                "week_range": date_key,
                "content": final_content
            }
            all_weeks_data.append(week_data)

    # 날짜순 정렬
    all_weeks_data.sort(key=lambda x: x['id'])
    
    return all_weeks_data

if __name__ == "__main__":
    # 현재 폴더(.)의 md 파일 변환
    data = parse_md_to_json_robust('.')
    
    print(f"🚀 변환 완료: 총 {len(data)}주차(Weeks) 데이터 추출됨")
    
    if len(data) > 0:
        first_week = data[0]
        print(f"📅 첫 주차: {first_week['week_range']}")
        print(f"📝 포함된 카테고리: {list(first_week['content'].keys())}")
        
        # 샘플 출력
        sample_cat = list(first_week['content'].keys())[0]
        print(f"🔍 '{sample_cat}' 내용 미리보기:\n{first_week['content'][sample_cat][:100]}...")

    with open('farming_data_final.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)