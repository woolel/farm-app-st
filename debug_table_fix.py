
import duckdb
import re

def format_content(text):
    if not text: return ""
    text = text.replace('~', r'\~') 
    lines = text.splitlines()
    output = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        if '|' in line and any(c.isalnum() for c in line):
            rows_data = []
            max_cols = 0
            while i < len(lines) and '|' in lines[i]:
                raw_row = lines[i].strip()
                
                # 가공: 도트 리더 제거 및 중복 파이프 정리
                processed = raw_row.replace('···', '').replace('...', '')
                processed = re.sub(r'\|+', '|', processed) # ||| -> |
                
                cells = [c.strip() for c in processed.strip('|').split('|')]
                while cells and not cells[-1]: cells.pop()
                
                if cells:
                    rows_data.append(cells)
                    max_cols = max(max_cols, len(cells))
                i += 1
            
            # 품질 관리: 열이 너무 많으면(TOC 망가진 경우) 최대 3열로 제한(주요 내용만)
            if max_cols > 5: max_cols = 5
                
            if rows_data and max_cols >= 2:
                for idx, cells in enumerate(rows_data):
                    # 5열 초과 데이터는 합치거나 자름
                    if len(cells) > max_cols:
                        cells = cells[:max_cols-1] + [" ".join(cells[max_cols-1:])]
                    
                    padded = cells + [""] * (max_cols - len(cells))
                    output.append("| " + " | ".join(padded) + " |")
                    if idx == 0:
                        output.append("|" + " --- |" * max_cols)
            elif rows_data:
                for cells in rows_data: output.append(" ".join(cells))
        else:
            output.append(line)
            i += 1
    return "\n".join(output)

con = duckdb.connect('farming_granular.duckdb', read_only=True)
res = con.execute("SELECT id, content FROM farming WHERE category = '요약' AND content LIKE '%|%' LIMIT 1").fetchall()

for rid, cont in res:
    print(f"--- ID: {rid} ---")
    print("[FORMATTED CONTENT]")
    print(format_content(cont))
    print("----------------------------------\n")
con.close()
