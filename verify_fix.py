
import re

def format_content(text):
    if not text: return ""
    text = text.replace('~', r'\~') 
    lines = text.split('\n')
    formatted_lines = []
    
    in_table = False
    for i, line in enumerate(lines):
        clean_line = line.strip()
        has_pipe = '|' in clean_line and len(clean_line.replace('|', '').strip()) > 0
        
        if has_pipe:
            processed_line = re.sub(r'\s*\|\s*', ' | ', clean_line).strip()
            if not processed_line.startswith('|'): processed_line = '| ' + processed_line
            if not processed_line.endswith('|'): processed_line = processed_line + ' |'
            
            if not in_table:
                if i > 0 and formatted_lines and formatted_lines[-1] != "":
                    pass 
                
                formatted_lines.append(processed_line)
                
                next_is_separator = False
                if i + 1 < len(lines):
                    next_clean = lines[i+1].strip()
                    if next_clean.startswith('|') and '-' in next_clean:
                        next_is_separator = True
                
                if not next_is_separator:
                    num_cols = processed_line.count('|') - 1
                    if num_cols > 0:
                        separator = "|" + "---|" * num_cols
                        formatted_lines.append(separator)
                
                in_table = True
            else:
                formatted_lines.append(processed_line)
        else:
            if in_table:
                in_table = False
            formatted_lines.append(line)
            
    return '\n'.join(formatted_lines)

# 테스트 데이터 (깨진 요약 테이블 모사)
test_data = """요약 정보입니다.
| 제1장 | 농 | |
| 업 | 1 | |
| 제2장 | 재 | |
| 배 | 5 | |
일반 텍스트입니다."""

print("--- INPUT ---")
print(test_data)
print("\n--- OUTPUT ---")
print(format_content(test_data))
