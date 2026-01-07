
import duckdb
import pandas as pd
import re

con = duckdb.connect('farming_granular.duckdb', read_only=True)

# 기상 카테고리의 실제 데이터 가져오기
sample_weather = con.execute("SELECT content FROM farming WHERE category='기상' AND content LIKE '%|%' LIMIT 3").fetchall()

def debug_render_weather_chart(content):
    print("\n--- Testing Content ---")
    print(content[:200] + "...")
    try:
        lines = content.split('\n')
        table_lines = [l.strip() for l in lines if '|' in l]
        print(f"Found {len(table_lines)} table lines")
        if len(table_lines) < 3: 
            print("Failed: Less than 3 table lines")
            return
        
        sep_idx = -1
        for i, line in enumerate(table_lines):
            if '---' in line:
                sep_idx = i
                break
        
        print(f"Separator index: {sep_idx}")
        if sep_idx <= 0: 
            print("Failed: No separator or separator at top")
            return
        
        header_line = table_lines[sep_idx - 1]
        headers = [h.strip() for h in header_line.split('|') if h.strip()]
        print(f"Headers: {headers}")
        
        if not any(k in "".join(headers) for k in ['기온', '강수', '온도', '습도']):
            print("Failed: No keywords in headers")
            return
            
        data = []
        for line in table_lines[sep_idx + 1:]:
            cols = [c.strip() for c in line.split('|') if c.strip()]
            if len(cols) >= 2:
                data.append(cols[:len(headers)])
        
        print(f"Data rows found: {len(data)}")
        if not data: 
            print("Failed: No data rows")
            return
        
        df = pd.DataFrame(data, columns=headers[:len(data[0])])
        print("Initial DataFrame:")
        print(df)
        
        def extract_num(text):
            pure_text = re.sub(r'\(.*?\)', '', text)
            nums = re.findall(r"[-+]?\d*\.\d+|\d+", pure_text)
            if not nums: return None
            return sum(float(n) for n in nums) / len(nums)

        val_cols = []
        for col in headers[1:]:
            if any(k in col for k in ['기온', '강수', '온도', '습도']):
                df[f'{col}_val'] = df[col].apply(extract_num)
                val_cols.append(col)
        
        print(f"Value columns: {val_cols}")
        df_plot = df.dropna(subset=[f'{c}_val' for c in val_cols])
        print(f"Plotting DataFrame shape: {df_plot.shape}")
        if df_plot.empty:
            print("Failed: Plotting DataFrame is empty")
            return
            
        print("SUCCESS: Chart would render")

    except Exception as e:
        print(f"Error: {e}")

for row in sample_weather:
    debug_render_weather_chart(row[0])
