import duckdb

try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    
    print("\n--- [2] Sample Rows with '참깨' in Content ---")
    # Just check rows with sesame content and see their tags
    sql_sample = """
        SELECT title, tags_crop 
        FROM farm_info
        WHERE content_md LIKE '%참깨%'
        LIMIT 5
    """
    rows = con.execute(sql_sample).fetchall()
    for r in rows:
        print(f"Title: {r[0]}")
        print(f"Tags: {r[1]}")
        print("-" * 20)
        
    print("\n--- [3] Check tag overlap (content '참깨' AND tag '감자') ---")
    sql_overlap = """
        SELECT count(*) 
        FROM farm_info 
        WHERE content_md LIKE '%참깨%' 
        AND list_contains(tags_crop, '감자')
    """
    print(f"Sesame content with Potato tag count: {con.execute(sql_overlap).fetchall()[0][0]}")

except Exception as e:
    print(f"Error: {e}")
