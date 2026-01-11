import duckdb

try:
    con = duckdb.connect('farming_granular.duckdb', read_only=True)
    
    print("--- [1] Top 5 Most Common Tags in 'tags_crop' ---")
    # Explode the list and count individual tags
    sql_count = """
        SELECT unnest(tags_crop) as tag, count(*) as count
        FROM farm_info
        GROUP BY tag
        ORDER BY count DESC
        LIMIT 5
    """
    print(con.execute(sql_count).fetchall())

    print("\n--- [2] Sample Rows with '참깨' in Content but '감자' in Tags ---")
    sql_sample = """
        SELECT title, tags_crop, content_md
        FROM farm_info
        WHERE content_md LIKE '%참깨%'
        AND list_contains(tags_crop, '감자')
        LIMIT 3
    """
    rows = con.execute(sql_sample).fetchall()
    for r in rows:
        print(f"Title: {r[0]}")
        print(f"Tags: {r[1]}")
        print(f"Content Snippet: {r[2][:50]}...")
        print("-" * 20)

except Exception as e:
    print(f"Error: {e}")
