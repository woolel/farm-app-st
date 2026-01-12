
import duckdb
import numpy as np

try:
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE farm_info AS SELECT * FROM 'weekly_farming.parquet'")
    
    # Simulate Index Build
    con.execute("INSTALL vss; LOAD vss;")
    con.execute("CREATE INDEX embedding_idx ON farm_info USING HNSW (embedding);")
    con.execute("INSTALL fts; LOAD fts;")
    con.execute("PRAGMA create_fts_index('farm_info', 'id', 'title', 'content_md');")
    
    # 1. Check FTS
    print("\n--- FTS Check (Query: 배추) ---")
    fts_res = con.execute("SELECT title, score FROM (SELECT *, fts_main_farm_info.match_bm25(id, '배추') as score FROM farm_info) WHERE score IS NOT NULL ORDER BY score DESC LIMIT 3").fetchall()
    print(fts_res)

    # 2. Check Vector Search
    print("\n--- Vector Search Check (Random Vector) ---")
    # Generate random 768-dim vector
    vec = np.random.rand(768).astype('float32').tolist()
    vec_res = con.execute("SELECT title, array_cosine_similarity(embedding, ?::FLOAT[768]) as score FROM farm_info ORDER BY score DESC LIMIT 3", [vec]).fetchall()
    print(vec_res)
    
except Exception as e:
    print(f"Error: {e}")
