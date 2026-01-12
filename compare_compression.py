
import duckdb
import os
import time

input_file = 'weekly_farming.parquet'
output_file = 'weekly_farming_zstd.parquet'

try:
    con = duckdb.connect(':memory:')
    
    # 1. Read original
    start_read = time.time()
    con.execute(f"CREATE TABLE data AS SELECT * FROM '{input_file}'")
    read_time = time.time() - start_read
    
    # 2. Write with ZSTD
    start_write = time.time()
    # Default compression for DuckDB parquet export is often Snappy. 
    # We specify ZSTD explicitly. try ZSTD (level 3 is default usually)
    con.execute(f"COPY data TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
    write_time = time.time() - start_write
    
    # 3. Compare Sizes
    orig_size = os.path.getsize(input_file)
    zstd_size = os.path.getsize(output_file)
    
    print(f"Original ({input_file}): {orig_size / 1024 / 1024:.2f} MB")
    print(f"ZSTD ({output_file}): {zstd_size / 1024 / 1024:.2f} MB")
    print(f"Reduction: {(1 - zstd_size/orig_size)*100:.2f}%")
    print(f"Read Time (Original): {read_time:.4f}s")
    print(f"Write Time (ZSTD): {write_time:.4f}s")

except Exception as e:
    print(f"Error: {e}")
