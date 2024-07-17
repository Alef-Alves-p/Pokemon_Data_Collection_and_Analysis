import pandas as pd
import duckdb

def average_height():
    df_dim = pd.read_parquet('../data_base/df_dim.parquet')
    
    con = duckdb.connect()
    result_avg_height = con.execute("""
        SELECT AVG(height) as avg_height
        FROM df_dim
    """).fetchdf()
    
    print("f) A média de altura entre os pokémons:")
    print(result_avg_height)

if __name__ == "__main__":
    average_height()
