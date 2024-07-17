import pandas as pd
import duckdb

def average_weight():
    df_dim = pd.read_parquet('../data_base/df_dim.parquet')
    
    con = duckdb.connect()
    result_avg_weight = con.execute("""
        SELECT AVG(weight) as avg_weight
        FROM df_dim
    """).fetchdf()
    
    print("e) A média de peso entre os pokémons:")
    print(result_avg_weight)

if __name__ == "__main__":
    average_weight()
