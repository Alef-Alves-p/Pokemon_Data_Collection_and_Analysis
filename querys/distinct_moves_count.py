import pandas as pd
import duckdb

def distinct_moves_count():
    df_moves = pd.read_parquet('../data_base/df_moves.parquet')
    
    con = duckdb.connect()
    result_distinct_moves = con.execute("""
        SELECT COUNT(DISTINCT move) as distinct_moves
        FROM (
            SELECT unnest(moves) as move
            FROM df_moves
        )
    """).fetchdf()
    
    print("b) Número de habilidades 'moves' distintas:")
    print(result_distinct_moves)

if __name__ == "__main__":
    distinct_moves_count()
