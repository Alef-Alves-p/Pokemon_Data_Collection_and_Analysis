import pandas as pd
import duckdb

def top_5_moves():
    df_moves = pd.read_parquet('../data_base/df_moves.parquet')
    
    con = duckdb.connect()
    result_top_moves = con.execute("""
        SELECT move, COUNT(*) as frequency
        FROM (
            SELECT id, name, unnest(moves) as move 
            FROM df_moves
        )
        GROUP BY move
        ORDER BY frequency DESC
        LIMIT 5
    """).fetchdf()
    
    print("a) As 5 habilidades 'moves' mais recorrentes entre os pokémons:")
    print(result_top_moves)

if __name__ == "__main__":
    top_5_moves()
