import pandas as pd
import duckdb

def min_height_pokemons():
    df_dim = pd.read_parquet('../data_base/df_dim.parquet')
    
    con = duckdb.connect()
    result_min_height = con.execute("""
        SELECT height
        FROM df_dim
        ORDER BY height ASC
        LIMIT 1
    """).fetchdf()
    
    min_height = result_min_height.iloc[0]['height']

    result_min_height_pokemons = con.execute(f"""
        SELECT id, name, height
        FROM df_dim
        WHERE height = {min_height}
    """).fetchdf()
    
    print("d) A menor altura entre os pokémons e quais são eles:")
    print(result_min_height_pokemons)

if __name__ == "__main__":
    min_height_pokemons()
