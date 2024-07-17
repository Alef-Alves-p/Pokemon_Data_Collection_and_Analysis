import pandas as pd
import duckdb

def max_weight_pokemons():
    df_dim = pd.read_parquet('../data_base/df_dim.parquet')
    
    con = duckdb.connect()
    result_max_weight = con.execute("""
        SELECT weight
        FROM df_dim
        ORDER BY weight DESC
        LIMIT 1
    """).fetchdf()
    
    max_weight = result_max_weight.iloc[0]['weight']

    result_max_weight_pokemons = con.execute(f"""
        SELECT id, name, weight
        FROM df_dim
        WHERE weight = {max_weight}
    """).fetchdf()
    
    print("c) O maior peso entre os pokémons e quais são eles:")
    print(result_max_weight_pokemons)

if __name__ == "__main__":
    max_weight_pokemons()
