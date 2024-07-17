import pandas as pd
import duckdb

# Função que lê e executa as consultas SQL
def query_pokemon_data():
    # Lendos os Parquets
    df_moves = pd.read_parquet('data_base\df_moves.parquet')
    df_types = pd.read_parquet('data_base\df_types.parquet')
    df_dim = pd.read_parquet('data_base\df_dim.parquet')
    
    # Consulta com DuckDB
    con = duckdb.connect()

    # Consultas SQL
    result_moves = con.execute("SELECT id, name, unnest(moves) as move FROM df_moves").fetchdf()
    result_types = con.execute("SELECT id, name, unnest(type) as type FROM df_types").fetchdf()
    result_dim = con.execute("SELECT id, name, weight, height FROM df_dim WHERE weight > 100").fetchdf()

    # print results
    print("Consulta df_moves:")
    print(result_moves.head())

    print("Consulta df_types:")
    print(result_types.head())

    print("Consulta df_dim:")
    print(result_dim.head())

if __name__ == "__main__":
    print("Running SQL queries using DuckDB...")
    query_pokemon_data()
