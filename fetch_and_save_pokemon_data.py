import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Get Data API
def fetch_pokemon_data(base_url, limit=20):
    offset = 0
    all_pokemons = []
    while True:
        url = f"{base_url}?offset={offset}&limit={limit}"
        print(f"Fetching data from: {url}")
        response = requests.get(url)
        data = response.json()
        
        if not data['results']:
            break
        
        for item in data['results']:
            pokemon_detail = requests.get(item['url']).json()
            all_pokemons.append(pokemon_detail)
        
        offset += limit
    
    return all_pokemons

# 
def create_dataframes(pokemons):
    moves_data = []
    types_data = []
    dim_data = []
    
    for pokemon in pokemons:
        id_ = pokemon['id']
        name = pokemon['name']
        
        # Processando moves
        moves = [move['move']['name'] for move in pokemon['moves']]
        moves_data.append({
            "id": id_,
            "name": name,
            "moves": moves
        })
        
        # Processando types
        types = [ptype['type']['name'] for ptype in pokemon['types']]
        types_data.append({
            "id": id_,
            "name": name,
            "type": types
        })
        
        # Processando dimensões
        dim_data.append({
            "id": id_,
            "name": name,
            "weight": pokemon['weight'],
            "height": pokemon['height']
        })
    
    df_moves = pd.DataFrame(moves_data)
    df_types = pd.DataFrame(types_data)
    df_dim = pd.DataFrame(dim_data)
    
    return df_moves, df_types, df_dim

# Salvando DataFrames como .Parquet
def save_to_parquet(df, filename):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    print(f"DataFrame saved to {filename}")

# URL da API
base_url = "https://pokeapi.co/api/v2/pokemon"

# Coletansdo dados da API
print("Starting data collection from API...")
pokemons = fetch_pokemon_data(base_url)
print(f"Collected data for {len(pokemons)} pokemons")

# Criar DataFrames
print("Creating DataFrames...")
df_moves, df_types, df_dim = create_dataframes(pokemons)

# Salvar DataFrames como arquivos Parquet
print("Saving DataFrames to Parquet files...")
save_to_parquet(df_moves, 'data_base\df_moves.parquet')
save_to_parquet(df_types, 'data_base\df_types.parquet')
save_to_parquet(df_dim, 'data_base\df_dim.parquet')

print("Dados coletados e salvos em formato Parquet com sucesso.")
