import pandas as pd
import os

# Funcao que particiona o DF por tipos e salvar em arquivos Parquet separados
def partition_by_types():
    # Output dos arquivos Parquet
    output_dir = 'pokemons_per_types'
    
    # # Verificar se o diretório base 'data_per_types' existe
    # if not os.path.exists(output_dir):
    #     print(f"O diretório base '{output_dir}' não existe. O script não criará diretórios nem salvará arquivos.")
    #     return
    
    # Leitura dos Parquet
    df_types = pd.read_parquet('data_base/df_types.parquet')
    
    # Verificando as primeiras linhas e colunas do DF para entender a estrutura
    print("Primeiras linhas do DataFrame df_types:")
    print(df_types.head())
    print("Colunas disponíveis no DataFrame df_types:")
    print(df_types.columns)
    
    # Certificar de que a coluna 'type' é uma string
    if 'type' not in df_types.columns:
        raise ValueError("A coluna 'type' não está presente no DataFrame df_types.")
    
    df_types['type'] = df_types['type'].astype(str)
    
    # Definir os Pokémon por tipos
    tipos_desejados = ['fogo', 'agua', 'fantasma', 'eletrico', 'inseto']
    
    # Processar cada tipo e salvar em um arquivo Parquet separado
    for tipo in tipos_desejados:
        df_tipo = df_types[df_types['type'] == tipo]
        tipo_dir = os.path.join(output_dir, tipo)
        
        if not os.path.exists(tipo_dir):
            os.makedirs(tipo_dir)
        
        output_file = os.path.join(tipo_dir, f'pokemons_{tipo}.parquet')
        
        # Salva o DF como arquivo Parquet
        df_tipo.to_parquet(output_file, index=False)
        print(f'Salvo: {output_file}')

if __name__ == "__main__":
    partition_by_types()
