# Projeto Pokémon - Processando Dados da API aberta do Pokemon

## Descrição

Este projeto utiliza a API aberta do PokemónsGo para coletar dados sobre Pokémons, processar essas informações e realizar análises usando SQL com DuckDB.
Os dados são salvos em arquivos Parquet e particionados com base no tipo de Pokémon.

## Estrutura do Projeto

1. **Coleta de Dados**: `fetch_and_save_pokemon_data.py`
2. **Consultas SQL**: Scripts para realizar consultas SQL nos DataFrames salvos em Parquet
3. **Particionamento de Dados**: `partition_by_types.py`
4. **README**: Este arquivo

## Scripts

### 1. `fetch_and_save_pokemon_data.py`

**Descrição**: Coleta dados dos Pokémons da API [PokeAPI](https://pokeapi.co/api/v2/pokemon), cria DataFrames e salva os dados em arquivos Parquet.

**Como Executar**:
1. Ative sua variavel de ambiente (opcional):
   ```bash
   venv\Scripts\activate - windows
   source venv/bin/activate - macOS e Linux
2. Certifique-se de que o Python e as bibliotecas necessárias (requests, pandas, pyarrow) estão instalados.
3. Execute o script:
   ```bash
   python fetch_and_save_pokemon_data.py

**Como executar as querys**
1. Execute o script:
   ```bash
   python query_pokemon_data.py

2. Certifique-se de que o Python e as bibliotecas necessárias (pandas, DuckDB) estão instalados.
3. Acesse a pasta querys para rodar os scripts a seguir com o comando:
   ```bash
   cd caminho\da\pasta\teste\querys

4. Execute cada script separadamente:
   ```bash
   python top_5_moves.py
   python distinct_moves_count.py
   python max_weight_pokemons.py
   python min_height_pokemons.py
   python average_weight.py
   python average_height.py

Para realizar as consultas SQL dos DataFrames, use os seguintes scripts.
Cada script deve ser executado separadamente para obter os resultados corretos.

1. top_5_moves.py: Identifica as 5 habilidades ('moves') mais recorrentes entre os Pokémons.
2. distinct_moves_count.py: Conta o número de habilidades ('moves') distintas no dataset.
3. max_weight_pokemon.py: Encontra o maior peso entre os Pokémons e lista os Pokémons com esse peso.
4. min_height_pokemon.py: Encontra a menor altura entre os Pokémons e lista os Pokémons com essa altura.
5. average_weight.py: Calcula a média de peso entre os Pokémons.
6. average_height.py: Calcula a média de altura entre os Pokémons.

**Como Executar o partition_by_types**
1. Certifique-se de que o Python e as bibliotecas necessárias (pandas, os) estão instalados.
2. Execute o script:
   ```bash
    cd .. - para sair da pasta querys
    python partition_by_types.py


**Estrutura dos Arquivos**

**Pasta data_base:**
df_moves.parquet
df_types.parquet
df_dim.parquet

**Pasta data_per_types:**
fogo/pokemons_fogo.parquet
agua/pokemons_agua.parquet
fantasma/pokemons_fantasma.parquet
eletrico/pokemons_eletrico.parquet
inseto/pokemons_inseto.parquet


**Interpretando os Resultados**
Consultas SQL: Os scripts de consulta SQL fornecem análises sobre as habilidades, peso e altura dos Pokémons. Os resultados são impressos diretamente no terminal.

Particionamento por Tipos: Os arquivos Parquet na pasta data_per_types contêm os Pokémons filtrados por tipo, organizados em subdiretórios.

**Dependências**
Bibliotecas Python:
pandas
requests
duckdb
pyarrow

Instale as bibliotecas necessárias com o comando:
pip install pandas requests duckdb pyarrow
