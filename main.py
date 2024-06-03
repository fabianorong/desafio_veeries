import os
import logging
from datetime import datetime
import pandas as pd
import sqlite3

# Importar as funções de scraping e salvamento de Paranaguá e Santos
from paranagua_scraper.main import main as main_paranagua
from santos_scraper.main import main as main_santos


def combine_data(paranagua_csv_path, santos_csv_path, output_csv_path, db_path):
    # Ler os arquivos CSV
    df_paranagua = pd.read_csv(paranagua_csv_path)
    df_santos = pd.read_csv(santos_csv_path)

    # Verificar as colunas para garantir que sejam consistentes
    print("Colunas em Paranaguá:", df_paranagua.columns)
    print("Colunas em Santos:", df_santos.columns)

    # Combinar os DataFrames
    df_combined = pd.concat([df_paranagua, df_santos], ignore_index=True)

    # Converter a coluna 'eta' para o formato datetime
    df_combined["eta"] = pd.to_datetime(df_combined["eta"], format="%d/%m/%Y")

    # Ordenar os dados combinados por 'eta'
    df_combined = df_combined.sort_values(by="eta")

    # Converter a coluna 'eta' de volta para string no formato original
    df_combined["eta"] = df_combined["eta"].dt.strftime("%d/%m/%Y")

    # Salvar o DataFrame combinado em um novo arquivo CSV
    df_combined.to_csv(output_csv_path, index=False)
    print(f"Dados combinados salvos em {output_csv_path}")

    # Conectar ao banco de dados SQLite e salvar os dados combinados
    conn = sqlite3.connect(db_path)
    df_combined.to_sql("combined_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Dados combinados salvos em {db_path}")


def main():
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Iniciando o processo de scraping e combinacao dos dados")

    # Definir diretórios de saída
    paranagua_csv_dir = os.path.join(
        "paranagua_scraper", "data", "csv", "Combined_ImpExp"
    )
    santos_csv_dir = os.path.join("santos_scraper", "data", "csv", "ImpExp")
    output_csv_dir = os.path.join("combined_data", "csv")
    db_dir = os.path.join("combined_data", "db")

    # Criar diretórios de saída, se não existirem
    os.makedirs(paranagua_csv_dir, exist_ok=True)
    os.makedirs(santos_csv_dir, exist_ok=True)
    os.makedirs(output_csv_dir, exist_ok=True)
    os.makedirs(db_dir, exist_ok=True)

    # Executar scraping de Paranagua
    main_paranagua()

    # Obter o arquivo CSV mais recente de Paranagua
    paranagua_csv_path = max(
        [os.path.join(paranagua_csv_dir, f) for f in os.listdir(paranagua_csv_dir)],
        key=os.path.getctime,
    )

    # Executar scraping de Santos
    main_santos()

    # Obter o arquivo CSV mais recente de Santos
    santos_csv_path = max(
        [os.path.join(santos_csv_dir, f) for f in os.listdir(santos_csv_dir)],
        key=os.path.getctime,
    )

    # Definir o caminho para o arquivo CSV combinado
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
    output_csv_path = os.path.join(
        output_csv_dir, f"combined_paranagua_santos_{timestamp}.csv"
    )

    # Definir o caminho para o arquivo do banco de dados
    db_path = os.path.join(db_dir, f"combined_data_{timestamp}.db")

    # Combinar os dados e salvar em um novo arquivo CSV e banco de dados
    combine_data(paranagua_csv_path, santos_csv_path, output_csv_path, db_path)

    logging.info("Processo concluido com sucesso")


if __name__ == "__main__":
    main()
