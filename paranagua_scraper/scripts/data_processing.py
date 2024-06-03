import sqlite3
import pandas as pd
from datetime import datetime
import logging
import os
from .utils import create_directories
import pytz
from ..config.config import DB_DIR, CSV_DIR, DATA_DIR, PARANAGUA_DIR

# Criação do logger para registrar mensagens de log
logger = logging.getLogger(__name__)


# Função para salvar dados no banco de dados SQLite
def save_to_database(all_data, sentido, db_path):
    """
    Salva os dados extraídos em um banco de dados SQLite.

    Parâmetros:
    all_data (list): Lista de tuplas contendo os dados a serem salvos.
    db_path (str): Caminho para o arquivo do banco de dados SQLite.
    """
    logging.info("Iniciando salvamento no banco de dados.")
    now = datetime.now()
    try:
        # Conecta ao banco de dados SQLite (ou cria o banco de dados, se não existir)
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            # Cria a tabela paranagua_data, se não existir
            c.execute(
                f"""CREATE TABLE IF NOT EXISTS paranagua_{sentido}(
                    porto TEXT, 
                    sentido TEXT,
                    mercadoria TEXT, 
                    eta DATE, 
                    peso INTEGER,
                    unidade_Peso TEXT,                    
                    updated_On TIMESTAMP 
                )"""
            )
            # Insere cada entrada de dados na tabela
            for entry in all_data:
                c.execute(
                    f"""INSERT INTO paranagua_{sentido} 
                        (porto, sentido, mercadoria, eta, peso, unidade_Peso, updated_On) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (*entry, now),
                )
            conn.commit()  # Confirma o commit no banco de dados
        logger.info("Dados salvos com sucesso no banco de dados.")
    except Exception as e:
        logger.error(f"Erro ao salvar no banco de dados: {e}")


# Função para salvar dados em um arquivo CSV
def save_to_csv(all_data, sentido, csv_path):
    """
    Salva os dados extraídos em um arquivo CSV.

    Parâmetros:
    all_data (list): Lista de tuplas contendo os dados a serem salvos.
    csv_path (str): Caminho para o arquivo CSV.
    """
    logging.info("Iniciando salvamento em CSV.")
    try:
        # Cria um DataFrame a partir dos dados
        df = pd.DataFrame(
            all_data,
            columns=["porto", "sentido", "mercadoria", "eta", "peso", "unidade_Peso"],
        )

        # junta os dados e soma os pesos
        df_grouped = (
            df.groupby(["porto", "sentido", "eta", "mercadoria", "unidade_Peso"])
            .agg({"peso": "sum"})
            .reset_index()
        )

        # Converte a coluna 'eta' para o formato de data
        df_grouped["eta"] = pd.to_datetime(df_grouped["eta"], format="%d/%m/%Y")

        # Ordena o DataFrame pela coluna 'eta'
        df_grouped = df_grouped.sort_values(by="eta").reset_index(drop=True)

        # Converte a coluna 'eta' de volta para o formato de string
        df_grouped["eta"] = df_grouped["eta"].dt.strftime("%d/%m/%Y")

        # Ordena as colunas no padrão de relevancia
        df_grouped = df_grouped[
            ["porto", "sentido", "eta", "mercadoria", "peso", "unidade_Peso"]
        ]

        df_grouped.to_csv(csv_path, index=False)
        logging.info("Dados salvos com sucesso no CSV.")
    except Exception as e:
        logging.error(f"Erro ao salvar no CSV: {e}")


def save_combined_data(data_import, data_export, data_import_export):
    """
    Combina e salva os dados de importação e exportação em um único arquivo CSV e banco de dados.

    Parâmetros:
    - data_import: Lista de dados de importação.
    - data_export: Lista de dados de exportação.
    """
    try:
        data_combined = data_import + data_export + data_import_export
        sentido = "Combined_ImpExp"
        db_path = _get_db_path(sentido)
        csv_path = _get_csv_path(sentido)

        save_to_database(data_combined, sentido, db_path)
        save_to_csv(data_combined, sentido, csv_path)
        logging.info(
            f"Dados combinados salvos com sucesso no banco de dados: {db_path} e CSV:{csv_path}"
        )
    except Exception as e:
        logging.error(f"Salvar os dados combinados: {e}")


def _get_db_path(sentido):
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    timestamp = now.strftime("%d%m%Y_%H%M%S")
    db_name = f"paranagua_{sentido}_{timestamp}.db"
    db_output_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, DB_DIR, sentido)
    create_directories([db_output_dir])
    return os.path.join(db_output_dir, db_name)


def _get_csv_path(sentido):
    # Configura o fuso horário e obtém o timestamp atual
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    timestamp = now.strftime("%d%m%Y_%H%M%S")

    csv_filename = f"paranagua_{sentido}_{timestamp}.csv"
    csv_output_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, CSV_DIR, sentido)
    create_directories([csv_output_dir])
    return os.path.join(csv_output_dir, csv_filename)
