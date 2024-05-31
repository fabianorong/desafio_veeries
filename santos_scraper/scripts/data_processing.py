import pandas as pd
import sqlite3
import pytz
from datetime import datetime
import os
import logging
from config.config import OUTPUT_DIR, DB_DIR, CSV_DIR
from .utils import create_directories

logger = logging.getLogger(__name__)


def save_to_database(data, sentido):
    """
    Salva os dados em um banco de dados SQLite.

    Parâmetros:
    - data: Lista de dados scrapados
    - sentido: Sentido da operação (importação, exportação).
    """
    if data:
        db_path = _get_db_path(sentido)
        _save_data_to_db(data, sentido, db_path)


def save_to_csv(data, sentido):
    """
    Salva os dados em um arquivo CSV.

    Parâmetros:
    - data: Lista de dados scrapados.
    - sentido: Sentido da operação (importação, exportação, etc.).
    """
    if data:
        csv_path = _get_csv_path(sentido)
        _save_data_to_csv(data, sentido, csv_path)


def save_combined_data(data_import, data_export):
    """
    Combina e salva os dados de importação e exportação em um único arquivo CSV e banco de dados.

    Parâmetros:
    - data_import: Lista de dados de importação.
    - data_export: Lista de dados de exportação.
    """
    try:
        data_combined = data_import + data_export
        sentido = "ImpExp"
        db_path = _get_db_path(sentido)
        csv_path = _get_csv_path(sentido)

        _save_data_to_db(data_combined, sentido, db_path)
        _save_data_to_csv(data_combined, sentido, csv_path)
        logger.info(
            f"Dados combinados salvos com sucesso no banco de dados: {db_path} e CSV:{csv_path}"
        )
    except Exception as e:
        logger.error(f"Salvar os dados combinados: {e}")


def _get_db_path(sentido):
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    timestamp = now.strftime("%d%m%Y_%H%M%S")
    db_name = f"santos_{sentido}_{timestamp}.db"
    db_output_dir = os.path.join(OUTPUT_DIR, "data", DB_DIR, sentido)
    create_directories([db_output_dir])
    return os.path.join(db_output_dir, db_name)


def _save_data_to_db(data, sentido, db_path):
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)

    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        c.execute(
            f"""CREATE TABLE IF NOT EXISTS santos_{sentido}(
                porto TEXT, 
                sentido TEXT,
                mercadoria TEXT, 
                eta DATE, 
                peso INTEGER,                    
                updated_On TIMESTAMP 
            )"""
        )
        for entry in data:
            c.execute(
                f"""INSERT INTO santos_{sentido} (porto, sentido, mercadoria, eta, peso, updated_On) VALUES (?, ?, ?, ?, ?, ?)""",
                (*entry, now),
            )
        conn.commit()
    logger.info(f"Dados salvos com sucesso no banco de dados: {db_path}")


def _get_csv_path(sentido):
    # Configura o fuso horário e obtém o timestamp atual
    tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(tz)
    timestamp = now.strftime("%d%m%Y_%H%M%S")

    csv_filename = f"santos_{sentido}_{timestamp}.csv"
    csv_output_dir = os.path.join(OUTPUT_DIR, "data", CSV_DIR, sentido)
    create_directories([csv_output_dir])
    return os.path.join(csv_output_dir, csv_filename)


def _save_data_to_csv(data, sentido, csv_path):
    # Cria um DataFrame a partir dos dados
    df = pd.DataFrame(data, columns=["porto", "sentido", "mercadoria", "eta", "peso"])

    # Agrupa os dados por porto, sentido, eta e mercadoria, somando os pesos
    df_grouped = (
        df.groupby(["porto", "sentido", "eta", "mercadoria"])
        .agg({"peso": "sum"})
        .reset_index()
    )
    # Converte a coluna 'eta' para o formato de data
    df_grouped["eta"] = pd.to_datetime(df_grouped["eta"], format="%d/%m/%Y")

    # Ordena o DataFrame pela coluna 'eta
    df_grouped = df_grouped.sort_values(by="eta").reset_index(drop=True)

    # Converte a coluna 'eta' de volta para o formato de string
    df_grouped["eta"] = df_grouped["eta"].dt.strftime("%d/%m/%Y")

    # Salva o DataFrame no arquivo CSV
    df_grouped.to_csv(csv_path, index=False)

    logger.info(f"Dados salvos com sucesso no CSV: {csv_path}")
