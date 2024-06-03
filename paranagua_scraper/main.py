import os
import logging
from datetime import datetime
from .scripts.scraper import scrape_paranagua_data
from .scripts.data_processing import save_to_database, save_to_csv, save_combined_data
from .scripts.utils import create_directories
from .config.config import DB_DIR, CSV_DIR, DATA_DIR, PARANAGUA_DIR

# Configuração de logging
# Define o diretório de saída dos logs
output_log_dir = os.path.join("paranagua_scraper", "logs")
create_directories([output_log_dir])
log_file = os.path.join(output_log_dir, "scraper.log")

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)


def main():
    """
    Função principal que faz o scraping, processamento e salvamento dos dados.
    """
    try:
        # Gera um timestamp para os nomes dos arquivos
        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")

        # Define os nomes dos arquivos CSV e de banco de dados
        csv_filename = f"paranagua__{timestamp}.csv"
        db_filename = f"paranagua__{timestamp}.db"

        # Define os diretórios de saída para o banco de dados e CSV
        output_db_Imp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, DB_DIR, "Imp")
        output_db_Exp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, DB_DIR, "Exp")
        output_db_ImpExp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, DB_DIR, "ImpExp")

        output_csv_Imp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, CSV_DIR, "Imp")
        output_csv_Exp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, CSV_DIR, "Exp")
        output_csv_ImpExp_dir = os.path.join(PARANAGUA_DIR, DATA_DIR, CSV_DIR, "ImpExp")

        # Cria os diretórios de saída, se não existirem
        create_directories(
            [
                output_db_Imp_dir,
                output_db_Exp_dir,
                output_db_ImpExp_dir,
                output_csv_Imp_dir,
                output_csv_Exp_dir,
                output_csv_ImpExp_dir,
            ]
        )

        sentido_import = "Imp"
        sentido_export = "Exp"
        sentido_import_export = "Imp/Exp"

        db_imp_path = os.path.join(output_db_Imp_dir, db_filename)
        db_exp_path = os.path.join(output_db_Exp_dir, db_filename)
        db_imp_exp_path = os.path.join(output_db_ImpExp_dir, db_filename)

        csv_imp_path = os.path.join(output_csv_Imp_dir, csv_filename)
        csv_exp_path = os.path.join(output_csv_Exp_dir, csv_filename)
        csv_imp_exp_path = os.path.join(output_csv_ImpExp_dir, csv_filename)

        # Scrape de dados de importação
        data_import = scrape_paranagua_data(sentido_import, "import")
        save_to_database(data_import, "import", db_imp_path)
        save_to_csv(data_import, "import", csv_imp_path)

        # Scrape de dados de exportação
        data_export = scrape_paranagua_data(sentido_export, "export")
        save_to_database(data_export, "export", db_exp_path)
        save_to_csv(data_export, "export", csv_exp_path)

        # Scrape de dados de importação e exportação
        data_import_export = scrape_paranagua_data(
            sentido_import_export, "import_export"
        )
        save_to_database(data_import_export, "import_export", db_imp_exp_path)
        save_to_csv(data_import_export, "import_export", csv_imp_exp_path)

        # Combinação dos dados de importação e exportação
        save_combined_data(data_import, data_export, data_import_export)

        logging.info("Scraping e arquivos exportados concluído com sucesso")
    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")

    # # Realiza o scraping dos dados para os sentidos definidos
    # all_data = scrape_paranagua_data(sentidos)

    # # Se houver dados extraídos
    # if all_data:
    #     # Gera um timestamp para os nomes dos arquivos
    #     timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")

    #     # Define os nomes dos arquivos CSV e de banco de dados
    #     csv_filename = f"paranagua__{timestamp}.csv"
    #     db_filename = f"paranagua__{timestamp}.csv"

    #     # Define os diretórios de saída para o banco de dados e CSV
    #     output_db_dir = os.path.join("paranagua_scraper", DATA_DIR, DB_DIR)
    #     output_csv_dir = os.path.join("paranagua_scraper", DATA_DIR, CSV_DIR)

    #     # Cria os diretórios de saída, se não existirem
    #     create_directories([output_db_dir, output_csv_dir])

    #     # Define os caminhos completos dos arquivos
    #     db_path = os.path.join(output_db_dir, db_filename)
    #     csv_path = os.path.join(output_csv_dir, csv_filename)

    #     # Salva os dados no banco de dados e no arquivo CSV
    #     save_to_database(all_data, db_path)
    #     save_to_csv(all_data, csv_path)
    #     logging.info("Processo concluido com sucesso")
    # else:
    #     logging.warning("Nenhum dado foi extraido")


if __name__ == "__main__":
    main()
