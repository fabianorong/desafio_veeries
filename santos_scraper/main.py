import logging
from scripts.scraper import scrape_santos_data
from scripts.data_processing import save_to_database, save_to_csv, save_combined_data
from scripts.utils import create_directories
import os

# Configuração de logging
# Define o diretório de saída dos logs
output_log_dir = os.path.join("santos_scraper", "logs")
create_directories([output_log_dir])
log_file = os.path.join(output_log_dir, "scraper.log")

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)


def main():
    logging.info("Iniciando o processo de scraping dos dados do Porto de Santos")
    try:
        time_sleep = 5  # Tempo de espera para o carregamento da página
        import_table_number = 4
        export_table_number = 5

        # Scrape de dados de importação
        data_import = scrape_santos_data(time_sleep, import_table_number, "import")
        save_to_database(data_import, "import")
        save_to_csv(data_import, "import")

        # Scrape de dados de exportação
        data_export = scrape_santos_data(time_sleep, export_table_number, "export")
        save_to_database(data_export, "export")
        save_to_csv(data_export, "export")

        # Combinação dos dados de importação e exportação
        save_combined_data(data_import, data_export)

        logging.info("Scraping e arquivos exportados concluído com sucesso")
    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")


if __name__ == "__main__":
    main()
