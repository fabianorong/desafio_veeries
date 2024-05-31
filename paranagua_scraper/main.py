import os
import logging
from datetime import datetime
from scripts.scraper import scrape_paranagua_data
from scripts.data_processing import save_to_database, save_to_csv
from scripts.utils import create_directories


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

    # Define os sentidos para scraping
    sentidos = [("Import", "Imp"), ("Export", "Exp"), ("ImportExport", "Imp/Exp")]

    # Realiza o scraping dos dados para os sentidos definidos
    all_data = scrape_paranagua_data(sentidos)

    # Se houver dados extraídos
    if all_data:
        # Gera um timestamp para os nomes dos arquivos
        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")

        # Define os nomes dos arquivos CSV e de banco de dados
        csv_filename = f"paranagua__{timestamp}.csv"
        db_filename = f"paranagua__{timestamp}.csv"

        # Define os diretórios de saída para o banco de dados e CSV
        output_db_dir = os.path.join("paranagua_scraper", "data", "db")
        output_csv_dir = os.path.join("paranagua_scraper", "data", "csv")

        # Cria os diretórios de saída, se não existirem
        create_directories([output_db_dir, output_csv_dir])

        # Define os caminhos completos dos arquivos
        db_path = os.path.join(output_db_dir, db_filename)
        csv_path = os.path.join(output_csv_dir, csv_filename)

        # Salva os dados no banco de dados e no arquivo CSV
        save_to_database(all_data, db_path)
        save_to_csv(all_data, csv_path)
        logging.info("Processo concluido com sucesso")
    else:
        logging.warning("Nenhum dado foi extraido")


if __name__ == "__main__":
    main()
