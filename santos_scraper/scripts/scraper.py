from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from ..config.config import CHROME_PATH
from ..config.config_request import ConfigRequest
import time
import logging

logger = logging.getLogger(__name__)


def scrape_santos_data(time_sleep, table_number, sentido):
    """
    Parâmetros:
    - time_sleep: Tempo para aguardar o carregamento da página.
    - table_number: Número da tabela a ser raspada.
    - sentido: Sentido da operação (importação, exportação, etc.).

    Retorna:
    - Lista de dados raspados.
    """
    logger.info(f"Iniciando scraping para o sentido: {sentido}")
    data = []

    try:
        options = webdriver.ChromeOptions()
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        service = ChromeService(executable_path=CHROME_PATH)
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(ConfigRequest.URL)
        time.sleep(time_sleep)

        xpath = f"((//table)[{table_number}])/tbody/tr"
        santos_tabela_selecionada = driver.find_elements(By.XPATH, xpath)

        for row in santos_tabela_selecionada:
            mercadoria = row.find_element(By.XPATH, "./td[9]").text
            eta_full = row.find_element(By.XPATH, "./td[5]").text
            eta_date = eta_full.split()[0]
            peso = int(
                row.find_element(By.XPATH, "./td[10]")
                .text.replace(",", "")
                .replace(".", "")
            )

            data.append(("Santos", sentido, mercadoria, eta_date, peso, "Tons"))
    except Exception as e:
        logger.error(f"Erro ao fazer scraping: {e}")
    finally:
        driver.quit()

    logger.info(
        f"Scraping concluído para o sentido: {sentido} com {len(data)} registros."
    )
    return data
