import logging
from .utils import fetch_page, parse_html
from ..config.config_request import ConfigRequest


logger = logging.getLogger(__name__)


def parse_table(content, sentido_cell_value, sentido):
    """
    Extrai os dados da tabela HTML.

    Parâmetros:
    - content: Conteúdo HTML da página.
    - sentido_cell_value: Valor que identifica o sentido na tabela.
    - sentido: Sentido da operação (importação, exportação, etc.).

    Retorna:
    Lista de tuplas contendo os dados extraídos.
    """
    logger.info(f"Iniciando parsing da tabela para o sentido: {sentido}")
    soup = parse_html(content)
    tabela = soup.find_all(
        "table", class_="table table-bordered table-striped table-hover"
    )
    tabela_esperados = tabela[4]  # Obter a tabela de esperados
    conteudo_tabela_esperados = tabela_esperados.find("tbody")
    linhas_tabela_esperados = conteudo_tabela_esperados.find_all("tr")
    data = []

    # Itera sobre as linhas da tabela
    for linha in linhas_tabela_esperados:
        if linha.find("td", string=f"{sentido_cell_value}"):
            cells = linha.find_all("td")
            if len(cells) > 10:
                data.append(extract_data(cells, 11, 12, 15, sentido))
            else:
                data.append(extract_data(cells, 3, 4, 7, sentido))
    logger.info(
        f"Parsing concluído para o sentido: {sentido}, {len(data)} registros encontrados"
    )
    return data


def extract_data(cells, mercadoria_idx, eta_idx, peso_idx, sentido):
    """
    Extrai os dados de cada linha da tabela.

    Parâmetros:
    - cells: Células da linha da tabela.
    - mercadoria_idx: Índice da coluna que contém a informação da mercadoria.
    - eta_idx: Índice da coluna que contém a informação da data estimada de chegada.
    - peso_idx: Índice da coluna que contém a informação do peso.
    - sentido: Sentido da operação (importação, exportação, etc.).

    Retorna:
    Uma tupla contendo os dados extraídos.
    """
    mercadoria = cells[mercadoria_idx].get_text(strip=True)
    eta_full = cells[eta_idx].get_text(strip=True)
    eta_date = eta_full.split()[0]
    peso_full = cells[peso_idx].get_text(strip=True).replace(",", "").replace(".", "")
    peso = int(peso_full.split()[0])
    unidade_peso = peso_full.split()[1]
    return ("Paranagua", sentido, mercadoria, eta_date, peso, unidade_peso)


def scrape_paranagua_data(sentidos):
    """
    Realiza o scraping dos dados de Paranaguá.

    Parâmetros:
    - sentidos: Lista de tuplas contendo os sentidos e seus valores correspondentes.

    Retorna:
    Lista de tuplas contendo os dados extraídos de todas as páginas.
    """
    logger.info("Iniciando scraping de dados de Paranagua")
    all_data = []
    for sentido, sentido_cell_value in sentidos:
        logger.info(f"Iniciando scraping para o sentido: {sentido}")
        content = fetch_page(ConfigRequest.URL, ConfigRequest.HEADERS)
        if content:
            data = parse_table(content, sentido_cell_value, sentido)
            if data:
                all_data.extend(data)
        else:
            logger.warning(f"Falha ao obter conteúdo para o sentido: {sentido}")
    logger.info("Scraping concluído")
    return all_data
