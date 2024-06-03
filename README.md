# Projeto de Web Scraping do Porto de Paranaguá e Santos

Este projeto realiza a extração de dados do Porto de Paranaguá, processa as informações e salva os resultados em um banco de dados SQLite e em um arquivo CSV.

O objetivo é gerar uma tabela com o volume diário previsto por produto e sentido (exportação e importação).


## Pré-requisitos

- Python 3.7 ou superior
- Pacotes listados no arquivo `requirements.txt`

## Instalação

1. Clone o repositório para a sua máquina local:
   ```sh
   git clone https://github.com/fabianorong/desafio_veeries.git
   cd desafio_veeries

2. Crie e ative um ambiente virtual:
   ```sh
   python -m venv venv
   source venv/bin/activate  # No Windows, use `venv\Scripts\activate`

3. Instale as dependências:
   ```sh
   pip install -r requirements.txt
   ```

## Configuração 
Certifique-se de que as configurações de requisição estão corretas no arquivo config/config_request.py:
```python
class ConfigRequest:
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    }
    URL = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"

```

## Uso
Para executar o script principal e realizar o scraping, processamento e salvamento dos dados, execute:
```sh
python main.py
```
Os dados serão salvos nos diretórios:
- Para o porto de santos: santos_scraper/data/csv e santos_scraper/data/db
- Para o porto de paranagua: paranagua_scraper/data/csv e paranagua_scraper/data/db
- Para os dois portos combinados: combined_data/csv e combined_data/db
- E os logs serão registrados no diretório logs.

## Estrutura dos Scripts
- main.py: Script principal que organiza o fluxo de scraping, processamento e salvamento dos dados.
- scripts/scraper.py: Contém funções para realizar o scraping das páginas HTML.
- scripts/data_processing.py: Contém funções para salvar os dados no banco de dados e em arquivos CSV.
- scripts/utils.py: Funções utilitárias como fetch_page, parse_html e create_directories.
- config/config_request.py: Contém as configurações de requisição HTTP.


