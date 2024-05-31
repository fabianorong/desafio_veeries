import os
import requests
from bs4 import BeautifulSoup


def fetch_page(url, headers):
    """Busca uma página web e retorna seu conteudo"""
    try:
        result = requests.get(url, headers=headers)
        result.raise_for_status()
        return result.content
    except requests.RequestException as e:
        print(f"Erro ao acessar o site: {e}")
        return None


def parse_html(content):
    """Analisa o conteúdo HTML e retorna um objeto BeautifulSoup"""
    return BeautifulSoup(content, "html.parser")


def create_directories(paths):
    """Cria diretórios se eles não existirem"""
    for path in paths:
        os.makedirs(path, exist_ok=True)
