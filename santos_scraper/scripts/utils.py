import os


def create_directories(paths):
    """
    Cria diretórios se não existirem
    """
    for path in paths:
        os.makedirs(path, exist_ok=True)
