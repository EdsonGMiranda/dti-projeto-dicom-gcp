import os
import re

def clean_filename(filepath: str) -> str:
    """
    Remove barras, pastas, dashes (-), underscores (_) do nome do arquivo.
    
    Args:
    - filepath (str): O caminho completo ou nome do arquivo para ser limpo.
    
    Returns:
    - str: O nome do arquivo limpo.
    """
    # Extrair apenas o nome do arquivo, removendo o caminho (se houver)
    filename = os.path.basename(filepath)
    
    # Remove dashes, underscores, dots
    cleaned_filename = re.sub(r'[-_]', '', filename)
    
    return cleaned_filename