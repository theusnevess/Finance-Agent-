# Importando as bibliotecas 
import os # Acessa as variáveis de ambiente 
from typing import Any, Dict # Para anotação e retorno de tipos de dados nas funções
import requests # Faz requisições HTTP para a API brapi.dev

try: 
    from dotenv import load_dotenv # Carrega automaticamente as variáveis de ambiente no arquivo .env 
except ImportError: 
    load_dotenv = None 

# from ..utils.cache import get_redis_connection # acessa o cache Redis para reaproveitar dados 

if load_dotenv: # Garante que as variáveis de ambiente estejam disponíveis
    load_dotenv() # Cria a instância do objeto load_dotenv

# ---------------------------------------------------------------------------------------------------------------------------------------

def build_api_url(ticker: str, periodo: str) -> str: 
    """Monta a URL para a API brapi.dev com os parâmetros de interesse""" 

    """Monta a query automaticamente, incluindo parâmetros essenciais (range, fundamental=true, dividends=true), 
    e o token de autenticação se disponível"""

    base_url = f"https://brapi.dev/api/quote/{ticker}"

    # Parâmetros utilizados para montar a URL
    params = {
        "range": periodo, 
        "fundamental": True, 
        "dividends": True
    }

    token = os.getenv("BRAPI_TOKEN") # A utilização do token permite melhores buscas (em quantidade de requisições e variedade de opções)

    if token: 
        params["token"] = token # Caso haja o token da chave de api da brapi, então ele é adicionado ao dicionário de parâmetros da URL

    query = "&".join([f"{k}={v}" for k, v in params.items()]) # Monta a query com base nos parâmetros 
    return f"{base_url}?{query}" # Concatena a query na URL base

# ---------------------------------------------------------------------------------------------------------------------------------------

def build_brapi_history_url(ticker: str, periodo: str) -> str: 
    """Monta a URL do endpoint de histórico diário da brapi.dev.

    Ex.: https://brapi.dev/api/quote/PETR4?range=1y"""

    base_url = f"https://brapi.dev/api/quote/{ticker}"
    params = {
        "range": periodo
    }

    token = os.getenv("BRAPI_TOKEN")

    if token: 
        params["token"] = token

    query = "&".join([f"{k}={v}" for k, v in params.items()]) # Monta a query somente com o range (período) e o token (se houver)
    return f"{base_url}?{query}"

# ---------------------------------------------------------------------------------------------------------------------------------------

def fetch_brapi_data(ticker: str, periodo: str) -> Dict[str, Any]:
    """Faz uma requisição HTTP à brapi.dev e retorna o JSON.

    Esta função é utilizada pelo worker de ingestão.  Se preferir
    realizar testes locais sem filas, você pode chamá‑la diretamente.

    Args:
        ticker (str): código do ativo (ex.: 'PETR4').
        periodo (str): intervalo (ex.: '1mo', '1y').

    Returns:
        dict: resposta JSON da brapi
    """

    # FAKE_DATA é utilizado para testes, com uma "simulação" de buscas off-line
    if os.getenv("FAKE_DATA") == "1":
        return {
            "results": [
                {
                    "historicalDataPrice": [
                        {"close": 10.0},
                        {"close": 10.2},
                        {"close": 10.1},
                        {"close": 10.5},
                    ]
                }
            ]
        }
    
    url = build_api_url(ticker, periodo) # Monta a URL com base nos parâmetros passados na função

    # Headers para evitar erro 417 (Expectation Failed)
    headers = {
        'User-Agent': 'FinanceAdvisor/1.0', # Define o nome de usuário para acesso a API
        'Accept': 'application/json', # Define o tipo de dado para retorno
    }

    # Faz a requisição à API 
    resp = requests.get(url, headers=headers) # Passa a URL e os headers para requisição dos dados 
    resp.raise_for_status() # Caso não haja nenhum erro na conexão ou retorno da API, o script continua 
    data = resp.json() # Transforma a resposta da API em um dicionário Python

    # Se a resposta não contém série histórica, tenta o endpoint/history e injeta em results[0]
    try:
        result0 = data.get("results", [])[0] # Tenta buscar o primeiro índice na lista de dicionários na chave "results"
    except Exception:
        result0 = None # Caso a lista esteja vazia, então retorna None 
    has_history = False

    if isinstance(result0, dict): # Verifica se o índice 0 da lista oriunda da chave "results" é um dicionário
        series = result0.get("historicalDataPrice") or result0.get("historicalData") or result0.get("prices") # Busca por uma key (não vazia) com algum dos nomes inseridos na condicional
        has_history = isinstance(series, list) and len(series) > 0 # Caso encontre, True é atribuída a variável has_history

    if not has_history: # A condicional é acessada caso o valor da variável seja False 
        hurl = build_brapi_history_url(ticker, periodo) # É feita uma nova busca, mas utilizando a função secundária
        hresp = requests.get(hurl, headers=headers) # É requisitado uma nova busca 
        hresp.raise_for_status() # Caso não haja erro, o script continua 
        hjson = hresp.json() or {} # Retorna a resposta no formato dicionário
        prices = hjson.get("prices") or hjson.get("historicalDataPrice") or [] # Busca na resposta da API alguma das keys e armazena na variável 
        # É atribuído ao dicionário principal, no valor da lista oriunda da chave "results" (result0), essa nova "key"
        if isinstance(result0, dict):
            result0["prices"] = prices 
        elif isinstance(data.get("results"), list) and data["results"]:
            data["results"][0] = {"prices": prices}
        else:
            data = {"results": [{"prices": prices}]}
    return data

# ---------------------------------------------------------------------------------------------------------------------------------------

def enqueue_ingestion(ticker: str, periodo: str) -> None:
    """Mantido por compatibilidade: não faz nada no fluxo síncrono."""
    return None

# ---------------------------------------------------------------------------------------------------------------------------------------

def get_rawdata_from_cache(ticker: str, periodo: str):
    """Recupera dados brutos do Redis se estiverem em cache.

    Args:
        ticker (str): código do ativo.
        periodo (str): intervalo.

    Returns:
        dict ou None: JSON se presente no cache ou None caso contrário.
    """
    #r = get_redis_connection() # Faz a conexão com o Redis
    #key = f"rawdata:{ticker}:{periodo}" # Monta a key para busca no Redis
    #data = r.get(key) # Faz a busca com base na key
    #if data:
        #import json
        #return json.loads(data) # Caso haja uma resposta, ela é transformada no formato dicionário do Python
    #return None # Caso retorne None, então a busca será realizada na API 









