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
    """Monta a URL para a API brapi.dev com parâmetros de interesse""" 

    """Monta a query adicionando automaticamente incluindo parâmetros essenciais (range, fundamental=true, dividends=true), 
    e o token de autenticação se disponível"""

    base_url = f"https://brapi.dev/api/quote/{ticker}"
    params = {
        "range": periodo, 
        "fundamental": True, 
        "dividends": True
    }

    token = os.getenv("BRAPI_TOKEN")
    if token: 
        params["token"] = token

    # Concatena os parâmetros na URL
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{query}"

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
    query = "&".join([f"{k}={v}" for k, v in params.items()])
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
    # Modo fake para bootcamp/testes offline
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
    url = build_api_url(ticker, periodo)
    # Headers para evitar erro 417 (Expectation Failed)
    headers = {
        'User-Agent': 'FinanceAdvisor/1.0',
        'Accept': 'application/json',
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    # Se a resposta não contém série histórica, tenta o endpoint /history e injeta em results[0]
    try:
        result0 = data.get("results", [])[0]
    except Exception:
        result0 = None
    has_history = False
    if isinstance(result0, dict):
        series = result0.get("historicalDataPrice") or result0.get("historicalData") or result0.get("prices")
        has_history = isinstance(series, list) and len(series) > 0
    if not has_history:
        hurl = build_brapi_history_url(ticker, periodo)
        hresp = requests.get(hurl, headers=headers)
        hresp.raise_for_status()
        hjson = hresp.json() or {}
        prices = hjson.get("prices") or hjson.get("historicalDataPrice") or []
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






