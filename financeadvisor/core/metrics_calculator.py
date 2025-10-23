"""
Agente de cálculo de métricas financeiras.

Este módulo define funções para calcular indicadores quantitativos a
partir dos dados brutos obtidos da brapi.dev.  Na arquitetura
proposta, essas funções são utilizadas pelo worker de métricas
(`workers/metrics_worker.py`).
"""
# Importando as bibliotecas 
from typing import Any, Dict, List # Para anotação e retorno de tipos de dados nas funções
from datetime import datetime, timedelta # Permite manipular datas e tempo
from ..utils.cache import get_redis_connection # Conecta ao Redis

# ---------------------------------------------------------------------------------------------------------------------------------------

def _extract_dividends(brapi_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extrai dados de dividendos do resultado da brapi.
    
    Args:
        brapi_result: resultado da API brapi contendo dividendsData
        
    Returns:
        Lista de dividendos pagos (cashDividends)
    """
    dividends_data = brapi_result.get("dividendsData", {}) # Busca no dicionário retornado pela API a chave "dividendData"
    cash_dividends = dividends_data.get("cashDividends", []) # Na respectiva chave, busca toda a lista de "cashDividends"
    return cash_dividends if isinstance(cash_dividends, list) else [] # Retorna a lista de dividendos

# ---------------------------------------------------------------------------------------------------------------------------------------

def _calculate_dividend_yield(brapi_result: Dict[str, Any]) -> float:
    """Calcula o dividend yield anual baseado nos últimos 12 meses.
    
    Args:
        brapi_result: resultado da API brapi
        
    Returns:
        Dividend yield anual em percentual (ex: 7.5 para 7.5%)
    """
    from datetime import timezone
    
    # Obtém o preço atual
    current_price = brapi_result.get("regularMarketPrice")

    if not current_price or current_price <= 0:
        return 0.0
    
    # Obtém os dividendos
    dividends = _extract_dividends(brapi_result) # Retorna a lista de dividendos 
    if not dividends:
        return 0.0
    
    # Filtra dividendos dos últimos 12 meses (timezone-aware)
    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)
    
    total_dividends = 0.0 # Contador do valor total de dividendos inicializado em 0
    for div in dividends:
        payment_date_str = div.get("paymentDate") # Itera sobre cada dividendo (dicionários) e obtém o valor da chave referente a data de pagamento
        if not payment_date_str:
            continue # Caso o dividendo não tenha data de pagamento, então o iterador prossegue para o próximo índice
            
        try:
            # Parse ISO format: "2025-09-22T00:00:00.000Z"
            payment_date = datetime.fromisoformat(payment_date_str.replace("Z", "+00:00")) # Formata a data para o formato de String 
            
            # Se o pagamento foi nos últimos 12 meses
            if payment_date >= one_year_ago and payment_date <= now:
                rate = div.get("rate", 0)
                if rate:
                    total_dividends += float(rate) # Adiciona a taxa do dividendo ao contador 
        except Exception:
            # Ignora dividendos com data inválida
            continue
    
    # Calcula dividend yield: (dividendos anuais / preço) * 100
    dividend_yield = (total_dividends / current_price) * 100
    return round(dividend_yield, 2)

# ---------------------------------------------------------------------------------------------------------------------------------------

def calc_metrics_from_raw(data_json: Dict[str, Any]) -> Dict[str, float]: # Obtém como parâmetro o dicionário retornado pela API
    """Calcula métricas simples a partir do JSON da brapi.

    Args:
        data_json (dict): resposta JSON da brapi contendo `results[0]` com
            campo `historicalDataPrice` (lista de objetos com 'close') e
            dados de dividendos.

    Returns:
        dict: dicionário com métricas calculadas, incluindo dividend_yield.
    """
    # Extrai preços de fechamento da primeira ação retornada
    try:
        result0 = data_json["results"][0] # Armazena na variável result0 o primeiro índice da chave "results"
    except (KeyError, IndexError):
        raise ValueError("Formato inesperado de dados da brapi.dev: campo 'results[0]' ausente")

    import os as _os
    if _os.getenv("DEBUG_METRICS") == "1":
        print(data_json)
    
    # Calcula dividend yield (métrica principal para analista de dividendos)
    dividend_yield = _calculate_dividend_yield(result0)
    
    # Calcula preço atual e informações básicas
    current_price = result0.get("regularMarketPrice", 0.0)
    ticker = result0.get("symbol", "N/A")
    company_name = result0.get("shortName", "N/A")
    
    # Calcula total de dividendos dos últimos 12 meses
    from datetime import timezone
    
    dividends = _extract_dividends(result0)
    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)
    
    total_dividends_12m = 0.0
    dividend_count = 0
    
    for div in dividends:
        payment_date_str = div.get("paymentDate") # Obtém a data de pagamento do dividendo que está sendo iterado
        if not payment_date_str:
            continue # Caso False então prossegue para o índice seguinte 
            
        try:
            payment_date = datetime.fromisoformat(payment_date_str.replace("Z", "+00:00")) # Formata a data para o formato de String 
            if payment_date >= one_year_ago and payment_date <= now: # Verifica se a data de pagamento do dividendo tem menos de 1 ano
                rate = div.get("rate", 0) # Obtém a taxa do dividendo
                if rate:
                    total_dividends_12m += float(rate) # Adiciona o valor da taxa ao contador 
                    dividend_count += 1 # Adiciona 1 ao contador do total de dividendos com datas de pagamento há menos de 1 ano
        except Exception:
            continue
    
    # Retorna um dicionário com as informações
    return {
        "dividend_yield": dividend_yield,
        "preco_atual": float(current_price),
        "dividendos_12m": round(total_dividends_12m, 2),
        "quantidade_pagamentos": float(dividend_count),
    }

# ---------------------------------------------------------------------------------------------------------------------------------------

# Função para armazenamento temporario de consultas na memória cache (Redis)
def store_metrics_in_cache(ticker: str, periodo: str, metrics: Dict[str, float], ttl: int = 86400) -> None:
    """Armazena métricas no Redis para uso futuro.

    Args:
        ticker (str): código do ativo.
        periodo (str): intervalo analisado.
        metrics (dict): dicionário com métricas calculadas.
        ttl (int): tempo de vida em segundos (default 24h).
    """
    import json
    r = get_redis_connection() # Faz a conexão com o Redis
    key = f"metrics:{ticker}:{periodo}" # Monta a chave de pesquisa
    r.set(key, json.dumps(metrics), ex=ttl) # Insere no cache, identificado por essa chave de pesquisa, o objeto JSON (dicionário), com um tempo definido para expiração

# ---------------------------------------------------------------------------------------------------------------------------------------

def enqueue_metrics_calculation(ticker: str, periodo: str) -> None:
    """Mantido por compatibilidade: não faz nada no fluxo síncrono."""
    return None