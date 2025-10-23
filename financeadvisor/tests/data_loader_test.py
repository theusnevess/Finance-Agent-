import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from financeadvisor.core.data_loader import fetch_brapi_data

# Teste com ticker gratuito e período
data = fetch_brapi_data("PETR4", "1mo")

print(data.keys()) # Deve mostrar "results"
print(data["results"][0].keys()) # Deve ter 'historicalDataPrice' ou 'prices'