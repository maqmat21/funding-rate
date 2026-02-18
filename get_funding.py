import requests
import json
import time
import os
import urllib3
from concurrent.futures import ThreadPoolExecutor

# Silenciar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_all_market():
    # Puente para evitar bloqueos de IP en GitHub Actions
    PROXY_BRIDGE = "https://api.codetabs.com/v1/proxy?quest="
    endpoints = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1",
        "https://fapi2.binance.com/fapi/v1"
    ]
    
    # Umbral mínimo para entrar en el radar (0.7%)
    THRESHOLD_RADAR = 0.007 
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def smart_fetch(path):
        """Intenta conexión directa y recurre al proxy si falla."""
        for base in endpoints:
            target_url = f"{base}{path}"
            try:
                res = requests.get(target_url, headers=headers, timeout=10, verify=False)
                if res.status_code == 200: return res.json()
            except:
                try:
                    res = requests.get(f"{PROXY_BRIDGE}{target_url}", headers=headers, timeout=12)
                    if res.status_code == 200: return res.json()
                except: continue
        return None

    print("--- INICIANDO ESCANEO PROFESIONAL (1 MIN CYCLE) ---")
    
    # Obtención de datos maestros de Binance
    data_funding = smart_fetch("/premiumIndex")
    data_tickers = smart_fetch("/ticker/24hr")
    data_liq = smart_fetch("/allForceOrders")

    if not data_funding or not data_tickers:
        print("CRÍTICO: Fallo de conexión con los nodos de datos.")
        return

    # Indexación para acceso rápido (O(1))
    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = {l['symbol'] for l in data_liq} if isinstance(data_liq, list) else set()

    # Cargar base de datos histórica
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    now_ts = int(time.time())
    
    # Filtrar candidatos iniciales (abs(funding) >= 0.7%)
    candidates = [item for item in data_funding if isinstance(item, dict) and 
                  item.get("symbol", "").endswith('USDT') and 
                  abs(float(item.get("lastFundingRate", 0))) >= THRESHOLD_RADAR]

    print(f"Analizando {len(candidates)} candidatos de {len(data_funding)} pares totales...")

    def fetch_oi_data(item):
        """Descarga de Open Interest para procesamiento paralelo."""
        symbol = item.get("symbol")
        oi_data = smart_fetch(f"/openInterest?symbol={symbol}")
        if oi_data and isinstance(oi_data, dict):
            return {
                "symbol": symbol, 
                "oi_raw": float(oi_data.get('openInterest', 0)), 
                "f_rate": float(item.get("lastFundingRate", 0))
            }
        return None

    # Paralelización de peticiones de red (I/O Bound)
    downloaded_data = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(fetch_oi_data, candidates))
        downloaded_data = [r for r in results if r is not None]

    final_results = []
    
    # Procesamiento de lógica de trading y flags
    for data in downloaded_data:
        symbol = data['symbol']
        oi_raw = data['oi_raw']
        f_rate = data['f_rate']
        f_pct = round(f_rate * 100, 4)
        
        ticker = tickers.get(symbol, {})
        price = float(ticker.get('lastPrice', 0)) if ticker else 0.0

        # Mantenimiento de historial para cálculos de cambio %
        if symbol not in history or not isinstance(history[symbol], list):
            history[symbol] = []
        
        history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
        # Limpiar datos antiguos (>24h)
        history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

        def get_pct_change(records, seconds):
            if len(records) < 2: return 0.0
            target_time = now_ts - seconds
            # Encontrar el registro más cercano al punto objetivo
            past_record = min(records, key=lambda x: abs(x['ts'] - target_time))
            if now_ts - past_record['ts'] > (seconds * 1.5): return 0.0 # Data gap too large
            return round(((oi_raw - past_record['oi']) / past_record['oi'] * 100), 2) if past_record['oi'] > 0 else 0.0

        oi_4h_change = get_pct_change(history[symbol], 14400)
        oi_24h_change = get_pct_change(history[symbol], 86400)
        
        # Determinación de tendencia de Funding
        trend = "STABLE"
        if len(history[symbol]) > 1:
            prev_f = history[symbol][-2]['funding']
            if f_pct > prev_f: trend = "INCREASING"
            elif f_pct < prev_f: trend = "DECREASING"

        # REGLA PROFESIONAL: Candidate Squeeze Flag
        candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h_change >= -5

        # Construcción del JSON solicitado
        final_results.append({
            "symbol": symbol,
            "timestamp": now_ts,
            "funding_rate_pct": f_pct,
            "funding_trend": trend,
            "price": price,
            "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)) if ticker else 0.0,
            "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2) if ticker else 0.0,
            "open_interest_usd": round(oi_raw * price, 2),
            "oi_4h_change_pct": oi_4h_change,
            "oi_24h_change_pct": oi_24h_change,
            "liquidation_cluster_detected": symbol in liq_symbols,
            "candidate_squeeze": candidate_squeeze,
            "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
        })

    # Ordenar por severidad de funding (más alto primero)
    final_results = sorted(final_results, key=lambda x: abs(x['funding_rate_pct']), reverse=True)

    # Persistencia de datos
    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_results, f, indent=4)
    
    print(f"--- ESCANEO FINALIZADO: {len(final_results)} ACTIVOS IDENTIFICADOS ---")

if __name__ == "__main__":
    check_all_market()
