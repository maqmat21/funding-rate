import requests
import json
import time
import os
import urllib3
import random
from concurrent.futures import ThreadPoolExecutor

# Silenciar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_all_market():
    # Lista extendida de mirrors de Binance y Proxies de respaldo
    # Usar variaciones de subdominios ayuda a saltar bloqueos de IP
    endpoints = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1",
        "https://fapi2.binance.com/fapi/v1",
        "https://fapi3.binance.com/fapi/v1",
        "https://fapi4.binance.com/fapi/v1",
        "https://fapi5.binance.com/fapi/v1"
    ]
    
    # Umbral de radar (0.7%)
    THRESHOLD_RADAR = 0.007 
    
    # Rotación de User-Agents para evitar detección de bot
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    ]

    def smart_fetch(path):
        """Intenta conexión rotando mirrors y agentes."""
        # Mezclamos los endpoints para no atacar siempre el mismo primero
        current_endpoints = endpoints.copy()
        random.shuffle(current_endpoints)
        
        for base in current_endpoints:
            target_url = f"{base}{path}"
            headers = {
                'User-Agent': random.choice(user_agents),
                'Accept': 'application/json'
            }
            try:
                # Reducimos el timeout para rotar más rápido si un nodo falla
                res = requests.get(target_url, headers=headers, timeout=5, verify=False)
                if res.status_code == 200: 
                    return res.json()
                elif res.status_code == 429:
                    print(f"Rate limit en {base}, saltando...")
                    continue
            except:
                continue
        
        # Último recurso: Proxy AllOrigins si los directos fallan
        try:
            proxy_url = f"https://api.allorigins.win/raw?url={random.choice(endpoints)}{path}"
            res = requests.get(proxy_url, timeout=10)
            if res.status_code == 200: return res.json()
        except: pass
            
        return None

    print("--- INICIANDO ESCANEO PROFESIONAL (DYNAMIC ROTATION) ---")
    
    # Paso 1: Obtención de datos maestros
    data_funding = smart_fetch("/premiumIndex")
    # Si falla el primero, damos un pequeño respiro y reintentamos
    if not data_funding:
        time.sleep(2)
        data_funding = smart_fetch("/premiumIndex")

    data_tickers = smart_fetch("/ticker/24hr")
    data_liq = smart_fetch("/allForceOrders")

    if not data_funding or not data_tickers:
        print("CRÍTICO: Fallo total de conexión. Binance bloqueó los nodos.")
        return

    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = {l['symbol'] for l in data_liq} if isinstance(data_liq, list) else set()

    # Cargar Historial
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    now_ts = int(time.time())
    
    # Filtrar candidatos (abs(funding) >= 0.7%)
    candidates = [item for item in data_funding if isinstance(item, dict) and 
                  item.get("symbol", "").endswith('USDT') and 
                  abs(float(item.get("lastFundingRate", 0))) >= THRESHOLD_RADAR]

    print(f"Analizando {len(candidates)} candidatos de {len(data_funding)} pares totales...")

    def fetch_oi_data(item):
        symbol = item.get("symbol")
        # Para OI usamos una rotación individual
        oi_data = smart_fetch(f"/openInterest?symbol={symbol}")
        if oi_data and isinstance(oi_data, dict):
            return {
                "symbol": symbol, 
                "oi_raw": float(oi_data.get('openInterest', 0)), 
                "f_rate": float(item.get("lastFundingRate", 0))
            }
        return None

    # Procesamiento en paralelo
    downloaded_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_oi_data, candidates))
        downloaded_data = [r for r in results if r is not None]

    final_results = []
    
    for data in downloaded_data:
        symbol = data['symbol']
        oi_raw = data['oi_raw']
        f_rate = data['f_rate']
        f_pct = round(f_rate * 100, 4)
        
        ticker = tickers.get(symbol, {})
        price = float(ticker.get('lastPrice', 0)) if ticker else 0.0

        if symbol not in history or not isinstance(history[symbol], list):
            history[symbol] = []
        
        history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
        history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

        def get_pct_change(records, seconds):
            if len(records) < 2: return 0.0
            target_time = now_ts - seconds
            past_record = min(records, key=lambda x: abs(x['ts'] - target_time))
            if now_ts - past_record['ts'] > (seconds * 1.6): return 0.0 
            return round(((oi_raw - past_record['oi']) / past_record['oi'] * 100), 2) if past_record['oi'] > 0 else 0.0

        oi_4h_change = get_pct_change(history[symbol], 14400)
        oi_24h_change = get_pct_change(history[symbol], 86400)
        
        trend = "STABLE"
        if len(history[symbol]) > 1:
            prev_f = history[symbol][-2]['funding']
            if f_pct > prev_f: trend = "INCREASING"
            elif f_pct < prev_f: trend = "DECREASING"

        # REGLA PROFESIONAL SOLICITADA
        candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h_change >= -5

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

    final_results = sorted(final_results, key=lambda x: abs(x['funding_rate_pct']), reverse=True)

    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_results, f, indent=4)
    
    print(f"--- ESCANEO FINALIZADO: {len(final_results)} MONEDAS ---")

if __name__ == "__main__":
    check_all_market()
