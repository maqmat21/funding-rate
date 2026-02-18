import requests
import json
import time
import os
import urllib3
import random
from concurrent.futures import ThreadPoolExecutor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_all_market():
    # Estrategia de Redundancia: Lista de puentes y mirrors directos
    bridges = [
        "https://api.allorigins.win/raw?url=",
        "https://api.codetabs.com/v1/proxy?quest=",
        "https://thingproxy.freeboard.io/fetch/"
    ]
    
    binance_mirrors = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1",
        "https://fapi2.binance.com/fapi/v1",
        "https://fapi3.binance.com/fapi/v1"
    ]
    
    THRESHOLD_RADAR = 0.007
    
    def smart_fetch(path):
        """Intenta conexión directa, luego por mirrors, luego por 3 puentes distintos."""
        # 1. Intento Directo + Mirrors
        random.shuffle(binance_mirrors)
        for base in binance_mirrors:
            try:
                url = f"{base}{path}"
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0'}
                res = requests.get(url, headers=headers, timeout=5, verify=False)
                if res.status_code == 200: return res.json()
            except: continue

        # 2. Intento vía Puentes (Failover)
        for bridge in bridges:
            try:
                target = f"{random.choice(binance_mirrors)}{path}"
                res = requests.get(f"{bridge}{target}", timeout=10)
                if res.status_code == 200: return res.json()
            except: continue
        return None

    print("--- INICIANDO ESCANEO DE ALTA DISPONIBILIDAD (1 MIN) ---")
    
    # Obtención de datos con reintentos
    data_funding = smart_fetch("/premiumIndex")
    data_tickers = smart_fetch("/ticker/24hr")
    data_liq = smart_fetch("/allForceOrders")

    if not data_funding or not data_tickers:
        print("CRÍTICO: No se pudo establecer conexión segura con Binance/Puentes.")
        return

    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = {l['symbol'] for l in data_liq} if isinstance(data_liq, list) else set()

    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    now_ts = int(time.time())
    
    # Filtrar Candidatos (abs >= 0.7%)
    candidates = [item for item in data_funding if isinstance(item, dict) and 
                  item.get("symbol", "").endswith('USDT') and 
                  abs(float(item.get("lastFundingRate", 0))) >= THRESHOLD_RADAR]

    print(f"Pares totales: {len(data_funding)} | Candidatos >0.7%: {len(candidates)}")

    def fetch_worker(item):
        symbol = item.get("symbol")
        oi_data = smart_fetch(f"/openInterest?symbol={symbol}")
        if oi_data and isinstance(oi_data, dict):
            return {"symbol": symbol, "oi": float(oi_data.get('openInterest', 0)), "f_rate": float(item.get("lastFundingRate", 0))}
        return None

    # Procesamiento Paralelo con menos hilos para evitar baneo por exceso de peticiones
    downloaded = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(fetch_worker, candidates))
        downloaded = [r for r in results if r is not None]

    final_results = []
    for data in downloaded:
        symbol, oi_raw, f_rate = data['symbol'], data['oi'], data['f_rate']
        f_pct = round(f_rate * 100, 4)
        ticker = tickers.get(symbol, {})
        price = float(ticker.get('lastPrice', 0)) if ticker else 0.0

        if symbol not in history or not isinstance(history[symbol], list):
            history[symbol] = []
        
        history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
        history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

        def get_pct_change(records, seconds):
            if len(records) < 2: return 0.0
            target = now_ts - seconds
            past = min(records, key=lambda x: abs(x['ts'] - target))
            if now_ts - past['ts'] > (seconds * 1.8): return 0.0 
            return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

        oi_4h = get_pct_change(history[symbol], 14400)
        oi_24h = get_pct_change(history[symbol], 86400)
        
        trend = "STABLE"
        if len(history[symbol]) > 1:
            prev_f = history[symbol][-2]['funding']
            trend = "INCREASING" if f_pct > prev_f else "DECREASING"

        # REGLA: Candidate Squeeze
        candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h >= -5

        final_results.append({
            "symbol": symbol,
            "timestamp": now_ts,
            "funding_rate_pct": f_pct,
            "funding_trend": trend,
            "price": price,
            "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)) if ticker else 0.0,
            "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2) if ticker else 0.0,
            "open_interest_usd": round(oi_raw * price, 2),
            "oi_4h_change_pct": oi_4h,
            "oi_24h_change_pct": oi_24h,
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
