import requests
import json
import datetime
import time
import os

def check_all_market():
    # Endpoints de respaldo
    endpoints = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1",
        "https://fapi2.binance.com/fapi/v1"
    ]
    
    # Proxies de respaldo (Añadido un mirror de Heroku)
    proxies = [
        "https://api.codetabs.com/v1/proxy?quest=",
        "https://api.allorigins.win/raw?url=",
        "" # Intento sin proxy por si acaso
    ]
    
    threshold = 0.007
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def smart_fetch(path):
        """Intenta todas las combinaciones de proxy y endpoint hasta que una sirva."""
        for proxy in proxies:
            for base in endpoints:
                try:
                    url = f"{proxy}{base}{path}"
                    print(f"Probando: {url[:60]}...")
                    res = requests.get(url, headers=headers, timeout=15)
                    if res.status_code == 200:
                        data = res.json()
                        if data: return data
                except:
                    continue
        return None

    print("--- INICIANDO ESCANEO DE MERCADO ---")
    data_funding = smart_fetch("/premiumIndex")
    data_tickers = smart_fetch("/ticker/24hr")
    data_liq = smart_fetch("/allForceOrders")

    if not data_funding or not data_tickers:
        print("CRÍTICO: Todos los proxies y endpoints fallaron.")
        return

    # Mapeo de datos
    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = [l['symbol'] for l in data_liq] if isinstance(data_liq, list) else []

    # Gestión de Historial
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    final_results = []
    now_ts = int(time.time())

    for item in data_funding:
        if not isinstance(item, dict): continue
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= threshold:
            # Obtener OI con el mismo sistema de reintentos
            oi_data = smart_fetch(f"/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0

            if symbol not in history or not isinstance(history[symbol], list):
                history[symbol] = []
            
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            def get_change(records, seconds):
                if len(records) < 2: return 0.0
                target = now_ts - seconds
                past = min(records, key=lambda x: abs(x['ts'] - target))
                if now_ts - past['ts'] > (seconds * 0.7): return 0.0 
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = get_change(history[symbol], 14400)
            oi_24h = get_change(history[symbol], 86400)
            
            # Tendencia
            trend = "STABLE"
            if len(history[symbol]) > 1:
                prev_f = history[symbol][-2]['funding']
                if f_pct > prev_f: trend = "INCREASING"
                elif f_pct < prev_f: trend = "DECREASING"

            candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h >= -5
            ticker = tickers.get(symbol, {})
            
            final_results.append({
                "symbol": symbol,
                "timestamp": now_ts,
                "funding_rate_pct": f_pct,
                "funding_trend": trend,
                "price": float(ticker.get('lastPrice', 0)),
                "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)),
                "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2),
                "open_interest_usd": round(oi_raw * float(ticker.get('lastPrice', 0)), 2),
                "oi_4h_change_pct": oi_4h,
                "oi_24h_change_pct": oi_24h,
                "liquidation_cluster_detected": symbol in liq_symbols,
                "candidate_squeeze": candidate_squeeze,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })

    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_results, f, indent=4)
    print(f"--- ESCANEO FINALIZADO: {len(final_results)} MONEDAS ---")

if __name__ == "__main__":
    check_all_market()
