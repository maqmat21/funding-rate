import requests
import json
import time
import os

def check_all_market():
    # Usamos fapi1 que es un mirror con menos restricciones que el principal
    base_url = "https://fapi1.binance.com/fapi/v1"
    
    # Lista de proxies: si el directo falla, probamos uno, si no el otro.
    # He añadido 'corsproxy.io' que es más rápido que los anteriores.
    proxies = [
        "", # Intento directo primero (en fapi1 a veces funciona)
        "https://api.allorigins.win/raw?url=",
        "https://corsproxy.io/?"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    def fetch_data(endpoint):
        for p in proxies:
            try:
                url = f"{p}{base_url}{endpoint}"
                print(f"Intentando: {url}")
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    return response.json()
            except:
                continue
        return None

    # 1. Obtener Datos
    data_funding = fetch_data("/premiumIndex")
    data_tickers = fetch_data("/ticker/24hr")
    
    if not data_funding or not isinstance(data_funding, list):
        print("ERROR: No se pudo conectar con Binance tras agotar proxies.")
        return

    # 2. Diccionarios de apoyo
    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict)} if data_tickers else {}

    # 3. Historial ( history_db.json )
    history_file = "history_db.json"
    history = {}
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f:
                history = json.load(f)
        except:
            history = {}

    final_list = []
    now_ts = int(time.time())

    # 4. Procesar (Solo monedas con Funding >= 0.7%)
    for item in data_funding:
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= 0.7:
            # Para el Open Interest, usamos el mismo sistema de proxies
            oi_data = fetch_data(f"/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0
            
            # Gestionar historial para este símbolo
            if symbol not in history or not isinstance(history[symbol], list):
                history[symbol] = []
            
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            # Calcular OI Change (4h y 24h)
            def calc_change(secs):
                if len(history[symbol]) < 2: return 0.0
                target = now_ts - secs
                past = min(history[symbol], key=lambda x: abs(x['ts'] - target))
                if now_ts - past['ts'] < (secs * 0.1): return 0.0
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = calc_change(14400)
            ticker = tickers.get(symbol, {})
            price = float(ticker.get('lastPrice', 0))

            final_list.append({
                "symbol": symbol,
                "timestamp": now_ts,
                "funding_rate_pct": f_pct,
                "funding_trend": "STABLE", # Se compara en la siguiente vuelta
                "price": price,
                "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)),
                "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2),
                "open_interest_usd": round(oi_raw * price, 2),
                "oi_4h_change_pct": oi_4h,
                "oi_24h_change_pct": calc_change(86400),
                "liquidation_cluster_detected": False,
                "candidate_squeeze": abs(f_pct) >= 1.8 and oi_4h >= -5,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })

    # Guardar archivos
    with open(history_file, "w") as f:
        json.dump(history, f)
    with open("high_funding.json", "w") as f:
        json.dump(final_list, f, indent=4)
    
    print(f"Éxito. {len(final_list)} monedas guardadas.")

if __name__ == "__main__":
    check_all_market()
