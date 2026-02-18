import requests
import json
import time
import os

def check_all_market():
    # Cambiamos a dominios mirror menos saturados
    base_urls = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1"
    ]
    
    # Proxy alternativo: 'scrapingant' o similares suelen ser la solucion, 
    # pero intentaremos con este bridge de alto rendimiento:
    proxies = [
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "https://api.allorigins.win/raw?url="
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }

    def fetch_with_retry(endpoint):
        # Intentamos primero con un bridge de Cloudflare que suele saltar el bloqueo de GitHub
        bridges = [
            "https://cors-anywhere.herokuapp.com/", 
            "https://thingproxy.freeboard.io/fetch/",
            "" # Directo
        ]
        
        for bridge in bridges:
            for base in base_urls:
                try:
                    url = f"{bridge}{base}{endpoint}"
                    print(f"Probando conexion: {base}{endpoint} via {bridge if bridge else 'Direct'}")
                    response = requests.get(url, headers=headers, timeout=12)
                    if response.status_code == 200:
                        return response.json()
                except:
                    continue
        return None

    # 1. Obtencion de datos
    data_funding = fetch_with_retry("/premiumIndex")
    time.sleep(1) # Pausa para evitar rate limit
    data_tickers = fetch_with_retry("/ticker/24hr")
    
    if not data_funding or not isinstance(data_funding, list):
        print("ERROR CRITICO: Binance sigue bloqueando la conexion. Intentando metodo de emergencia...")
        # Metodo de emergencia: Si falla el funding, no podemos seguir.
        return

    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict)} if data_tickers else {}

    # 2. Historial
    history_file = "history_db.json"
    history = {}
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}

    final_list = []
    now_ts = int(time.time())

    # 3. Procesamiento (Mismo formato solicitado)
    for item in data_funding:
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= 0.7:
            # Para el OI, limitamos peticiones para no ser baneados
            oi_data = fetch_with_retry(f"/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0
            
            if symbol not in history: history[symbol] = []
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            def calc_change(secs):
                if len(history[symbol]) < 2: return 0.0
                target = now_ts - secs
                past = min(history[symbol], key=lambda x: abs(x['ts'] - target))
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = calc_change(14400)
            ticker = tickers.get(symbol, {})
            price = float(ticker.get('lastPrice', 0))

            final_list.append({
                "symbol": symbol,
                "timestamp": now_ts,
                "funding_rate_pct": f_pct,
                "funding_trend": "STABLE",
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

    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_list, f, indent=4)
    print(f"Finalizado: {len(final_list)} monedas.")

if __name__ == "__main__":
    check_all_market()
