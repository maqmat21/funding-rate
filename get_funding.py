import requests
import json
import time
import os

def check_all_market():
    # Obtenemos la API KEY desde las variables de entorno de GitHub
    api_key = os.getenv("SCRAPER_API_KEY")
    if not api_key:
        print("ERROR: No se encontró la SCRAPER_API_KEY en los Secrets de GitHub.")
        return

    def fetch_professional(endpoint):
        target_url = f"https://fapi.binance.com/fapi/v1{endpoint}"
        # ScraperAPI funciona pasando la URL de Binance como un parámetro
        proxy_url = f"http://api.scraperapi.com?api_key={api_key}&url={target_url}"
        
        try:
            print(f"Pidiendo datos vía ScraperAPI: {endpoint}")
            response = requests.get(proxy_url, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            print(f"Fallo de conexión: {e}")
            return None

    # 1. Obtención de datos masivos
    data_funding = fetch_professional("/premiumIndex")
    data_tickers = fetch_professional("/ticker/24hr")
    
    if not data_funding or not isinstance(data_funding, list):
        print("CRÍTICO: ScraperAPI no pudo obtener los datos.")
        return

    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict)}
    
    # 2. Gestión de Historial
    history_file = "history_db.json"
    history = {}
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}

    final_list = []
    now_ts = int(time.time())

    # 3. Procesamiento
    for item in data_funding:
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("last_funding_rate", item.get("lastFundingRate", 0)))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= 0.7:
            # IMPORTANTE: Para ahorrar créditos de ScraperAPI, no pediremos el OI por separado 
            # en cada vuelta si no es necesario. Por ahora lo dejamos directo.
            oi_data = fetch_professional(f"/openInterest?symbol={symbol}")
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
    print(f"Finalizado con éxito: {len(final_list)} monedas.")

if __name__ == "__main__":
    check_all_market()
