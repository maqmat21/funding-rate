import requests
import json
import time
import os

def check_all_market():
    # Usaremos este proxy que suele ser más estable para APIs de Crypto
    proxy_url = "https://api.allorigins.win/raw?url="
    base_url = "https://fapi.binance.com/fapi/v1"
    
    threshold = 0.007
    # User-Agent de un navegador real para evitar el bloqueo instantáneo
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    def fetch(endpoint):
        try:
            # Intentamos vía Proxy AllOrigins
            full_url = f"{proxy_url}{base_url}{endpoint}"
            print(f"Pidiendo: {endpoint}...")
            response = requests.get(full_url, headers=headers, timeout=20)
            
            if response.status_code == 200:
                return response.json()
            else:
                # Si el proxy falla, intentamos conexión directa (a veces fapi1 funciona)
                print(f"Proxy falló ({response.status_code}). Intentando directo...")
                direct_url = f"https://fapi1.binance.com/fapi/v1{endpoint}"
                res_direct = requests.get(direct_url, headers=headers, timeout=10)
                return res_direct.json()
        except Exception as e:
            print(f"Error en {endpoint}: {e}")
            return None

    # 1. Obtener datos base
    data_funding = fetch("/premiumIndex")
    data_tickers = fetch("/ticker/24hr")
    
    if not data_funding or not isinstance(data_funding, list):
        print("CRÍTICO: No se pudo obtener la lista de Funding.")
        return

    # 2. Procesar Tickers
    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict)} if data_tickers else {}

    # 3. Cargar Historial
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    final_results = []
    now_ts = int(time.time())

    # Solo procesamos las primeras 15 monedas con funding más alto para evitar que el proxy nos corte por exceso de peticiones
    sorted_funding = sorted(data_funding, key=lambda x: abs(float(x.get('lastFundingRate', 0))), reverse=True)

    for item in sorted_funding:
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= 0.7:
            # Solo pedimos Open Interest si pasa el filtro para ahorrar "puntos" de API
            oi_data = fetch(f"/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0
            
            # --- Gestión de Historial ---
            if symbol not in history or not isinstance(history[symbol], list):
                history[symbol] = []
            
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            # Cambios
            def calc_change(records, secs):
                if len(records) < 2: return 0.0
                target = now_ts - secs
                past = min(records, key=lambda x: abs(x['ts'] - target))
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = calc_change(history[symbol], 14400)
            
            ticker = tickers.get(symbol, {})
            price = float(ticker.get('lastPrice', 0))
            
            final_results.append({
                "symbol": symbol,
                "timestamp": now_ts,
                "funding_rate_pct": f_pct,
                "funding_trend": "STABLE", # Se actualizará en la siguiente corrida
                "price": price,
                "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)),
                "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2),
                "open_interest_usd": round(oi_raw * price, 2),
                "oi_4h_change_pct": oi_4h,
                "oi_24h_change_pct": calc_change(history[symbol], 86400),
                "liquidation_cluster_detected": False,
                "candidate_squeeze": abs(f_pct) >= 1.8 and oi_4h >= -5,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })
            
            # Límite de seguridad: no pedir más de 20 monedas para evitar BAN
            if len(final_results) >= 20: break

    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_results, f, indent=4)
    print(f"Escaneo finalizado: {len(final_results)} monedas.")

if __name__ == "__main__":
    check_all_market()
