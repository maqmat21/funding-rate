import requests
import json
import datetime
import time
import os

def check_all_market():
    url_funding = "https://fapi.binance.com/fapi/v1/premiumIndex"
    url_tickers = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    url_orders = "https://fapi.binance.com/fapi/v1/allForceOrders"
    
    proxies_list = [
        "https://api.codetabs.com/v1/proxy?quest=",
        "http://api.allorigins.win/get?url="
    ]
    
    threshold = 0.007
    headers = {'User-Agent': 'Mozilla/5.0'}

    def fetch_data(target_url):
        for proxy in proxies_list:
            try:
                full_url = f"{proxy}{target_url}"
                res = requests.get(full_url, headers=headers, timeout=25)
                if res.status_code == 200:
                    r_json = res.json()
                    if isinstance(r_json, dict) and 'contents' in r_json:
                        return json.loads(r_json['contents'])
                    return r_json
            except:
                continue
        return None

    print("Descargando datos de mercado...")
    data_funding = fetch_data(url_funding)
    data_tickers = fetch_data(url_tickers)
    data_liq = fetch_data(url_orders)

    if not data_funding or not data_tickers:
        print("Error: No se pudieron obtener datos. Verifica tu conexión o el proxy.")
        return

    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = [l['symbol'] for l in data_liq] if isinstance(data_liq, list) else []

    # Gestión de Historial (Memoria)
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f:
                history = json.load(f)
        except: history = {}
    else:
        history = {}

    final_results = []
    now_ts = int(time.time())

    for item in data_funding:
        if not isinstance(item, dict): continue
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_pct) >= 0.7:
            print(f"Procesando {symbol}...")
            ticker = tickers.get(symbol, {})
            price = float(ticker.get('lastPrice', 0))
            
            # Obtener Open Interest real
            oi_data = fetch_data(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0

            # --- VALIDACIÓN Y ACTUALIZACIÓN DE HISTORIAL ---
            # Si el historial es del formato viejo (dict), lo reseteamos a lista
            if symbol not in history or not isinstance(history[symbol], list):
                history[symbol] = []
            
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            # Mantener solo las últimas 24 horas (86400 seg)
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            # Cálculo de Cambios
            def get_change(records, seconds):
                if len(records) < 2: return 0.0
                target = now_ts - seconds
                past = min(records, key=lambda x: abs(x['ts'] - target))
                if now_ts - past['ts'] > (seconds * 0.5): return 0.0 
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = get_change(history[symbol], 14400)
            oi_24h = get_change(history[symbol], 86400)
            
            # Tendencia
            trend = "STABLE"
            if len(history[symbol]) > 1:
                prev_f = history[symbol][-2]['funding']
                if f_pct > prev_f: trend = "INCREASING"
                elif f_pct < prev_f: trend = "DECREASING"

            # REGLAS SOLICITADAS
            candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h >= -5
            liq_cluster = symbol in liq_symbols and abs(float(ticker.get('priceChangePercent', 0))) > 5

            final_results.append({
                "symbol": symbol,
                "timestamp": now_ts,
                "funding_rate_pct": f_pct,
                "funding_trend": trend,
                "price": price,
                "price_24h_change_pct": float(ticker.get('priceChangePercent', 0)),
                "volume_24h_usd": round(float(ticker.get('quoteVolume', 0)), 2),
                "open_interest_usd": round(oi_raw * price, 2),
                "oi_4h_change_pct": oi_4h,
                "oi_24h_change_pct": oi_24h,
                "liquidation_cluster_detected": liq_cluster,
                "candidate_squeeze": candidate_squeeze,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })

    # Guardar archivos
    with open(history_file, "w") as f:
        json.dump(history, f)
    with open("high_funding.json", "w") as f:
        json.dump(final_results, f, indent=4)

    print(f"Archivo actualizado: {len(final_results)} monedas encontradas.")

if __name__ == "__main__":
    check_all_market()
