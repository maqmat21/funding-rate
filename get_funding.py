import requests
import json
import time
import os
import urllib3
import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def check_all_market():
    # Endpoints para redundancia
    ENDPOINTS = [
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        "https://www.binance.com/fapi/v1/premiumIndex"
    ]
    TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    LIQ_URL = "https://fapi.binance.com/fapi/v1/allForceOrders"
    
    THRESHOLD = 0.006 # 0.7%
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }

    print("--- INICIANDO ESCANEO PROFESIONAL (MODO MULTI-FUENTE) ---")

    data_funding = None
    # Intento de lectura de Funding con redundancia
    for url in ENDPOINTS:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                data_funding = res.json()
                print(f"Datos obtenidos con éxito desde: {url}")
                break
        except:
            continue

    if not data_funding:
        print("CRÍTICO: Todas las fuentes fallaron. Binance bloqueó la IP de GitHub.")
        return

    # Obtención de Tickers (Volumen y Cambio %)
    try:
        res_t = requests.get(TICKER_URL, headers=headers, timeout=15)
        ticker_map = {t['symbol']: t for t in res_t.json()}
    except:
        ticker_map = {}

    # Obtención de Liquidaciones
    try:
        res_l = requests.get(LIQ_URL, headers=headers, timeout=10)
        liq_symbols = {l['symbol'] for l in res_l.json()}
    except:
        liq_symbols = set()

    # Gestión de Historial
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else: history = {}

    now_ts = int(time.time())
    final_results = []

    for item in data_funding:
        symbol = item.get('symbol', '')
        if not symbol.endswith('USDT'): continue
        
        # El funding rate real en este endpoint viene en decimal (ej: -0.0071)
        f_rate = float(item.get('lastFundingRate', 0))
        f_pct = round(f_rate * 100, 4)

        if abs(f_rate) >= THRESHOLD:
            price = float(item.get('markPrice', 0))
            t_info = ticker_map.get(symbol, {})
            
            # Para el OI, usamos el endpoint específico para garantizar tiempo real
            # Solo se llama para las monedas que pasan el filtro (evita baneos)
            try:
                oi_res = requests.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}", headers=headers, timeout=5).json()
                oi_raw = float(oi_res.get('openInterest', 0))
                oi_usd = oi_raw * price
            except:
                oi_usd = 0.0

            vol_24h = float(t_info.get('quoteVolume', 0)) if t_info else 0.0
            price_chg = float(t_info.get('priceChangePercent', 0)) if t_info else 0.0

            # Guardar en Historial
            if symbol not in history: history[symbol] = []
            history[symbol].append({"ts": now_ts, "oi": oi_usd, "funding": f_pct})
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            def get_change(records, seconds):
                if len(records) < 2: return 0.0
                target = now_ts - seconds
                past = min(records, key=lambda x: abs(x['ts'] - target))
                if now_ts - past['ts'] > (seconds * 2): return 0.0
                return round(((oi_usd - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = get_change(history[symbol], 14400)
            oi_24h = get_change(history[symbol], 86400)

            # Lógica de Tendencia
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
                "price_24h_change_pct": price_chg,
                "volume_24h_usd": round(vol_24h, 2),
                "open_interest_usd": round(oi_usd, 2),
                "oi_4h_change_pct": oi_4h,
                "oi_24h_change_pct": oi_24h,
                "liquidation_cluster_detected": symbol in liq_symbols,
                "candidate_squeeze": candidate_squeeze,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })

    # Ordenar por funding absoluto
    final_results = sorted(final_results, key=lambda x: abs(x['funding_rate_pct']), reverse=True)

    with open(history_file, "w") as f: json.dump(history, f)
    with open("high_funding.json", "w") as f: json.dump(final_results, f, indent=4)
    
    print(f"--- ESCANEO FINALIZADO: {len(final_results)} MONEDAS DETECTADAS ---")

if __name__ == "__main__":
    check_all_market()
