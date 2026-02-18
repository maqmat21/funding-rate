import requests
import json
import datetime
import time
import os

def check_all_market():
    # URL de tu Worker de Cloudflare (Túnel Privado)
    # Se añade ?url= al final para que el Worker sepa a dónde redirigir
    CLOUDFLARE_TUNNEL = "https://funding-rate.shirirshir.workers.dev/?url="
    
    # Endpoints oficiales de Binance
    endpoints = [
        "https://fapi.binance.com/fapi/v1",
        "https://fapi1.binance.com/fapi/v1",
        "https://fapi2.binance.com/fapi/v1"
    ]
    
    threshold = 0.007
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def smart_fetch(path):
        """Intenta obtener datos a través de tu túnel de Cloudflare."""
        for base in endpoints:
            try:
                # Combinamos tu túnel con el endpoint de Binance
                url = f"{CLOUDFLARE_TUNNEL}{base}{path}"
                print(f"Pidiendo datos: {base}{path}")
                
                res = requests.get(url, headers=headers, timeout=20)
                if res.status_code == 200:
                    data = res.json()
                    if data: return data
                else:
                    print(f"Error {res.status_code} en mirror {base}")
            except Exception as e:
                print(f"Fallo en conexión: {e}")
                continue
        return None

    print("--- INICIANDO ESCANEO DE MERCADO (1 MIN CYCLE) ---")
    
    # 1. Obtener datos masivos
    data_funding = smart_fetch("/premiumIndex")
    data_tickers = smart_fetch("/ticker/24hr")
    data_liq = smart_fetch("/allForceOrders")

    if not data_funding or not data_tickers:
        print("CRÍTICO: El túnel de Cloudflare no pudo conectar con Binance.")
        return

    # Mapeo de datos para acceso rápido
    tickers = {t['symbol']: t for t in data_tickers if isinstance(t, dict) and 'symbol' in t}
    liq_symbols = [l['symbol'] for l in data_liq] if isinstance(data_liq, list) else []

    # Gestión de Historial (history_db.json)
    history_file = "history_db.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, "r") as f: history = json.load(f)
        except: history = {}
    else:
        history = {}

    final_results = []
    now_ts = int(time.time())

    # 2. Procesar cada moneda
    for item in data_funding:
        if not isinstance(item, dict): continue
        symbol = item.get("symbol", "")
        if not symbol.endswith('USDT'): continue
        
        f_rate = float(item.get("lastFundingRate", 0))
        f_pct = round(f_rate * 100, 4)

        # Filtro de Funding (0.7% o superior)
        if abs(f_pct) >= threshold:
            # Obtener Open Interest individual
            oi_data = smart_fetch(f"/openInterest?symbol={symbol}")
            oi_raw = float(oi_data.get('openInterest', 0)) if (oi_data and isinstance(oi_data, dict)) else 0

            # Inicializar historial para el símbolo si no existe
            if symbol not in history or not isinstance(history[symbol], list):
                history[symbol] = []
            
            # Guardar registro actual
            history[symbol].append({"ts": now_ts, "oi": oi_raw, "funding": f_pct})
            
            # Mantener solo las últimas 24 horas (86400 segundos)
            history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

            # Función para calcular cambios porcentuales de OI
            def get_change(records, seconds):
                if len(records) < 2: return 0.0
                target = now_ts - seconds
                # Buscar el registro más cercano al tiempo objetivo
                past = min(records, key=lambda x: abs(x['ts'] - target))
                # Si el dato más viejo tiene más de un margen de error del 70%, no es válido
                if now_ts - past['ts'] > (seconds * 1.7): return 0.0 
                return round(((oi_raw - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

            oi_4h = get_change(history[symbol], 14400)
            oi_24h = get_change(history[symbol], 86400)
            
            # Determinar tendencia del Funding
            trend = "STABLE"
            if len(history[symbol]) > 1:
                prev_f = history[symbol][-2]['funding']
                if f_pct > prev_f: trend = "INCREASING"
                elif f_pct < prev_f: trend = "DECREASING"

            # Lógica de Candidate Squeeze
            candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h >= -5
            ticker = tickers.get(symbol, {})
            price = float(ticker.get('lastPrice', 0))
            
            # Construir objeto de resultado
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
                "liquidation_cluster_detected": symbol in liq_symbols,
                "candidate_squeeze": candidate_squeeze,
                "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
            })

    # 3. Guardar archivos finales
    with open(history_file, "w") as f:
        json.dump(history, f)
    
    with open("high_funding.json", "w") as f:
        json.dump(final_results, f, indent=4)
        
    print(f"--- ESCANEO FINALIZADO: {len(final_results)} MONEDAS ENCONTRADAS ---")

if __name__ == "__main__":
    check_all_market()
