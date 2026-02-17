import requests
import json
import datetime
import os

proxies_list = [
    "http://api.allorigins.win/get?url=",
    "https://api.codetabs.com/v1/proxy?quest="
]

HISTORY_FILE = "oi_history.json"

def fetch_via_proxy(url):
    for proxy in proxies_list:
        try:
            target_url = f"{proxy}{url}"
            response = requests.get(target_url, timeout=20)
            if response.status_code == 200:
                raw_data = response.json()
                if isinstance(raw_data, dict) and 'contents' in raw_data:
                    return json.loads(raw_data['contents'])
                return raw_data
        except:
            continue
    return None

def get_open_interest(symbol):
    url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
    data = fetch_via_proxy(url)
    try:
        if data and "openInterest" in data:
            return float(data["openInterest"])
    except:
        return None
    return None

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return {}

def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=4)

def calculate_oi_change(symbol, current_oi):
    history = load_history()
    previous_oi = history.get(symbol, {}).get("open_interest")

    oi_change_pct = None

    if previous_oi and previous_oi != 0:
        oi_change_pct = round(((current_oi - previous_oi) / previous_oi) * 100, 4)

    # Guardamos el valor actual para próxima ejecución
    history[symbol] = {
        "open_interest": current_oi,
        "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    save_history(history)

    return oi_change_pct

def get_24h_ticker(symbol):
    url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
    data = fetch_via_proxy(url)
    try:
        if data:
            return {
                "price": float(data["lastPrice"]),
                "price_24h_change_pct": float(data["priceChangePercent"]),
                "volume_24h": float(data["volume"])
            }
    except:
        return None
    return None

def check_all_market():
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    threshold = 0.007
    data = fetch_via_proxy(url)

    if not data or not isinstance(data, list):
        print("No se pudo obtener datos.")
        return

    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    extreme_funding = []

    for item in data:
        funding_rate = float(item["lastFundingRate"])
        
        if abs(funding_rate) >= threshold:
            symbol = item["symbol"]
            
            oi = get_open_interest(symbol)
            oi_change = None

            if oi is not None:
                oi_change = calculate_oi_change(symbol, oi)

            ticker_data = get_24h_ticker(symbol)

            enriched = {
                "symbol": symbol,
                "funding_rate_pct": round(funding_rate * 100, 4),
                "type": "POSITIVE" if funding_rate > 0 else "NEGATIVE",
                "open_interest": oi,
                "oi_1h_change_pct": oi_change
            }

            if ticker_data:
                enriched.update(ticker_data)

            extreme_funding.append(enriched)

    resultado = {
        "ultima_actualizacion": ahora,
        "conteo": len(extreme_funding),
        "data": extreme_funding if extreme_funding else "Sin movimientos extremos"
    }
    
    with open("high_funding.json", "w") as f:
        json.dump(resultado, f, indent=4)
    
    print(f"Archivo actualizado con éxito a las {ahora}.")

if __name__ == "__main__":
    check_all_market()
