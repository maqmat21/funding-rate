import requests
import json
import datetime

def get_open_interest(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return float(response.json()["openInterest"])
    except:
        return None
    return None

def get_24h_ticker(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
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
    
    proxies_list = [
        "http://api.allorigins.win/get?url=",
        "https://api.codetabs.com/v1/proxy?quest="
    ]
    
    threshold = 0.007
    data = None

    for proxy in proxies_list:
        try:
            target_url = f"{proxy}{url}"
            print(f"Intentando vía Proxy: {proxy}")
            response = requests.get(target_url, timeout=20)
            
            if response.status_code == 200:
                raw_data = response.json()
                if isinstance(raw_data, dict) and 'contents' in raw_data:
                    data = json.loads(raw_data['contents'])
                else:
                    data = raw_data
                
                if isinstance(data, list):
                    print("¡Conexión exitosa a través del proxy!")
                    break
        except Exception as e:
            print(f"Fallo proxy {proxy}: {e}")

    if not data or not isinstance(data, list):
        print("No se pudo saltar el bloqueo de Binance.")
        return

    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    extreme_funding = []

    for item in data:
        funding_rate = float(item["lastFundingRate"])
        
        if abs(funding_rate) >= threshold:
            symbol = item["symbol"]
            
            oi = get_open_interest(symbol)
            ticker_data = get_24h_ticker(symbol)

            enriched = {
                "symbol": symbol,
                "funding_rate_pct": round(funding_rate * 100, 4),
                "type": "POSITIVE" if funding_rate > 0 else "NEGATIVE",
                "open_interest": oi,
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
