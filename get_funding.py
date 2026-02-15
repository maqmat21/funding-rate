import requests
import json
import datetime

def check_all_market():
    # Usamos un endpoint alternativo que suele tener menos restricciones
    # Si este falla, probaremos con fapi.binance.com
    urls = [
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        "https://fapi1.binance.com/fapi/v1/premiumIndex",
        "https://fapi2.binance.com/fapi/v1/premiumIndex"
    ]
    
    threshold = 0.007
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    data = None
    for url in urls:
        try:
            print(f"Intentando conectar a: {url}")
            response = requests.get(url, headers=headers, timeout=15)
            
            # Verificamos si la respuesta es exitosa antes de procesar
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    print(f"Conexión exitosa con {url}")
                    break
            else:
                print(f"Fallo {url} con status: {response.status_code}")
        except Exception as e:
            print(f"Error en {url}: {e}")

    if not data or not isinstance(data, list):
        print("No se pudo obtener una lista válida de Binance de ningún endpoint.")
        return

    # Procesamiento de datos (igual que antes)
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    extreme_funding = [
        {
            "symbol": item["symbol"],
            "funding_rate_pct": round(float(item["lastFundingRate"]) * 100, 4),
            "type": "POSITIVE" if float(item["lastFundingRate"]) > 0 else "NEGATIVE"
        }
        for item in data 
        if abs(float(item["lastFundingRate"])) >= threshold
    ]
    
    resultado = {
        "ultima_actualizacion": ahora,
        "conteo": len(extreme_funding),
        "data": extreme_funding if extreme_funding else "Sin movimientos extremos"
    }
    
    with open("high_funding.json", "w") as f:
        json.dump(resultado, f, indent=4)
    
    print(f"Escaneo finalizado a las {ahora}.")

if __name__ == "__main__":
    check_all_market()
