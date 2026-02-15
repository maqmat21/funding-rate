import requests
import json
import datetime

def check_all_market():
    # Usamos fapi1 que es un endpoint de respaldo común
    url = "https://fapi1.binance.com/fapi/v1/premiumIndex"
    threshold = 0.007 # 0.7%
    
    # Esto hace que la petición parezca venir de Chrome en Windows
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()
        
        # Si fapi1 falla, intentamos el endpoint estándar como último recurso
        if not isinstance(data, list):
            print("Reintentando con endpoint estándar...")
            url_alt = "https://fapi.binance.com/fapi/v1/premiumIndex"
            response = requests.get(url_alt, headers=headers, timeout=15)
            data = response.json()

        if not isinstance(data, list):
            print(f"Error de API persistente: {data}")
            return

        ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        extreme_funding = [
            {
                "symbol": item["symbol"],
                "funding_rate_pct": round(float(item["lastFundingRate"]) * 100, 4),
                "type": "POSITIVE" if float(item["lastFundingRate"]) > 0 else "NEGATIVE",
                "mark_price": item.get("markPrice", "N/A")
            }
            for item in data 
            if abs(float(item["lastFundingRate"])) >= threshold
        ]
        
        # Estructura con fecha para forzar el cambio en GitHub
        resultado = {
            "ultima_actualizacion": ahora,
            "conteo": len(extreme_funding),
            "data": extreme_funding if extreme_funding else "Sin monedas extremas"
        }
        
        with open("high_funding.json", "w") as f:
            json.dump(resultado, f, indent=4)
        
        print(f"[{ahora}] Proceso completado. Encontrados: {len(extreme_funding)}")

    except Exception as e:
        print(f"Error crítico: {e}")

if __name__ == "__main__":
    check_all_market()
