import requests
import json
import datetime

def check_all_market():
    # Usamos un servicio de Proxy/Mirror que redirige la petición
    # Esto hace que Binance vea una IP distinta a la de GitHub
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    
    # Lista de proxies gratuitos para intentar saltar el bloqueo
    # Si uno falla, el script intentará el siguiente
    proxies_list = [
        "http://api.allorigins.win/get?url=", # Proxy 1 (Capa de abstracción)
        "https://api.codetabs.com/v1/proxy?quest=" # Proxy 2
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
                # Algunos proxies devuelven el JSON dentro de una llave 'contents'
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
        print("No se pudo saltar el bloqueo de Binance. Intentando última alternativa...")
        # Alternativa final: Usar un mirror público de datos de criptos si existe
        return

    # Procesamiento
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
    
    print(f"Archivo actualizado con éxito a las {ahora}.")

if __name__ == "__main__":
    check_all_market()
