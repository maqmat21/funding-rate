import requests
import json
import time

def check_all_market():
    # Endpoint para futuros USDT-M
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    filename = "high_funding.json"
    
    try:
        start_time = time.time()
        response = requests.get(url, timeout=10)
        data = response.json()
        
        # Umbral de 0.7% (en decimal es 0.007)
        threshold = 0.007 
        
        # Filtramos usando abs() para capturar > 0.7% y < -0.7%
        extreme_funding = [
            {
                "symbol": item["symbol"],
                "funding_rate_pct": round(float(item["lastFundingRate"]) * 100, 4),
                "mark_price": item["markPrice"],
                "type": "POSITIVE" if float(item["lastFundingRate"]) > 0 else "NEGATIVE"
            }
            for item in data 
            if abs(float(item["lastFundingRate"])) >= threshold
        ]
        
        # Lógica de guardado: Limpiar y escribir
        with open(filename, "w") as f:
            if extreme_funding:
                json.dump(extreme_funding, f, indent=4)
            else:
                # Si no hay nada, dejamos el aviso en el JSON
                json.dump([{"status": "No hay monedas con funding extremo (+/- 0.7%)"}], f, indent=4)
        
        end_time = time.time()
        
        print(f"--- ESCANEO COMPLETADO ---")
        print(f"Total pares revisados: {len(data)}")
        print(f"Monedas extremas detectadas: {len(extreme_funding)}")
        print(f"Tiempo de ejecución: {round(end_time - start_time, 2)}s")

    except Exception as e:
        print(f"Error al conectar con la API: {e}")

if __name__ == "__main__":
    check_all_market()