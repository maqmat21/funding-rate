import requests
import json
import datetime

def check_all_market():
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    threshold = 0.007 # 0.7%
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        
        if not isinstance(data, list):
            print(f"Error de API: {data}")
            return

        extreme_funding = [
            {
                "symbol": item["symbol"],
                "funding_rate_pct": round(float(item["lastFundingRate"]) * 100, 4),
                "type": "POSITIVE" if float(item["lastFundingRate"]) > 0 else "NEGATIVE"
            }
            for item in data 
            if abs(float(item["lastFundingRate"])) >= threshold
        ]
        
        with open("high_funding.json", "w") as f:
            if extreme_funding:
                json.dump(extreme_funding, f, indent=4)
            else:
                json.dump([{"status": "No data", "time": str(datetime.datetime.now())}], f, indent=4)
        
        print(f"Proceso completado. Encontrados: {len(extreme_funding)}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_all_market()