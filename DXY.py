import requests
import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime, timezone
from sklearn.linear_model import LinearRegression

# ==========================================
# CONFIGURACIÓN DE ENDPOINTS
# ==========================================
DXY_URL = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?range=5d&interval=5m"
US10Y_URL = "https://query1.finance.yahoo.com/v8/finance/chart/^TNX?range=5d&interval=5m"
OUTPUT_FILE = "macro_engine.json"
HISTORY_FILE = "macro_history_state.json"

def fetch_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()
        result = data['chart']['result'][0]
        df = pd.DataFrame({
            'Date': pd.to_datetime(result['timestamp'], unit='s'),
            'Open': result['indicators']['quote'][0]['open'],
            'High': result['indicators']['quote'][0]['high'],
            'Low': result['indicators']['quote'][0]['low'],
            'Close': result['indicators']['quote'][0]['close']
        }).dropna()
        return df.sort_values("Date")
    except Exception as e:
        print(f"Error en descarga: {e}")
        return pd.DataFrame()

def compute_structure(df, timeframe):
    df_res = df.set_index("Date").resample(timeframe).agg({
        "Open": "first", "High": "max", "Low": "min", "Close": "last"
    }).dropna()
    if len(df_res) < 2: return "neutral"
    curr, prev = df_res.iloc[-1], df_res.iloc[-2]
    if curr['High'] > prev['High'] and curr['Low'] > prev['Low']: return "alcista"
    if curr['High'] < prev['High'] and curr['Low'] < prev['Low']: return "bajista"
    return "neutral"

def get_state_summary(ema_t, s1h, s4h, brk, slope):
    if ema_t == "alcista" and s4h == "alcista": return "tendencia_fuerte"
    if s1h == "neutral": return "rango_lateral"
    if brk != "none": return "expansion_volatil"
    if slope < 0 and s4h == "alcista": return "debilidad_temporal"
    if (slope > 0 and s4h == "bajista") or (slope < 0 and s4h == "alcista"): return "posible_reversion"
    return "consolidacion"

def run_macro_engine():
    # 1. Obtención de datos
    dxy = fetch_data(DXY_URL)
    us10y = fetch_data(US10Y_URL)
    
    if dxy.empty or us10y.empty:
        print("Datos insuficientes para procesar.")
        return

    # 2. Cálculos DXY
    dxy['EMA20'] = dxy['Close'].ewm(span=20).mean()
    dxy['EMA50'] = dxy['Close'].ewm(span=50).mean()
    latest_dxy = dxy.iloc[-1]
    
    ema_trend = "alcista" if latest_dxy['EMA20'] > latest_dxy['EMA50'] else "bajista"
    s1h = compute_structure(dxy, "1h")
    s4h = compute_structure(dxy, "4h")
    
    # Momentum Slope (12 velas de 5m = 1h)
    m_data = dxy['Close'].tail(12).values.reshape(-1, 1)
    model = LinearRegression().fit(np.arange(len(m_data)).reshape(-1, 1), m_data)
    slope = float(model.coef_[0][0])
    m_state = "alcista" if slope > 0 else "bajista"
    
    # Breakout 24h (288 velas de 5m)
    b_window = dxy.tail(288)
    if latest_dxy['Close'] > b_window['High'].max(): breakout = "ruptura_alcista"
    elif latest_dxy['Close'] < b_window['Low'].min(): breakout = "ruptura_bajista"
    else: breakout = "none"

    # 3. Cálculos US10Y
    us_curr = float(us10y.iloc[-1]['Close'])
    us_prev = float(us10y.iloc[-12]['Close']) # 1h atrás
    us_dir = "subiendo" if us_curr > us_prev else "cayendo"
    us_m_state = "alcista" if (us_curr - us_prev) > 0 else "bajista"

    # 4. Score Cuantitativo
    score = 0
    if ema_trend == "alcista": score += 15
    if s1h == "alcista": score += 15
    if s4h == "alcista": score += 20
    if m_state == "alcista": score += 15
    if breakout == "ruptura_alcista": score += 15
    if us_dir == "subiendo": score += 20

    # 5. Interpretación de Bias
    bias = "RISK_OFF" if score >= 60 else "RISK_ON" if score <= 30 else "NEUTRAL"
    btc_prob = "bajista" if bias == "RISK_OFF" else "alcista" if bias == "RISK_ON" else "neutral"
    conf_level = "alta" if score > 80 or score < 20 else "media"

    # 6. Alertas y Persistencia
    macro_shift = False
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                prev_score = json.load(f).get('score', 0)
                if abs(score - prev_score) >= 20: macro_shift = True
        except: pass
    
    with open(HISTORY_FILE, 'w') as f:
        json.dump({'score': score, 'timestamp': time.time()}, f)

    # 7. Construcción del JSON Final
    final_json = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "macro_bias": bias,
        "macro_score": score,
        "dxy": {
            "price": round(float(latest_dxy['Close']), 4),
            "ema20": round(float(latest_dxy['EMA20']), 4),
            "ema50": round(float(latest_dxy['EMA50']), 4),
            "ema_trend": ema_trend,
            "structure_1h": s1h,
            "structure_4h": s4h,
            "momentum_slope": round(slope, 6),
            "momentum_state": m_state,
            "breakout_24h": breakout,
            "state_summary": get_state_summary(ema_trend, s1h, s4h, breakout, slope)
        },
        "us10y": {
            "price": round(us_curr, 4),
            "direction_1h": us_dir,
            "momentum_state": us_m_state
        },
        "risk_environment": {
            "btc_probability_bias": btc_prob,
            "altcoins_probability_bias": btc_prob,
            "confidence_level": conf_level
        },
        "alerts": {
            "macro_shift": macro_shift,
            "volatility_expansion": breakout != "none"
        }
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_json, f, indent=4)
    
    print(f"Update Exitosa: {bias} ({score} pts)")

if __name__ == "__main__":
    run_macro_engine()
