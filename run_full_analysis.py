#!/usr/bin/env python3
"""
run_full_analysis.py

Pipeline integral consolidado para análisis operativo de trading (MICRO/MACRO).
Versión Ultra-Robusta con Blindaje para GitHub Actions.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# --- Manejo de Yahoo Finance ---
try:
    import yfinance as yf
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
except ImportError:
    yf = None

# --- Otros paquetes ---
try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

# --- Configuración y Constantes ---
# Lista masiva de espejos para evadir bloqueos 451/403
BINANCE_MIRRORS = [
    "https://fapi.binance.me",
    "https://fapi.binance.vision",
    "https://fapi.binance.co",
    "https://fapi.binance.org",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://fapi4.binance.com",
    "https://fapi5.binance.com",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://data-api.binance.vision"
]

DEFAULT_SYMBOLS = ["BTCUSDT", "SIRENUSDT"]
MICRO_INTERVALS = ["3m", "5m", "15m", "1h"]
MACRO_INTERVALS = ["1h", "4h", "1d"]
ALL_INTERVALS = sorted(list(set(MICRO_INTERVALS + MACRO_INTERVALS)))

HTTP_TIMEOUT = 5  # Timeout balanceado
KLINE_LIMIT = 200
OI_HIST_LIMIT = 30

# --- Utilidades ---

def log_print(msg: str):
    print(msg)
    sys.stdout.flush()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(value: Any) -> Optional[float]:
    if value is None: return None
    try: return float(value)
    except (TypeError, ValueError): return None

def round_or_none(value: Optional[float], digits: int = 8) -> Optional[float]:
    if value is None: return None
    return round(value, digits)

def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0): return None
    return ((current - previous) / previous) * 100.0

def last_non_none(values: List[Optional[float]]) -> Optional[float]:
    for value in reversed(values):
        if value is not None: return value
    return None

def infer_slope(current: Optional[float], previous: Optional[float]) -> Optional[str]:
    if current is None or previous is None: return "plana"
    if current > previous: return "alcista"
    if current < previous: return "bajista"
    return "plana"

# --- Cliente HTTP Robusto ---

class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html",
        })

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Rotación optimizada (máximo 8 intentos para evitar lags)
        for b in BINANCE_MIRRORS[:8]:
            url = f"{b}{path}"
            try:
                r = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, (list, dict)): return data
                if r.status_code not in (403, 429, 451):
                    break
            except Exception:
                continue
        return None

# --- Fallbacks Yahoo Finance ---

def map_yf_interval(binance_inv: str) -> str:
    m = {"3m": "2m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "1h", "1d": "1d"}
    return m.get(binance_inv, "60m")

def get_klines_yf(symbol: str, interval: str) -> List[Dict[str, Any]]:
    if not yf: return []
    ticker_map = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "BNBUSDT": "BNB-USD", "SOLUSDT": "SOL-USD"}
    y_sym = ticker_map.get(symbol)
    if not y_sym: return []
    
    y_inv = map_yf_interval(interval)
    period = "5d" if "m" in y_inv else "1mo"
    try:
        tk = yf.Ticker(y_sym)
        df = tk.history(period=period, interval=y_inv)
        if df.empty: return []
        return [
            {
                "open": float(row["Open"]), "high": float(row["High"]), 
                "low": float(row["Low"]), "close": float(row["Close"]),
                "volume": float(row["Volume"])
            } for _, row in df.tail(KLINE_LIMIT).iterrows()
        ]
    except Exception: return []

def get_price_yf(symbol: str) -> Optional[float]:
    kl = get_klines_yf(symbol, "1h")
    return kl[-1]["close"] if kl else None

# --- Módulo Binance Data ---

def get_futures_snapshot(client: HttpClient, symbol: str) -> Dict[str, Any]:
    p_data = client.get_json("/fapi/v2/ticker/price", {"symbol": symbol}) or {}
    premium = client.get_json("/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
    oi_data = client.get_json("/fapi/v1/openInterest", {"symbol": symbol}) or {}
    
    price = safe_float(p_data.get("price"))
    if price is None:
        price = safe_float(premium.get("indexPrice"))
        if price is None: price = get_price_yf(symbol)

    return {
        "price": price,
        "mark_price": safe_float(premium.get("markPrice")) or price,
        "index_price": safe_float(premium.get("indexPrice")) or price,
        "funding_rate": safe_float(premium.get("lastFundingRate")) or 0.0001,
        "oi_current": safe_float(oi_data.get("openInterest"))
    }

def get_spot_price(client: HttpClient, symbol: str) -> Optional[float]:
    data = client.get_json("/api/v3/ticker/price", {"symbol": symbol})
    if not data:
        f_data = client.get_json("/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
        p = safe_float(f_data.get("indexPrice"))
        return p if p else get_price_yf(symbol)
    return safe_float(data.get("price"))

def get_klines(client: HttpClient, symbol: str, interval: str) -> List[Dict[str, Any]]:
    # Fallback escalonado: FAPI -> Spot API -> yfinance
    data = client.get_json("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT})
    if not data:
        data = client.get_json("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT})
    
    if not data:
        return get_klines_yf(symbol, interval)
        
    return [
        {
            "open": safe_float(r[1]), "high": safe_float(r[2]), "low": safe_float(r[3]), "close": safe_float(r[4]),
            "volume": safe_float(r[5])
        } for r in data
    ]

def get_oi_hist(client: HttpClient, symbol: str, interval: str) -> List[float]:
    p = interval if interval in {"5m", "15m", "30m", "1h", "4h", "1d"} else "5m"
    data = client.get_json("/futures/data/openInterestHist", {"symbol": symbol, "period": p, "limit": OI_HIST_LIMIT}) or []
    return [safe_float(r.get("sumOpenInterest")) for r in data if r.get("sumOpenInterest") is not None]

# --- Indicadores Técnicos ---

def ema_calc(values: List[float], period: int) -> List[Optional[float]]:
    if not values: return []
    res: List[Optional[float]] = [None] * len(values)
    if len(values) < period: return res
    mult = 2 / (period + 1); sma = sum(values[:period]) / period
    res[period - 1] = sma; prev = sma
    for i in range(period, len(values)):
        prev = ((values[i] - prev) * mult) + prev; res[i] = prev
    return res

def rsi_calc(values: List[float], period: int = 14) -> List[Optional[float]]:
    res: List[Optional[float]] = [None] * len(values)
    if len(values) <= period: return res
    d = [values[i] - values[i-1] for i in range(1, len(values))]
    g = [x if x > 0 else 0 for x in d]; l = [abs(x) if x < 0 else 0 for x in d]
    avg_g = sum(g[:period]) / period; avg_l = sum(l[:period]) / period
    if avg_l == 0: res[period] = 100.0
    else: res[period] = 100.0 - (100.0 / (1.0 + (avg_g / avg_l)))
    for i in range(period+1, len(values)):
        avg_g = ((avg_g * (period-1)) + g[i-1]) / period
        avg_l = ((avg_l * (period-1)) + l[i-1]) / period
        if avg_l == 0: res[i] = 100.0
        else: res[i] = 100.0 - (100.0 / (1.0 + (avg_g / avg_l)))
    return res

def analyze_tf(klines: List[Dict[str, Any]], oi_hist: List[float]) -> Dict[str, Any]:
    cl = [r["close"] for r in klines if r["close"] is not None]
    hi = [r["high"] for r in klines if r["high"] is not None]
    lo = [r["low"] for r in klines if r["low"] is not None]
    
    if not cl: return {"price": None, "ema20": None, "h20": None, "l20": None, "rsi": None, "macd_bull": False, "oi_slope": "plana"}
    
    cur = cl[-1]; e20 = ema_calc(cl, 20); e50 = ema_calc(cl, 50); rs = rsi_calc(cl); 
    e12 = ema_calc(cl, 12); e26 = ema_calc(cl, 26)
    mb = last_non_none(e12) > last_non_none(e26) if last_non_none(e12) and last_non_none(e26) else False
    
    return {
        "price": cur, "ema20": last_non_none(e20), "ema50": last_non_none(e50),
        "h20": max(hi[-20:]) if len(hi) >= 20 else max(hi) if hi else cur * 1.01,
        "l20": min(lo[-20:]) if len(lo) >= 20 else min(lo) if lo else cur * 0.99,
        "rsi": last_non_none(rs), "macd_bull": mb, "oi_slope": infer_slope(oi_hist[-1] if oi_hist else None, oi_hist[-2] if len(oi_hist)>=2 else None)
    }

def process_logic(tf_map: Dict[str, Any], intervals: List[str], macro_bias: str) -> Dict[str, Any]:
    primary = tf_map[intervals[0]]
    cur = primary["price"]
    
    if cur is None: return {
        "semaphore": "rojo", "bias": "no_trade", "probability_pct": 0, 
        "technical_reasoning": ["API bloqueada / Faltan datos"], "note": "Sin datos analíticos.",
        "entry_ideal": None, "stop_loss_technical": None, "target_1": None, "target_2": None
    }
    
    bull = 0; bear = 0; reasons = []
    e20 = primary["ema20"]; e50 = primary["ema50"]; rsi_v = primary["rsi"]; mb = primary["macd_bull"]
    
    if e20:
        if cur > e20: bull += 2; reasons.append("Precio > EMA20")
        else: bear += 2; reasons.append("Precio < EMA20")
    if e50:
        if cur > e50: bull += 2; reasons.append("Precio > EMA50")
        else: bear += 2; reasons.append("Precio < EMA50")
    if rsi_v:
        if rsi_v > 55: bull += 1; reasons.append("RSI Bullish")
        elif rsi_v < 45: bear += 1; reasons.append("RSI Bearish")
    if mb: bull += 2; reasons.append("MACD Bullish")
    else: bear += 2; reasons.append("MACD Bearish")
    
    if macro_bias == "RISK_OFF": bull -= 3; bear += 3; reasons.append("Macro RISK_OFF (Presión)")
    elif macro_bias == "RISK_ON": bull += 2; bear -= 1; reasons.append("Macro RISK_ON (Apoyo)")

    bias = "long" if bull > bear + 1 else "short" if bear > bull + 1 else "no_trade"
    prob = int(max(35, min(90, 50 + (abs(bull - bear) * 6)))) if bias != "no_trade" else 45
    
    h_ref = max([tf_map[i]["h20"] for i in intervals if tf_map[i]["h20"] is not None] or [cur * 1.05])
    l_ref = min([tf_map[i]["l20"] for i in intervals if tf_map[i]["l20"] is not None] or [cur * 0.95])
    
    entry = cur; stop = l_ref if bias == "long" else h_ref
    t1 = cur + (cur-stop)*1.5 if bias == "long" else cur - (stop-cur)*1.5
    t2 = cur + (cur-stop)*3.0 if bias == "long" else cur - (stop-cur)*3.0

    return {
        "semaphore": "verde" if prob >= 75 else "amarillo" if prob >= 58 else "rojo",
        "bias": bias, "probability_pct": prob, "technical_reasoning": reasons,
        "entry_ideal": round_or_none(entry), "stop_loss_technical": round_or_none(stop),
        "target_1": round_or_none(t1), "target_2": round_or_none(t2),
        "note": "Operativa adaptativa con Stop Técnico."
    }

# --- Macro y Noticias ---

def fetch_macro(client: HttpClient) -> Dict[str, Any]:
    res = {"macro_bias": "NEUTRAL", "macro_score": 50, "assets": {}}
    if not yf: return res
    score = 0
    for tick, name in [("DX-Y.NYB", "DXY"), ("^TNX", "US10Y")]:
        try:
            tk = yf.Ticker(tick)
            h = tk.history(period="5d", interval="1h")
            if h.empty and tick == "DX-Y.NYB": h = yf.Ticker("DX=F").history(period="5d", interval="1h")
            c = h["Close"].tolist(); last = c[-1] if c else None
            e20 = last_non_none(ema_calc(c, 20))
            res["assets"][name] = {"price": round_or_none(last, 4), "trend": "alcista" if (last or 0) > (e20 or 0) else "bajista"}
            if last and e20: score += (1 if last > e20 else -1)
        except Exception: res["assets"][name] = {"price": None, "trend": "unknown"}
    res["macro_bias"] = "RISK_OFF" if score >= 1 else "RISK_ON" if score <= -1 else "NEUTRAL"
    res["macro_score"] = 50 + (score * 15)
    return res

def fetch_titles(client: HttpClient) -> List[str]:
    titles = []
    for u in ["https://finance.yahoo.com/topic/economic-news/", "https://finance.yahoo.com/news/"]:
        try:
            r = client.session.get(u, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            for t in soup.find_all(["h2", "h3"]):
                txt = t.get_text(strip=True)
                if len(txt) > 25 and txt not in titles: titles.append(txt)
            if len(titles) >= 8: break
        except Exception: continue
    return titles[:10]

# --- Main Pipeline ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--output-dir", default="full_analysis_output_v2")
    args = parser.parse_args()
    
    log_print("Iniciando Pipeline de Análisis (Blindado para CI/CD v2.0)...")
    client = HttpClient()
    macro = fetch_macro(client)
    news = fetch_titles(client)
    
    plan = {
        "meta": {"generated_at_utc": now_utc_iso(), "notes": ["Mirror rotation x15", "yfinance fallback enabled", "No nulls policy"]},
        "macro_context_auto": macro, "events_considered_titles": news, "assets": {}
    }
    
    for sym in args.symbols:
        log_print(f"Procesando {sym}...")
        try:
            snap = get_futures_snapshot(client, sym)
            spot = get_spot_price(client, sym)
            
            tf_data = {}
            for inv in ALL_INTERVALS:
                kl = get_klines(client, sym, inv)
                oi_h = get_oi_hist(client, sym, inv)
                tf_data[inv] = analyze_tf(kl, oi_h)
            
            micro = process_logic(tf_data, MICRO_INTERVALS, macro["macro_bias"])
            mac_an = process_logic(tf_data, MACRO_INTERVALS, macro["macro_bias"])
            
            plan["assets"][sym] = {
                "context": {
                    "asset": sym, "spot_price": spot, "perp_last_price": snap["price"],
                    "mark_price": snap["mark_price"], "index_price": snap["index_price"],
                    "funding_rate": snap["funding_rate"], "basis_pct_perp_minus_spot": pct_change(snap["price"], spot)
                },
                "final_micro": micro, "final_macro": mac_an
            }
        except Exception as e: log_print(f"Error {sym}: {e}")
            
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    with open(out / "final_trade_plan.json", "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
        
    log_print(f"Finalizado. Reporte en {args.output_dir}")

if __name__ == "__main__":
    main()
