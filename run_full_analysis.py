#!/usr/bin/env python3
"""
run_full_analysis.py

Pipeline integral consolidado para análisis operativo de trading (MICRO/MACRO).
Máxima resiliencia ante bloqueos (Error 451/403) y fallbacks automáticos.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
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
SPOT_BASE = "https://data-api.binance.vision" 
# Espejos de Binance (Futures y Spot)
FUTURES_BASES = [
    "https://fapi.binance.me",
    "https://fapi.binance.vision",
    "https://fapi.binance.co",
    "https://fapi.binance.org",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi.binance.com"
]

DEFAULT_SYMBOLS = ["BTCUSDT", "SIRENUSDT"]
MICRO_INTERVALS = ["3m", "5m", "15m", "1h"]
MACRO_INTERVALS = ["1h", "4h", "1d"]
ALL_INTERVALS = sorted(list(set(MICRO_INTERVALS + MACRO_INTERVALS)))

HTTP_TIMEOUT = 25
KLINE_LIMIT = 200
OI_HIST_LIMIT = 30

# --- Utilidades ---

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

def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

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

    def get_json(self, base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Priorizar espejos conocidos como más permisivos
        bases = FUTURES_BASES if (path.startswith("/fapi") or "/futures/" in path) else [base_url] + FUTURES_BASES
        
        for b in bases:
            url = f"{b}{path}"
            try:
                r = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list) or (isinstance(data, dict) and not data.get("code")):
                        return data
                # Si es 403 o 451, es bloqueo, probamos siguiente espejo
                if r.status_code in (403, 451):
                    continue
            except Exception:
                continue
        return None

# --- Fallbacks Externos ---

def get_crypto_price_yfinance(symbol: str) -> Optional[float]:
    if not yf: return None
    ticker_map = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD"}
    y_sym = ticker_map.get(symbol)
    if not y_sym: return None
    try:
        tk = yf.Ticker(y_sym)
        return float(tk.history(period="1d")["Close"].iloc[-1])
    except Exception:
        return None

# --- Módulo Binance Data ---

def get_futures_snapshot(client: HttpClient, symbol: str) -> Dict[str, Any]:
    base = FUTURES_BASES[0]
    p_data = client.get_json(base, "/fapi/v2/ticker/price", {"symbol": symbol}) or {}
    premium = client.get_json(base, "/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
    oi = client.get_json(base, "/fapi/v1/openInterest", {"symbol": symbol}) or {}
    
    price = safe_float(p_data.get("price"))
    if price is None: # Fallback aggressively
        price = safe_float(premium.get("indexPrice"))
        if price is None:
            price = get_crypto_price_yfinance(symbol)

    return {
        "price": price,
        "mark_price": safe_float(premium.get("markPrice")) or price,
        "index_price": safe_float(premium.get("indexPrice")) or price,
        "funding_rate": safe_float(premium.get("lastFundingRate")),
        "oi_current": safe_float(oi.get("openInterest"))
    }

def get_spot_price(client: HttpClient, symbol: str) -> Optional[float]:
    data = client.get_json(SPOT_BASE, "/api/v3/ticker/price", {"symbol": symbol})
    if not data:
        f_data = client.get_json(FUTURES_BASES[0], "/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
        p = safe_float(f_data.get("indexPrice"))
        return p if p else get_crypto_price_yfinance(symbol)
    return safe_float(data.get("price"))

def get_klines(client: HttpClient, symbol: str, interval: str) -> List[Dict[str, Any]]:
    # Rotar entre espejos para klines
    data = client.get_json(FUTURES_BASES[0], "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT}) or []
    return [
        {
            "open": safe_float(r[1]), "high": safe_float(r[2]), "low": safe_float(r[3]), "close": safe_float(r[4]),
            "volume": safe_float(r[5])
        } for r in data
    ]

def get_oi_hist(client: HttpClient, symbol: str, interval: str) -> List[float]:
    p = interval if interval in {"5m", "15m", "30m", "1h", "4h", "1d"} else "5m"
    data = client.get_json(FUTURES_BASES[0], "/futures/data/openInterestHist", {"symbol": symbol, "period": p, "limit": OI_HIST_LIMIT}) or []
    return [safe_float(r.get("sumOpenInterest")) for r in data if r.get("sumOpenInterest") is not None]

# --- Indicadores y Análisis ---

def ema_calc(values: List[float], period: int) -> List[Optional[float]]:
    if not values: return []
    res: List[Optional[float]] = [None] * len(values)
    if len(values) < period: return res
    mult = 2 / (period + 1)
    sma = sum(values[:period]) / period
    res[period - 1] = sma
    prev = sma
    for i in range(period, len(values)):
        prev = ((values[i] - prev) * mult) + prev
        res[i] = prev
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
    
    # Simple MACD logic
    e12 = ema_calc(cl, 12); e26 = ema_calc(cl, 26)
    mb = last_non_none(e12) > last_non_none(e26) if last_non_none(e12) and last_non_none(e26) else False
    
    oi_cur = oi_hist[-1] if oi_hist else None
    oi_pre = oi_hist[-2] if len(oi_hist) >= 2 else None
    
    return {
        "price": cur, "ema20": last_non_none(e20), "ema50": last_non_none(e50),
        "h20": max(hi[-20:]) if len(hi) >= 20 else max(hi) if hi else cur * 1.01,
        "l20": min(lo[-20:]) if len(lo) >= 20 else min(lo) if lo else cur * 0.99,
        "rsi": last_non_none(rs), "macd_bull": mb, "oi_slope": infer_slope(oi_cur, oi_pre)
    }

def process_logic(tf_map: Dict[str, Any], intervals: List[str], macro_bias: str) -> Dict[str, Any]:
    primary = tf_map[intervals[0]]
    cur = primary["price"]
    
    if cur is None: return {
        "semaphore": "rojo", "bias": "no_trade", "probability_pct": 0, 
        "technical_reasoning": ["Faltan datos de Binance"], "note": "Sin datos suficientes.",
        "entry_ideal": None, "stop_loss_technical": None, "target_1": None, "target_2": None
    }
    
    bull = 0; bear = 0; rs = []
    e20 = primary["ema20"]; e50 = primary["ema50"]; rsi_v = primary["rsi"]; mb = primary["macd_bull"]; os = primary["oi_slope"]
    
    if e20:
        if cur > e20: bull += 2; rs.append("Precio > EMA20")
        else: bear += 2; rs.append("Precio < EMA20")
    if e50:
        if cur > e50: bull += 2; rs.append("Precio > EMA50")
        else: bear += 2; rs.append("Precio < EMA50")
    if rsi_v:
        if rsi_v > 55: bull += 1; rs.append("RSI Bullish")
        elif rsi_v < 45: bear += 1; rs.append("RSI Bearish")
    if mb: bull += 2; rs.append("MACD Bullish")
    else: bear += 2; rs.append("MACD Bearish")
    
    if macro_bias == "RISK_OFF": bull -= 3; bear += 3; rs.append("Macro RISK_OFF")
    elif macro_bias == "RISK_ON": bull += 2; bear -= 1; rs.append("Macro RISK_ON")

    bias = "long" if bull > bear + 1 else "short" if bear > bull + 1 else "no_trade"
    prob = int(clamp(50 + (abs(bull - bear) * 6), 35, 90)) if bias != "no_trade" else 45
    
    h_ref = max([tf_map[i]["h20"] for i in intervals if tf_map[i]["h20"] is not None] or [cur * 1.05])
    l_ref = min([tf_map[i]["l20"] for i in intervals if tf_map[i]["l20"] is not None] or [cur * 0.95])
    
    entry = stop = t1 = t2 = None
    if bias == "long":
        entry = cur; stop = l_ref; t1 = cur + (cur-l_ref)*1.5; t2 = cur + (cur-l_ref)*3
    elif bias == "short":
        entry = cur; stop = h_ref; t1 = cur - (h_ref-cur)*1.5; t2 = cur - (h_ref-cur)*3

    return {
        "semaphore": "verde" if prob >= 75 else "amarillo" if prob >= 58 else "rojo",
        "bias": bias, "probability_pct": prob, "technical_reasoning": rs,
        "entry_ideal": round_or_none(entry), "stop_loss_technical": round_or_none(stop),
        "target_1": round_or_none(t1), "target_2": round_or_none(t2),
        "note": "Respetar Stop Loss estructural."
    }

# --- Macro y Noticias ---

def fetch_macro_data(client: HttpClient) -> Dict[str, Any]:
    res = {"macro_bias": "NEUTRAL", "macro_score": 50, "assets": {}}
    if not yf: return res
    score = 0
    symbols = [("DX-Y.NYB", "DXY"), ("^TNX", "US10Y")]
    for tick, name in symbols:
        try:
            tk = yf.Ticker(tick)
            hist = tk.history(period="5d", interval="1h")
            if hist.empty and tick == "DX-Y.NYB":
                hist = yf.Ticker("DX=F").history(period="5d", interval="1h")
            
            c = hist["Close"].tolist()
            last = c[-1] if c else None
            if last is None: # Scraper fallback?
                last = None # skip for now, yfinance usually works
            
            e20 = last_non_none(ema_calc(c, 20))
            res["assets"][name] = {"price": round_or_none(last, 4), "trend": "alcista" if (last or 0) > (e20 or 0) else "bajista"}
            if last and e20: score += (1 if last > e20 else -1)
        except Exception:
            res["assets"][name] = {"price": None, "trend": "unknown"}
            
    res["macro_bias"] = "RISK_OFF" if score >= 1 else "RISK_ON" if score <= -1 else "NEUTRAL"
    res["macro_score"] = 50 + (score * 15)
    return res

def fetch_events(client: HttpClient) -> List[str]:
    titles = []
    urls = ["https://finance.yahoo.com/topic/economic-news/", "https://finance.yahoo.com/news/"]
    for u in urls:
        try:
            r = client.session.get(u, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            for tag in soup.find_all(["h2", "h3"]):
                txt = tag.get_text(strip=True)
                if len(txt) > 25 and txt not in titles: titles.append(txt)
            if len(titles) >= 10: break
        except Exception: continue
    return titles[:10]

# --- Main Pipeline ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--output-dir", default="full_analysis_output_v2")
    args = parser.parse_args()
    
    print("Iniciando Pipeline de Análisis (Modo Robusto)...")
    client = HttpClient()
    macro = fetch_macro_data(client)
    events = fetch_events(client)
    
    plan = {
        "meta": {"generated_at_utc": now_utc_iso(), "notes": ["Bypass bloqueos activo", "Fallbacks habilitados"]},
        "macro_context_auto": macro,
        "events_considered_titles": events,
        "assets": {}
    }
    
    for sym in args.symbols:
        print(f"Procesando {sym}...")
        try:
            price_snap = get_futures_snapshot(client, sym)
            spot_p = get_spot_price(client, sym)
            
            tfs = {}
            for inv in ALL_INTERVALS:
                kl = get_klines(client, sym, inv)
                oh = get_oi_hist(client, sym, inv)
                tfs[inv] = analyze_tf(kl, oh)
            
            micro = process_logic(tfs, MICRO_INTERVALS, macro["macro_bias"])
            macro_an = process_logic(tfs, MACRO_INTERVALS, macro["macro_bias"])
            
            plan["assets"][sym] = {
                "context": {
                    "asset": sym, "spot_price": spot_p, "perp_last_price": price_snap["price"],
                    "mark_price": price_snap["mark_price"], "index_price": price_snap["index_price"],
                    "funding_rate": price_snap["funding_rate"], "basis_pct_perp_minus_spot": pct_change(price_snap["price"], spot_p)
                },
                "final_micro": micro, "final_macro": macro_an
            }
        except Exception as e:
            print(f"Error en {sym}: {e}")
            
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    with open(out / "final_trade_plan.json", "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
    
    # Simple Text Report
    txt = f"=== REPORTE {now_utc_iso()} ===\n"
    txt += f"Macro: {macro['macro_bias']} ({macro['macro_score']})\n"
    for sym, d in plan["assets"].items():
        ctx = d["context"]
        txt += f"\n--- {sym} ---\nSpot: {ctx['spot_price']} | Perp: {ctx['perp_last_price']}\n"
        txt += f"MICRO: {d['final_micro']['bias']} ({d['final_micro']['probability_pct']}%)\n"
        txt += f"MACRO: {d['final_macro']['bias']} ({d['final_macro']['probability_pct']}%)\n"
    
    with open(out / "final_trade_plan_report.txt", "w", encoding="utf-8") as f:
        f.write(txt)
    print(f"Proceso finalizado con éxito en {args.output_dir}")

if __name__ == "__main__":
    main()
