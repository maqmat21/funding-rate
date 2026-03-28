#!/usr/bin/env python3
"""
run_full_analysis.py

Pipeline integral consolidado para análisis operativo de trading (MICRO/MACRO).
Blindaje Total para GitHub Actions (Resiliencia Extrema).
Mecánica: Rotación de espejos + Fallback Yahoo Finance + Persistencia de Precios.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# --- Manejo de Datos ---
try:
    import yfinance as yf
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
except ImportError:
    yf = None

# --- Configuración y Constantes ---
# Lista masiva de espejos (Shuffle activo para evadir flags de IP)
BINANCE_MIRRORS = [
    "https://fapi.binance.me",
    "https://api.binance.me",
    "https://fapi.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://fapi.binance.co",
    "https://fapi.binance.org",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
    "https://data-api.binance.vision"
]

DEFAULT_SYMBOLS = ["BTCUSDT", "SIRENUSDT"]
MICRO_INTERVALS = ["3m", "5m", "15m", "1h"]
MACRO_INTERVALS = ["1h", "4h", "1d"]
ALL_INTERVALS = sorted(list(set(MICRO_INTERVALS + MACRO_INTERVALS)))

HTTP_TIMEOUT = 4
HISTORY_DB = "price_history_v3.json"

# --- Utilidades ---

def log_print(msg: str):
    print(msg)
    sys.stdout.flush()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(v: Any) -> Optional[float]:
    if v is None: return None
    try: return float(v)
    except (TypeError, ValueError): return None

def round_or_none(v: Optional[float], d: int = 8) -> Optional[float]:
    if v is None: return None
    return round(v, d)

def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0): return None
    return ((current - previous) / previous) * 100.0

# --- Persistencia de Precios (No-Nulls Policy) ---

def load_history() -> Dict[str, Any]:
    if os.path.exists(HISTORY_DB):
        try:
            with open(HISTORY_DB, 'r') as f: return json.load(f)
        except: return {}
    return {}

def save_to_history(symbol: str, price: float):
    # Solo guardamos si el precio es válido
    db = load_history()
    db[symbol] = {"price": price, "updated_at": now_utc_iso()}
    try:
        with open(HISTORY_DB, 'w') as f: json.dump(db, f, indent=2)
    except: pass

def get_last_known_price(symbol: str) -> float:
    db = load_history()
    return db.get(symbol, {}).get("price", 0.0)

# --- Cliente HTTP Robusto ---

class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html",
        })

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Mezclar espejos para evitar bloqueos por patrón
        mirrors = list(BINANCE_MIRRORS)
        random.shuffle(mirrors)
        for b in mirrors[:8]:
            try:
                r = self.session.get(f"{b}{path}", params=params, timeout=HTTP_TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, (list, dict)): return data
                if r.status_code not in (403, 429, 451): break
            except: continue
        return None

# --- Fallbacks Yahoo Finance ---

def map_yf_inv(inv: str) -> str:
    m = {"3m": "2m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "1h", "1d": "1d"}
    return m.get(inv, "60m")

def get_kl_yf(symbol: str, interval: str) -> List[Dict[str, Any]]:
    if not yf: return []
    # Usar tickers conocidos si es posible
    t_map = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD"}
    y_sym = t_map.get(symbol)
    if not y_sym: return []
    try:
        y_inv = map_yf_inv(interval)
        tk = yf.Ticker(y_sym)
        df = tk.history(period="5d" if "m" in y_inv else "1mo", interval=y_inv)
        if df.empty: return []
        return [
            {"open": float(row["Open"]), "high": float(row["High"]), "low": float(row["Low"]), 
             "close": float(row["Close"]), "volume": float(row["Volume"])} 
            for _, row in df.tail(200).iterrows()
        ]
    except: return []

# --- Data Fetchers ---

def get_snap(client: HttpClient, symbol: str) -> Dict[str, Any]:
    p_data = client.get_json("/fapi/v2/ticker/price", {"symbol": symbol}) or {}
    prem = client.get_json("/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
    oi = client.get_json("/fapi/v1/openInterest", {"symbol": symbol}) or {}
    
    price = safe_float(p_data.get("price")) or safe_float(prem.get("indexPrice"))
    if price is None:
        price = get_kl_yf(symbol, "1h")[-1]["close"] if get_kl_yf(symbol, "1h") else get_last_known_price(symbol)
    
    if price > 0: save_to_history(symbol, price)

    return {
        "price": price, "mark_price": safe_float(prem.get("markPrice")) or price,
        "index_price": safe_float(prem.get("indexPrice")) or price,
        "funding_rate": safe_float(prem.get("lastFundingRate")) or 0.0001,
        "oi_current": safe_float(oi.get("openInterest"))
    }

def get_spot_p(client: HttpClient, symbol: str) -> float:
    data = client.get_json("/api/v3/ticker/price", {"symbol": symbol})
    if data:
        p = safe_float(data.get("price"))
        if p: 
            save_to_history(symbol, p)
            return p
    # Fallback to last known if even spot is blocked
    return get_last_known_price(symbol)

def get_klines_all(client: HttpClient, symbol: str, interval: str) -> List[Dict[str, Any]]:
    data = client.get_json("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": 200})
    if not data: data = client.get_json("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": 200})
    if not data: return get_kl_yf(symbol, interval)
    return [{"open": safe_float(r[1]), "high": safe_float(r[2]), "low": safe_float(r[3]), "close": safe_float(r[4]), "volume": safe_float(r[5])} for r in data]

# --- Análisis Técnico (EMA, RSI, MACD) ---

def ema(v: List[float], p: int):
    if len(v) < p: return [None]*len(v)
    m = 2/(p+1); r = [None]*len(v); s = sum(v[:p])/p; r[p-1] = s
    for i in range(p, len(v)): s = (v[i]-s)*m + s; r[i] = s
    return r

def rsi(v: List[float], p: int=14):
    if len(v) <= p: return [None]*len(v)
    r = [None]*len(v); d = [v[i]-v[i-1] for i in range(1, len(v))]
    g = [x if x>0 else 0 for x in d]; l = [abs(x) if x<0 else 0 for x in d]
    ag = sum(g[:p])/p; al = sum(l[:p])/p
    if al == 0: r[p] = 100.0
    else: r[p] = 100.0 - (100.0/(1.0+ag/al))
    for i in range(p+1, len(v)):
        ag = (ag*(p-1)+g[i-1])/p; al = (al*(p-1)+l[i-1])/p
        if al == 0: r[i] = 100.0
        else: r[i] = 100.0 - (100.0/(1.0+ag/al))
    return r

def analyze(kl: List[Dict[str, Any]], sym: str) -> Dict[str, Any]:
    cl = [x["close"] for x in kl if x["close"] is not None]
    if not cl: return {"price": None, "ema20": None, "rsi": None, "macd_bull": False, "h20": None, "l20": None}
    e20 = ema(cl, 20); e50 = ema(cl, 50); rs = rsi(cl)
    e12 = ema(cl, 12); e26 = ema(cl, 26)
    mb = (e12[-1] > e26[-1]) if e12[-1] and e26[-1] else False
    return {"price": cl[-1], "ema20": e20[-1], "ema50": e50[-1], "rsi": rs[-1], "macd_bull": mb, "h20": max([x["high"] for x in kl[-20:]]) if len(kl)>=20 else cl[-1]*1.01, "l20": min([x["low"] for x in kl[-20:]]) if len(kl)>=20 else cl[-1]*0.99}

def score(tfs: Dict[str, Any], intervals: List[str], macro: str) -> Dict[str, Any]:
    p = tfs[intervals[0]]
    if p["price"] is None: return {"semaphore": "rojo", "bias": "no_trade", "probability_pct": 0, "technical_reasoning": ["Sin datos"], "note": "Bloqueo extremo.", "entry_ideal": None, "stop_loss_technical": None, "target_1": None, "target_2": None}
    bull=0; bear=0; r=[]; cur=p["price"]
    if p["ema20"]: 
        if cur > p["ema20"]: bull+=2; r.append("Precio > EMA20")
        else: bear+=2; r.append("Precio < EMA20")
    if p["rsi"]:
        if p["rsi"] > 55: bull+=1; r.append("RSI Bull")
        elif p["rsi"] < 45: bear+=1; r.append("RSI Bear")
    if p["macd_bull"]: bull+=2; r.append("MACD Bull")
    else: bear+=2; r.append("MACD Bear")
    if macro == "RISK_OFF": bull-=3; bear+=3; r.append("Macro RISK_OFF")
    
    bias = "long" if bull > bear+1 else "short" if bear > bull+1 else "no_trade"
    pr = int(max(35, min(90, 50 + abs(bull-bear)*6))) if bias != "no_trade" else 45
    h_list = [tfs[i]["h20"] for i in intervals if tfs[i].get("h20")]
    l_list = [tfs[i]["l20"] for i in intervals if tfs[i].get("l20")]
    h = max(h_list) if h_list else cur*1.01
    l = min(l_list) if l_list else cur*0.99
    st = l if bias == "long" else h
    return {"semaphore": "verde" if pr>=75 else "amarillo" if pr>=58 else "rojo", "bias": bias, "probability_pct": pr, "technical_reasoning": r, "entry_ideal": round_or_none(cur), "stop_loss_technical": round_or_none(st), "target_1": round_or_none(cur+(cur-st)*1.5 if bias=="long" else cur-(st-cur)*1.5), "target_2": round_or_none(cur+(cur-st)*3 if bias=="long" else cur-(st-cur)*3), "note": "Operativa adaptativa."}

# --- Macro y Noticias ---

def fetch_macro(client: HttpClient) -> Dict[str, Any]:
    res = {"macro_bias": "NEUTRAL", "macro_score": 50, "assets": {}}
    if not yf: return res
    score = 0
    for tick, name in [("DX-Y.NYB", "DXY"), ("^TNX", "US10Y")]:
        try:
            tk = yf.Ticker(tick); h = tk.history(period="5d", interval="1h"); c = h["Close"].tolist(); last = c[-1]; e20 = ema(c, 20)[-1]
            res["assets"][name] = {"price": round(last, 4), "trend": "alcista" if last > e20 else "bajista"}
            score += (1 if last > e20 else -1)
        except: res["assets"][name] = {"price": None, "trend": "unknown"}
    res["macro_bias"] = "RISK_OFF" if score >= 1 else "RISK_ON" if score <= -1 else "NEUTRAL"
    res["macro_score"] = 50 + score * 15
    return res

def fetch_news(client: HttpClient) -> List[str]:
    titles = []
    for u in ["https://finance.yahoo.com/topic/economic-news/", "https://finance.yahoo.com/news/"]:
        try:
            r = client.session.get(u, timeout=10); soup = BeautifulSoup(r.text, "html.parser")
            for t in soup.find_all(["h2", "h3"]):
                txt = t.get_text(strip=True)
                if len(txt) > 25 and txt not in titles: titles.append(txt)
            if len(titles) >= 8: break
        except: continue
    return titles[:10]

# --- Main ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--output-dir", default="full_analysis_output_v2")
    args = parser.parse_args()
    
    log_print("Iniciando Pipeline Ultra-Robusto v3.0...")
    client = HttpClient(); macro = fetch_macro(client); news = fetch_news(client)
    
    plan = {"meta": {"generated_at_utc": now_utc_iso(), "notes": ["Mirror Shuffle Active", "History Persitence Enabled", "No Nulls Policy 3.0"]}, "macro_context_auto": macro, "events_considered_titles": news, "assets": {}}
    
    for sym in args.symbols:
        log_print(f"Procesando {sym}...")
        try:
            snap = get_snap(client, sym); spot = get_spot_p(client, sym)
            tfs = {}
            for inv in ALL_INTERVALS:
                kl = get_klines_all(client, sym, inv)
                tfs[inv] = analyze(kl, sym)
            
            micro = score(tfs, MICRO_INTERVALS, macro["macro_bias"])
            mac = score(tfs, MACRO_INTERVALS, macro["macro_bias"])
            
            plan["assets"][sym] = {
                "context": {
                    "asset": sym, "spot_price": spot, "perp_last_price": snap["price"],
                    "mark_price": snap["mark_price"], "index_price": snap["index_price"],
                    "funding_rate": snap["funding_rate"], "basis_pct_perp_minus_spot": pct_change(snap["price"], spot)
                },
                "final_micro": micro, "final_macro": mac
            }
        except Exception as e: log_print(f"Error {sym}: {e}")
            
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    with open(out / "final_trade_plan.json", "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
    log_print(f"Reporte final generado en {args.output_dir}")

if __name__ == "__main__":
    main()
