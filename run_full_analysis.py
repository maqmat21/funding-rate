#!/usr/bin/env python3
"""
run_full_analysis.py

Pipeline integral consolidado para análisis operativo de trading (MICRO/MACRO).
Versión Robusta con Bypass de Restricción (451) y Estructura JSON Específica.
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

# --- Configuración y Constantes ---
SPOT_BASE = "https://data-api.binance.vision" 
FUTURES_BASES = [
    "https://fapi.binance.com",
    "https://fapi.binance.me",
    "https://fapi.binance.vision",
    "https://fapi.binance.co",
    "https://fapi.binance.org",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com"
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

# --- Cliente HTTP con Rotación de Espejos ---

class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html",
        })

    def get_json(self, base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Si es futures, rotamos bases
        is_futures = any(b in base_url for b in FUTURES_BASES) or path.startswith("/fapi")
        bases = FUTURES_BASES if is_futures else [base_url]
        
        for b in bases:
            url = f"{b}{path}"
            try:
                r = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                # Si es 403 o 451, es bloqueo, probamos siguiente espejo
                if r.status_code in (403, 451):
                    continue
            except Exception:
                continue
        return None

# --- Módulo Binance Data ---

def get_futures_snapshot(client: HttpClient, symbol: str) -> Dict[str, Any]:
    base = FUTURES_BASES[0]
    p_data = client.get_json(base, "/fapi/v2/ticker/price", {"symbol": symbol}) or {}
    premium = client.get_json(base, "/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
    oi = client.get_json(base, "/fapi/v1/openInterest", {"symbol": symbol}) or {}
    
    return {
        "price": safe_float(p_data.get("price")),
        "mark_price": safe_float(premium.get("markPrice")),
        "index_price": safe_float(premium.get("indexPrice")),
        "funding_rate": safe_float(premium.get("lastFundingRate")),
        "oi_current": safe_float(oi.get("openInterest"))
    }

def get_spot_price(client: HttpClient, symbol: str) -> Optional[float]:
    # Intentamos spot directo (binance.vision suele estar abierto)
    data = client.get_json(SPOT_BASE, "/api/v3/ticker/price", {"symbol": symbol})
    if not data:
        # Fallback a fapi indexPrice que es un proxy del spot
        base = FUTURES_BASES[0]
        f_data = client.get_json(base, "/fapi/v1/premiumIndex", {"symbol": symbol}) or {}
        return safe_float(f_data.get("indexPrice"))
    return safe_float(data.get("price"))

def get_klines(client: HttpClient, symbol: str, interval: str) -> List[Dict[str, Any]]:
    base = FUTURES_BASES[0]
    data = client.get_json(base, "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT}) or []
    return [
        {
            "open": safe_float(r[1]), "high": safe_float(r[2]), "low": safe_float(r[3]), "close": safe_float(r[4]),
            "volume": safe_float(r[5])
        } for r in data
    ]

def get_oi_hist(client: HttpClient, symbol: str, interval: str) -> List[float]:
    base = FUTURES_BASES[0]
    p = interval if interval in {"5m", "15m", "30m", "1h", "4h", "1d"} else "5m"
    data = client.get_json(base, "/futures/data/openInterestHist", {"symbol": symbol, "period": p, "limit": OI_HIST_LIMIT}) or []
    return [safe_float(r.get("sumOpenInterest")) for r in data if r.get("sumOpenInterest") is not None]

# --- Indicadores y Análisis ---

def ema(values: List[float], period: int) -> List[Optional[float]]:
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

def rsi(values: List[float], period: int = 14) -> List[Optional[float]]:
    res: List[Optional[float]] = [None] * len(values)
    if len(values) <= period: return res
    deltas = [values[i] - values[i-1] for i in range(1, len(values))]
    gains = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    if avg_l == 0: res[period] = 100.0
    else: res[period] = 100.0 - (100.0 / (1.0 + (avg_g / avg_l)))
    for i in range(period+1, len(values)):
        avg_g = ((avg_g * (period-1)) + gains[i-1]) / period
        avg_l = ((avg_l * (period-1)) + losses[i-1]) / period
        if avg_l == 0: res[i] = 100.0
        else: res[i] = 100.0 - (100.0 / (1.0 + (avg_g / avg_l)))
    return res

def macd_bullish(values: List[float]) -> bool:
    e12 = ema(values, 12); e26 = ema(values, 26)
    if not e12 or not e26: return False
    l12 = last_non_none(e12); l26 = last_non_none(e26)
    if l12 is None or l26 is None: return False
    return l12 > l26

def analyze_timeframe(klines: List[Dict[str, Any]], oi_hist: List[float]) -> Dict[str, Any]:
    closes = [r["close"] for r in klines if r["close"] is not None]
    highs = [r["high"] for r in klines if r["high"] is not None]
    lows = [r["low"] for r in klines if r["low"] is not None]
    
    if not closes: return {"price": None, "ema20": None, "h20": None, "l20": None, "rsi": None, "macd_bull": False, "oi_slope": "plana"}
    
    cur_p = closes[-1]
    e20 = ema(closes, 20); e50 = ema(closes, 50); rs = rsi(closes)
    
    oi_cur = oi_hist[-1] if oi_hist else None
    oi_pre = oi_hist[-2] if len(oi_hist) >= 2 else None
    
    return {
        "price": cur_p,
        "ema20": last_non_none(e20),
        "ema50": last_non_none(e50),
        "h20": max(highs[-20:]) if len(highs) >= 20 else max(highs),
        "l20": min(lows[-20:]) if len(lows) >= 20 else min(lows),
        "rsi": last_non_none(rs),
        "macd_bull": macd_bullish(closes),
        "oi_slope": infer_slope(oi_cur, oi_pre)
    }

def score_and_levels(tf_map: Dict[str, Any], intervals: List[str], macro_bias: str) -> Dict[str, Any]:
    primary = tf_map[intervals[0]]
    cur = primary["price"]
    if cur is None: return {
        "semaphore": "rojo", "bias": "no_trade", "probability_pct": 0, 
        "technical_reasoning": ["Faltan datos"], "note": "Sin datos suficientes para operar.",
        "entry_ideal": None, "stop_loss_technical": None, "target_1": None, "target_2": None
    }
    
    bull = 0; bear = 0; reasons = []
    e20 = primary["ema20"]; e50 = primary["ema50"]; rs = primary["rsi"]; mb = primary["macd_bull"]; os = primary["oi_slope"]
    
    if e20:
        if cur > e20: bull += 2; reasons.append("Precio > EMA20")
        else: bear += 2; reasons.append("Precio < EMA20")
    if e50:
        if cur > e50: bull += 2; reasons.append("Precio > EMA50")
        else: bear += 2; reasons.append("Precio < EMA50")
    if rs:
        if rs > 55: bull += 1; reasons.append("RSI Bullish")
        elif rs < 45: bear += 1; reasons.append("RSI Bearish")
    if mb: bull += 2; reasons.append("MACD Bullish")
    else: bear += 2; reasons.append("MACD Bearish")
    if os == "alcista" and mb: bull += 1; reasons.append("OI apoyando alza")
    
    # Macro adjustment
    if macro_bias == "RISK_OFF":
        bull -= 2; bear += 2; reasons.append("Macro RISK_OFF (Presión Bajista)")
    elif macro_bias == "RISK_ON":
        bull += 1; bear -= 1; reasons.append("Macro RISK_ON (Apoyo Alcista)")

    bias = "long" if bull > bear + 1 else "short" if bear > bull + 1 else "no_trade"
    prob = int(clamp(50 + (abs(bull - bear) * 6), 35, 90)) if bias != "no_trade" else 45
    
    # Levels
    h_ref = max([tf_map[i]["h20"] for i in intervals if tf_map[i]["h20"] is not None] or [cur * 1.02])
    l_ref = min([tf_map[i]["l20"] for i in intervals if tf_map[i]["l20"] is not None] or [cur * 0.98])
    
    entry = stop = t1 = t2 = None
    if bias == "long":
        entry = max(cur, e20 or cur)
        stop = l_ref if l_ref < entry else entry * 0.985
        t1 = entry + (entry - stop) * 1.5; t2 = entry + (entry - stop) * 3
    elif bias == "short":
        entry = min(cur, e20 or cur)
        stop = h_ref if h_ref > entry else entry * 1.015
        t1 = entry - (stop - entry) * 1.5; t2 = entry - (stop - entry) * 3

    return {
        "semaphore": "verde" if prob >= 75 else "amarillo" if prob >= 58 else "rojo",
        "bias": bias, "probability_pct": prob, "technical_reasoning": reasons,
        "entry_ideal": round_or_none(entry), "stop_loss_technical": round_or_none(stop),
        "target_1": round_or_none(t1), "target_2": round_or_none(t2),
        "note": "Disciplina: Respetar stop técnico. Si el riesgo es > 3% del capital, reducir palancaje."
    }

# --- Macro y Noticias ---

def fetch_macro_data(client: HttpClient) -> Dict[str, Any]:
    res = {"macro_bias": "NEUTRAL", "macro_score": 50, "assets": {}}
    if not yf: return res
    score = 0
    for tick, name in [("DX-Y.NYB", "DXY"), ("^TNX", "US10Y")]:
        try:
            tk = yf.Ticker(tick)
            hist = tk.history(period="3d", interval="1h")
            if hist.empty and tick == "DX-Y.NYB":
                hist = yf.Ticker("DX=F").history(period="3d", interval="1h")
            
            c = hist["Close"].tolist()
            last = c[-1] if c else None
            e20 = last_non_none(ema(c, 20))
            res["assets"][name] = {"price": round_or_none(last, 4), "trend": infer_slope(last, e20)}
            if last and e20:
                score += (1 if last > e20 else -1)
        except Exception:
            res["assets"][name] = {"price": None, "trend": "unknown"}
            
    res["macro_bias"] = "RISK_OFF" if score >= 1 else "RISK_ON" if score <= -1 else "NEUTRAL"
    res["macro_score"] = 50 + (score * 15)
    return res

def fetch_news(client: HttpClient) -> List[str]:
    titles = []
    urls = ["https://finance.yahoo.com/topic/economic-news/", "https://finance.yahoo.com/topic/latest-news/"]
    for url in urls:
        try:
            r = client.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            for tag in soup.find_all("h3"):
                txt = tag.get_text(strip=True)
                if len(txt) > 25 and txt not in titles:
                    titles.append(txt)
            if len(titles) >= 8: break
        except Exception: continue
    return titles[:8]

# --- Reporte Texto ---

def generate_txt_report(plan: Dict[str, Any]) -> str:
    m = plan["macro_context_auto"]
    lines = [
        "==================================================",
        "   REPORTE INTEGRAL DE ANÁLISIS OPERATIVO",
        "==================================================",
        f"Generado (UTC): {plan['meta']['generated_at_utc']}\n",
        "--- BLOQUE MACRO AUTOMÁTICO ---",
        f"Sesgo Macro: {m.get('macro_bias')} (Score: {m.get('macro_score')})",
        f"DXY: {m.get('assets', {}).get('DXY', {}).get('price')} | US10Y: {m.get('assets', {}).get('US10Y', {}).get('price')}\n",
        "--- EVENTOS MACRO CONSIDERADOS ---"
    ]
    for t in plan["events_considered_titles"]: lines.append(f"- {t}")
    if not plan["events_considered_titles"]: lines.append("No se detectaron eventos.")

    for sym, asset in plan["assets"].items():
        lines.append(f"\n==================== {sym} ====================")
        ctx = asset["context"]
        lines.append(f"Spot: {ctx['spot_price']} | Perp: {ctx['perp_last_price']} | Funding: {ctx['funding_rate']}")
        
        for name, block in [("MICRO", asset["final_micro"]), ("MACRO", asset["final_macro"])]:
            lines.append(f"\n[{name}]")
            lines.append(f"Semaforo: {block['semaphore'].upper()} | Sesgo: {block['bias'].upper()} ({block['probability_pct']}%)")
            lines.append(f"Reasoning: {', '.join(block['technical_reasoning'][:4])}")
            lines.append(f"NIVEL ENTRADA: {block['entry_ideal']}")
            lines.append(f"STOP TÉCNICO:  {block['stop_loss_technical']}")
            lines.append(f"TARGETS: T1={block['target_1']} | T2={block['target_2']}")
            lines.append(f"Nota: {block['note']}")
            
    lines.append("\n==================================================")
    lines.append("FIN DEL REPORTE. Analizar con prudencia.")
    return "\n".join(lines)

# --- Main Pipeline ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--output-dir", default="full_analysis_output_v2")
    args = parser.parse_args()
    
    print("Iniciando Pipeline Consolidado...")
    client = HttpClient()
    
    # Macro y Noticias
    macro = fetch_macro_data(client)
    news = fetch_news(client)
    
    plan = {
        "meta": {"generated_at_utc": now_utc_iso(), "notes": ["Resultado automatizado", "Stop técnico obligatorio"]},
        "macro_context_auto": macro,
        "events_considered_titles": news,
        "assets": {}
    }
    
    for sym in args.symbols:
        print(f"Procesando {sym}...")
        try:
            spot = get_spot_price(client, sym)
            f_snap = get_futures_snapshot(client, sym)
            
            tf_data = {}
            for inv in ALL_INTERVALS:
                klines = get_klines(client, sym, inv)
                oi_h = get_oi_hist(client, sym, inv)
                tf_data[inv] = analyze_timeframe(klines, oi_h)
            
            micro = score_and_levels(tf_data, MICRO_INTERVALS, macro["macro_bias"])
            mac_anal = score_and_levels(tf_data, MACRO_INTERVALS, macro["macro_bias"])
            
            plan["assets"][sym] = {
                "context": {
                    "asset": sym,
                    "spot_price": spot,
                    "perp_last_price": f_snap["price"],
                    "mark_price": f_snap["mark_price"],
                    "index_price": f_snap["index_price"],
                    "funding_rate": f_snap["funding_rate"],
                    "basis_pct_perp_minus_spot": pct_change(f_snap["price"], spot)
                },
                "final_micro": micro,
                "final_macro": mac_anal
            }
        except Exception as e:
            print(f"Error procesando {sym}: {e}")
            
    # Guardar
    out_path = Path(args.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    with open(out_path / "final_trade_plan.json", "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
        
    rpt = generate_txt_report(plan)
    with open(out_path / "final_trade_plan_report.txt", "w", encoding="utf-8") as f:
        f.write(rpt)
        
    print(f"Éxito: Reportes generados en {args.output_dir}")

if __name__ == "__main__":
    main()
