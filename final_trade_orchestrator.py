#!/usr/bin/env python3
"""
final_trade_orchestrator.py

Tercer script del flujo.
Orquesta:
1) Snapshot Binance
2) Análisis técnico desde snapshot
3) Bloque macro automático de DXY y US10Y
4) Detección automática de títulos de eventos macro/noticiosos
5) Plan final operativo (MICRO + MACRO)

Entradas:
- binance_snapshot.json
- trade_analysis.json

Salida:
- final_trade_plan.json
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    import yfinance as yf
    # Silenciar logs internos de yfinance que pueden ser ruidosos ante fallos de conexión
    import logging
    logging.getLogger('yfinance').setLevel(logging.CRITICAL)
except Exception:
    yf = None


DEFAULT_SNAPSHOT = "binance_snapshot.json"
DEFAULT_ANALYSIS = "trade_analysis.json"
DEFAULT_OUTPUT = "final_trade_plan.json"

HTTP_TIMEOUT = 20


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def round_or_none(v: Optional[float], digits: int = 8) -> Optional[float]:
    if v is None:
        return None
    return round(v, digits)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/json",
    })
    return s


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if not values:
        return []
    result: List[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result
    multiplier = 2 / (period + 1)
    sma = sum(values[:period]) / period
    result[period - 1] = sma
    prev = sma
    for i in range(period, len(values)):
        prev = ((values[i] - prev) * multiplier) + prev
        result[i] = prev
    return result


def last_non_none(values: List[Optional[float]]) -> Optional[float]:
    for item in reversed(values):
        if item is not None:
            return item
    return None


def infer_slope(cur: Optional[float], prev: Optional[float]) -> Optional[str]:
    if cur is None or prev is None:
        return None
    if cur > prev:
        return "alcista"
    if cur < prev:
        return "bajista"
    return "plana"


def fetch_yahoo_chart_history(ticker: str, range_: str = "5d", interval: str = "1h") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if yf is None:
        return rows

    try:
        # Algunos tickers pueden fallar en periodos específicos, intentamos descarga silenciosa
        tk = yf.Ticker(ticker)
        hist = tk.history(period=range_, interval=interval, auto_adjust=False, raise_errors=False)
        
        if hist is None or hist.empty:
            # Reintento con símbolo alternativo para DXY si es el caso
            if ticker == "DX-Y.NYB":
                tk = yf.Ticker("DX=F")
                hist = tk.history(period=range_, interval=interval, auto_adjust=False, raise_errors=False)
            
        if hist is None or hist.empty:
            return rows
            
        for idx, row in hist.iterrows():
            rows.append({
                "timestamp": idx.to_pydatetime().astimezone(timezone.utc).isoformat(),
                "open": float(row["Open"]) if not math.isnan(row["Open"]) else None,
                "high": float(row["High"]) if not math.isnan(row["High"]) else None,
                "low": float(row["Low"]) if not math.isnan(row["Low"]) else None,
                "close": float(row["Close"]) if not math.isnan(row["Close"]) else None,
                "volume": float(row["Volume"]) if not math.isnan(row["Volume"]) else None,
            })
        return rows
    except Exception:
        return []


def scrape_yahoo_quote_price(ticker: str, session: requests.Session) -> Optional[float]:
    url = f"https://finance.yahoo.com/quote/{ticker}"
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        html = r.text

        patterns = [
            r'fin-streamer[^>]+data-field="regularMarketPrice"[^>]*value="([^"]+)"',
            r'"regularMarketPrice"\s*:\s*\{"raw"\s*:\s*([0-9.]+)',
            r'"currentPrice"\s*:\s*\{"raw"\s*:\s*([0-9.]+)',
        ]
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                return float(m.group(1).replace(",", ""))
        return None
    except Exception:
        return None


def fetch_yahoo_quote_page_news_titles(path: str, session: requests.Session, limit: int = 6) -> List[str]:
    url = f"https://finance.yahoo.com{path}"
    titles: List[str] = []
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for tag in soup.find_all(["h3", "h2", "a"]):
            text = tag.get_text(" ", strip=True)
            if not text:
                continue
            if len(text) < 25:
                continue
            if text in titles:
                continue
            titles.append(text)
            if len(titles) >= limit:
                break
        return titles[:limit]
    except Exception:
        return []


def get_macro_market_data(session: requests.Session) -> Dict[str, Any]:
    dxy_hist = fetch_yahoo_chart_history("DX-Y.NYB", range_="5d", interval="1h")
    us10y_hist = fetch_yahoo_chart_history("^TNX", range_="5d", interval="1h")

    dxy_price_fallback = scrape_yahoo_quote_price("DX-Y.NYB", session)
    us10y_price_fallback = scrape_yahoo_quote_price("%5ETNX", session)

    def summarize_series(hist: List[Dict[str, Any]], price_fallback: Optional[float]) -> Dict[str, Any]:
        closes = [x["close"] for x in hist if x.get("close") is not None]
        current = closes[-1] if closes else price_fallback
        e20 = last_non_none(ema(closes, 20)) if closes else None
        e50 = last_non_none(ema(closes, 50)) if closes else None
        prev_close = closes[-2] if len(closes) >= 2 else None
        recent_1h = closes[-2:] if len(closes) >= 2 else closes
        recent_4h = closes[-5:] if len(closes) >= 5 else closes
        structure_1h = infer_slope(recent_1h[-1] if len(recent_1h) >= 1 else None,
                                   recent_1h[0] if len(recent_1h) >= 2 else None)
        structure_4h = infer_slope(recent_4h[-1] if len(recent_4h) >= 1 else None,
                                   recent_4h[0] if len(recent_4h) >= 2 else None)

        momentum_slope = None
        if closes and prev_close not in (None, 0):
            momentum_slope = current - prev_close if current is not None else None

        ema_trend = None
        if e20 is not None and e50 is not None:
            if e20 > e50:
                ema_trend = "alcista"
            elif e20 < e50:
                ema_trend = "bajista"
            else:
                ema_trend = "plana"

        breakout_24h = "none"
        if len(closes) >= 24 and current is not None:
            prev_24 = closes[-24:]
            max24 = max(prev_24)
            min24 = min(prev_24)
            if current > max24:
                breakout_24h = "up"
            elif current < min24:
                breakout_24h = "down"

        state_summary = "mixto"
        points_up = 0
        points_down = 0
        for item in [ema_trend, structure_1h, structure_4h]:
            if item == "alcista":
                points_up += 1
            elif item == "bajista":
                points_down += 1
        if current is not None and e20 is not None:
            if current > e20:
                points_up += 1
            elif current < e20:
                points_down += 1

        if points_up >= 3:
            state_summary = "tendencia_fuerte"
        elif points_down >= 3:
            state_summary = "debilidad_fuerte"
        else:
            state_summary = "mixto"

        return {
            "price": round_or_none(current, 6),
            "ema20": round_or_none(e20, 6),
            "ema50": round_or_none(e50, 6),
            "ema_trend": ema_trend,
            "structure_1h": structure_1h,
            "structure_4h": structure_4h,
            "momentum_slope": round_or_none(momentum_slope, 6),
            "momentum_state": infer_slope(current, prev_close),
            "breakout_24h": breakout_24h,
            "state_summary": state_summary,
            "history_source_rows": len(hist),
        }

    dxy = summarize_series(dxy_hist, dxy_price_fallback)
    us10y = summarize_series(us10y_hist, us10y_price_fallback)

    macro_score = 50
    dxy_up = dxy.get("ema_trend") == "alcista" or dxy.get("structure_4h") == "alcista"
    dxy_down = dxy.get("ema_trend") == "bajista" or dxy.get("structure_4h") == "bajista"

    yld_up = us10y.get("momentum_state") == "alcista" or us10y.get("structure_4h") == "alcista"
    yld_down = us10y.get("momentum_state") == "bajista" or us10y.get("structure_4h") == "bajista"

    if dxy_up:
        macro_score += 15
    if yld_up:
        macro_score += 15
    if dxy_down:
        macro_score -= 10
    if yld_down:
        macro_score -= 10

    macro_score = int(clamp(macro_score, 0, 100))

    if macro_score >= 65:
        macro_bias = "RISK_OFF"
        btc_prob_bias = "bajista"
        alts_prob_bias = "bajista"
        confidence = "alta" if macro_score >= 80 else "media"
    elif macro_score <= 40:
        macro_bias = "RISK_ON"
        btc_prob_bias = "alcista"
        alts_prob_bias = "alcista"
        confidence = "alta" if macro_score <= 25 else "media"
    else:
        macro_bias = "NEUTRAL"
        btc_prob_bias = "mixto"
        alts_prob_bias = "mixto"
        confidence = "media"

    alerts = {
        "macro_shift": True if abs(macro_score - 50) >= 20 else False,
        "volatility_expansion": True if dxy.get("breakout_24h") != "none" or us10y.get("breakout_24h") != "none" else False,
    }

    return {
        "timestamp": now_utc_iso(),
        "macro_bias": macro_bias,
        "macro_score": macro_score,
        "dxy": dxy,
        "us10y": {
            "price": us10y.get("price"),
            "direction_1h": us10y.get("structure_1h"),
            "momentum_state": us10y.get("momentum_state"),
            "ema20": us10y.get("ema20"),
            "ema50": us10y.get("ema50"),
            "structure_4h": us10y.get("structure_4h"),
            "state_summary": us10y.get("state_summary"),
            "history_source_rows": us10y.get("history_source_rows"),
        },
        "risk_environment": {
            "btc_probability_bias": btc_prob_bias,
            "altcoins_probability_bias": alts_prob_bias,
            "confidence_level": confidence,
        },
        "alerts": alerts,
    }


def get_macro_event_titles(session: requests.Session, limit_total: int = 8) -> List[str]:
    candidates: List[str] = []

    paths = [
        "/topic/economic-news/",
        "/quote/DX-Y.NYB/news/",
        "/quote/%5ETNX/news/",
        "/topic/latest-news/",
    ]

    for path in paths:
        titles = fetch_yahoo_quote_page_news_titles(path, session, limit=5)
        for t in titles:
            low = t.lower()
            keywords = [
                "fed", "federal reserve", "inflation", "treasury", "yield", "dollar",
                "oil", "war", "iran", "economy", "rates", "stocks", "bitcoin", "crypto",
                "market", "tariff", "recession"
            ]
            if any(k in low for k in keywords):
                if t not in candidates:
                    candidates.append(t)

    return candidates[:limit_total]


def adjust_probability(base_prob: int, direction: str, symbol: str, macro: Dict[str, Any]) -> int:
    prob = base_prob
    macro_bias = macro.get("macro_bias")
    alt_bias = macro.get("risk_environment", {}).get("altcoins_probability_bias")
    btc_bias = macro.get("risk_environment", {}).get("btc_probability_bias")

    is_alt = symbol != "BTCUSDT"

    if direction == "long":
        if macro_bias == "RISK_OFF":
            prob -= 8 if is_alt else 6
        elif macro_bias == "RISK_ON":
            prob += 6 if is_alt else 5
    elif direction == "short":
        if macro_bias == "RISK_OFF":
            prob += 7 if is_alt else 5
        elif macro_bias == "RISK_ON":
            prob -= 7 if is_alt else 5

    if is_alt and alt_bias == "bajista" and direction == "long":
        prob -= 5
    if is_alt and alt_bias == "bajista" and direction == "short":
        prob += 4

    if (not is_alt) and btc_bias == "bajista" and direction == "long":
        prob -= 4
    if (not is_alt) and btc_bias == "bajista" and direction == "short":
        prob += 3

    if macro.get("alerts", {}).get("macro_shift"):
        if direction == "long" and macro_bias == "RISK_OFF":
            prob -= 3
        elif direction == "short" and macro_bias == "RISK_OFF":
            prob += 2

    return int(clamp(prob, 35, 90))


def probability_to_semaphore(prob: int) -> str:
    if prob >= 75:
        return "verde"
    if prob >= 58:
        return "amarillo"
    return "rojo"


def enrich_block(block: Dict[str, Any], symbol: str, macro: Dict[str, Any]) -> Dict[str, Any]:
    bias = block.get("bias", "no_trade")
    base_prob = int(block.get("probability_pct", 50))

    final_prob = adjust_probability(base_prob, bias, symbol, macro) if bias in {"long", "short"} else max(40, base_prob - 2)
    final_semaphore = probability_to_semaphore(final_prob)

    macro_notes = []
    macro_bias = macro.get("macro_bias")
    macro_score = macro.get("macro_score")
    dxy = macro.get("dxy", {})
    us10y = macro.get("us10y", {})

    macro_notes.append(f"Macro bias: {macro_bias} ({macro_score}/100)")
    if dxy:
        macro_notes.append(
            f"DXY {dxy.get('price')} | trend={dxy.get('ema_trend')} | 1H={dxy.get('structure_1h')} | 4H={dxy.get('structure_4h')}"
        )
    if us10y:
        macro_notes.append(
            f"US10Y {us10y.get('price')} | 1H={us10y.get('direction_1h')} | momentum={us10y.get('momentum_state')} | 4H={us10y.get('structure_4h')}"
        )

    discipline = block.get("discipline_note")
    if bias == "no_trade":
        macro_notes.append("Sin ventaja suficiente; preservar capital tiene prioridad.")
    elif macro_bias == "RISK_OFF" and bias == "long":
        macro_notes.append("El macro presiona en contra del lado long; exigir confirmación extra.")
    elif macro_bias == "RISK_OFF" and bias == "short":
        macro_notes.append("El macro favorece o tolera mejor el lado short.")
    elif macro_bias == "RISK_ON" and bias == "long":
        macro_notes.append("El macro favorece o tolera mejor el lado long.")

    result = dict(block)
    result["probability_pct"] = final_prob
    result["semaphore"] = final_semaphore
    result["macro_overlay"] = macro_notes
    result["discipline_note"] = discipline
    return result


def build_final_plan(snapshot: Dict[str, Any], analysis: Dict[str, Any], macro: Dict[str, Any], event_titles: List[str]) -> Dict[str, Any]:
    assets_out: Dict[str, Any] = {}

    for symbol, asset in analysis.get("assets", {}).items():
        micro = enrich_block(asset.get("micro", {}), symbol, macro)
        macro_block = enrich_block(asset.get("macro", {}), symbol, macro)

        assets_out[symbol] = {
            "context": asset.get("context", {}),
            "events_considered_titles": event_titles,
            "final_micro": micro,
            "final_macro": macro_block,
        }

    return {
        "meta": {
            "generator": "final_trade_orchestrator.py",
            "generated_at_utc": now_utc_iso(),
            "source_snapshot_captured_at_utc": snapshot.get("meta", {}).get("captured_at_utc"),
            "notes": [
                "El resultado se emite automáticamente sin requerir aceptación previa.",
                "Los eventos macro/noticiosos se muestran solo como títulos.",
                "Si luego aparece un evento relevante no contemplado, debe analizarse en línea fuera del script.",
                "El stop loss siempre se interpreta como técnico; si el PnL/riesgo es excesivo, se reduce tamaño o no se entra."
            ],
        },
        "macro_context_auto": macro,
        "events_considered_titles": event_titles,
        "assets": assets_out,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orquestador final de análisis técnico + macro + eventos.")
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT, help="Ruta del binance_snapshot.json")
    parser.add_argument("--analysis", default=DEFAULT_ANALYSIS, help="Ruta del trade_analysis.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Ruta del JSON final de salida")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        snapshot = load_json(args.snapshot)
        analysis = load_json(args.analysis)

        session = build_session()
        macro = get_macro_market_data(session)
        event_titles = get_macro_event_titles(session, limit_total=8)

        final_plan = build_final_plan(snapshot, analysis, macro, event_titles)
        save_json(args.output, final_plan)

        print(f"Archivo generado correctamente: {args.output}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())