#!/usr/bin/env python3
"""
trade_analysis_from_snapshot.py

Lee el JSON generado por binance_market_snapshot.py y construye un análisis
operativo estructurado para BTC y SIREN, separado en:
- MICRO: operaciones cortas de minutos
- MACRO: operaciones basadas en gráfica 4H

Salida:
- JSON con semáforo, sesgo, probabilidad estimada, contexto macro,
  condiciones de entrada, invalidación, stop técnico y targets.

Notas:
- No ejecuta órdenes.
- No usa datos externos; trabaja solo con el snapshot generado.
- La probabilidad es operativa/técnica, no estadística "real".
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional


DEFAULT_INPUT = "binance_snapshot.json"
DEFAULT_OUTPUT = "trade_analysis.json"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def last_close(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("price_summary", {}).get("close")


def high20(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("price_summary", {}).get("high_20")


def low20(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("price_summary", {}).get("low_20")


def ema20(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("ema", {}).get("ema20")


def ema50(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("ema", {}).get("ema50")


def rsi(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("rsi", {}).get("value")


def macd_state(interval_pack: Dict[str, Any]) -> Optional[str]:
    return interval_pack.get("macd", {}).get("state")


def volume_state(interval_pack: Dict[str, Any]) -> Optional[str]:
    return interval_pack.get("volume", {}).get("state")


def oi_slope(interval_pack: Dict[str, Any]) -> Optional[str]:
    return interval_pack.get("open_interest_histogram", {}).get("slope")


def oi_change(interval_pack: Dict[str, Any]) -> Optional[float]:
    return interval_pack.get("open_interest_histogram", {}).get("change_pct")


def round_or_none(v: Optional[float], digits: int = 8) -> Optional[float]:
    if v is None:
        return None
    return round(v, digits)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def infer_market_bias(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    btc = snapshot["assets"].get("BTCUSDT", {})
    btc_1h = btc.get("intervals", {}).get("1h", {})
    btc_4h = btc.get("intervals", {}).get("4h", {})

    bull_points = 0
    bear_points = 0
    notes = []

    for pack, label in [(btc_1h, "BTC 1H"), (btc_4h, "BTC 4H")]:
        p = last_close(pack)
        e20 = ema20(pack)
        e50 = ema50(pack)
        rs = rsi(pack)
        ms = macd_state(pack)

        if p is not None and e20 is not None and p > e20:
            bull_points += 1
            notes.append(f"{label}: precio sobre EMA20")
        elif p is not None and e20 is not None and p < e20:
            bear_points += 1
            notes.append(f"{label}: precio bajo EMA20")

        if p is not None and e50 is not None and p > e50:
            bull_points += 1
            notes.append(f"{label}: precio sobre EMA50")
        elif p is not None and e50 is not None and p < e50:
            bear_points += 1
            notes.append(f"{label}: precio bajo EMA50")

        if rs is not None:
            if rs > 55:
                bull_points += 1
                notes.append(f"{label}: RSI > 55")
            elif rs < 45:
                bear_points += 1
                notes.append(f"{label}: RSI < 45")

        if ms == "bullish":
            bull_points += 1
            notes.append(f"{label}: MACD bullish")
        elif ms == "bearish":
            bear_points += 1
            notes.append(f"{label}: MACD bearish")

    if bull_points > bear_points:
        state = "risk_on_relativo"
    elif bear_points > bull_points:
        state = "risk_off_relativo"
    else:
        state = "neutral"

    return {
        "state": state,
        "bull_points": bull_points,
        "bear_points": bear_points,
        "notes": notes,
    }


def score_direction(interval_pack: Dict[str, Any]) -> Dict[str, Any]:
    bull = 0
    bear = 0
    reasons = []

    p = last_close(interval_pack)
    e20 = ema20(interval_pack)
    e50 = ema50(interval_pack)
    rs = rsi(interval_pack)
    m_state = macd_state(interval_pack)
    vol = volume_state(interval_pack)
    oi_dir = oi_slope(interval_pack)
    oi_chg = oi_change(interval_pack)

    if p is not None and e20 is not None:
        if p > e20:
            bull += 2
            reasons.append("precio sobre EMA20")
        elif p < e20:
            bear += 2
            reasons.append("precio bajo EMA20")

    if p is not None and e50 is not None:
        if p > e50:
            bull += 2
            reasons.append("precio sobre EMA50")
        elif p < e50:
            bear += 2
            reasons.append("precio bajo EMA50")

    if e20 is not None and e50 is not None:
        if e20 > e50:
            bull += 2
            reasons.append("EMA20 > EMA50")
        elif e20 < e50:
            bear += 2
            reasons.append("EMA20 < EMA50")

    if rs is not None:
        if rs >= 55:
            bull += 1
            reasons.append("RSI favorable al alza")
        elif rs <= 45:
            bear += 1
            reasons.append("RSI favorable a la baja")

        if rs >= 75:
            bear += 1
            reasons.append("RSI en sobrecompra, riesgo de agotamiento")
        elif rs <= 25:
            bull += 1
            reasons.append("RSI en sobreventa, riesgo de rebote")

    if m_state == "bullish":
        bull += 2
        reasons.append("MACD bullish")
    elif m_state == "bearish":
        bear += 2
        reasons.append("MACD bearish")

    if vol == "alto":
        reasons.append("volumen alto")
    elif vol == "bajo":
        reasons.append("volumen bajo")

    if oi_dir == "alcista" and oi_chg is not None and oi_chg > 0:
        if m_state == "bullish":
            bull += 1
            reasons.append("OI creciente acompaña sesgo alcista")
        elif m_state == "bearish":
            bear += 1
            reasons.append("OI creciente acompaña sesgo bajista")

    bias = "neutral"
    if bull > bear:
        bias = "alcista"
    elif bear > bull:
        bias = "bajista"

    return {
        "bull_score": bull,
        "bear_score": bear,
        "bias": bias,
        "reasons": reasons,
    }


def derive_signal_strength(score: Dict[str, Any], market_bias: Dict[str, Any]) -> Dict[str, Any]:
    bull = score["bull_score"]
    bear = score["bear_score"]
    delta = bull - bear

    if delta >= 4:
        bias = "long"
        base_prob = 74
    elif delta >= 2:
        bias = "long"
        base_prob = 66
    elif delta <= -4:
        bias = "short"
        base_prob = 74
    elif delta <= -2:
        bias = "short"
        base_prob = 66
    else:
        bias = "no_trade"
        base_prob = 52

    mb = market_bias.get("state")
    if bias == "long":
        if mb == "risk_on_relativo":
            base_prob += 6
        elif mb == "risk_off_relativo":
            base_prob -= 7
    elif bias == "short":
        if mb == "risk_off_relativo":
            base_prob += 6
        elif mb == "risk_on_relativo":
            base_prob -= 7

    base_prob = int(clamp(base_prob, 35, 88))

    if base_prob >= 75:
        semaphore = "verde"
    elif base_prob >= 58:
        semaphore = "amarillo"
    else:
        semaphore = "rojo"

    return {
        "bias": bias,
        "probability_pct": base_prob,
        "semaphore": semaphore,
    }


def choose_levels_micro(intervals: Dict[str, Any], direction: str) -> Dict[str, Any]:
    p5 = intervals["5m"]
    p15 = intervals["15m"]
    p1h = intervals["1h"]

    current = last_close(p5)
    
    high_candidates = [high20(p5), high20(p15), high20(p1h)]
    high_vals = [x for x in high_candidates if x is not None]
    high_ref = max(high_vals) if high_vals else current

    low_candidates = [low20(p5), low20(p15), low20(p1h)]
    low_vals = [x for x in low_candidates if x is not None]
    low_ref = min(low_vals) if low_vals else current
    e20_5 = ema20(p5)
    e20_15 = ema20(p15)

    entry = None
    invalidation = None
    stop = None
    target_1 = None
    target_2 = None
    notes = []

    if direction == "long":
        v_entry = [v for v in [current, e20_5, e20_15] if v is not None]
        entry = max(v_entry) if v_entry else current
        
        invalidation_candidates = [low20(p5), low20(p15), e20_15]
        v_inv = [v for v in invalidation_candidates if v is not None]
        invalidation = min(v_inv) if v_inv else current
        
        stop = invalidation
        risk = entry - stop if entry is not None and stop is not None else None
        if risk is not None and risk > 0:
            target_1 = entry + (risk * 1.2)
            target_2 = min(high_ref, entry + (risk * 2.0)) if high_ref is not None else entry + (risk * 2.0)
        notes.append("Entrada ideal tras defensa/aceptación sobre EMA20 5m-15m")
        notes.append("Invalidación bajo estructura corta y/o pérdida de EMA20 15m")
    elif direction == "short":
        v_entry = [v for v in [current, e20_5, e20_15] if v is not None]
        entry = min(v_entry) if v_entry else current
        
        invalidation_candidates = [high20(p5), high20(p15), e20_15]
        v_inv = [v for v in invalidation_candidates if v is not None]
        invalidation = max(v_inv) if v_inv else current
        
        stop = invalidation
        risk = stop - entry if entry is not None and stop is not None else None
        if risk is not None and risk > 0:
            target_1 = entry - (risk * 1.2)
            target_2 = max(low_ref, entry - (risk * 2.0)) if low_ref is not None else entry - (risk * 2.0)
        notes.append("Entrada ideal en rechazo/resistencia o pérdida de soporte corto")
        notes.append("Invalidación sobre estructura corta y/o recuperación de EMA20 15m")
    else:
        notes.append("Sin ventaja clara; mejor no operar hasta nueva confirmación")

    return {
        "entry_ideal": round_or_none(entry, 8),
        "invalidation_level": round_or_none(invalidation, 8),
        "stop_loss_technical": round_or_none(stop, 8),
        "target_1": round_or_none(target_1, 8),
        "target_2": round_or_none(target_2, 8),
        "execution_notes": notes,
    }


def choose_levels_macro(intervals: Dict[str, Any], direction: str) -> Dict[str, Any]:
    p4h = intervals["4h"]
    p1d = intervals["1d"]

    current = last_close(p4h)

    high_candidates = [high20(p4h), high20(p1d)]
    high_vals = [x for x in high_candidates if x is not None]
    high_ref = max(high_vals) if high_vals else current

    low_candidates = [low20(p4h), low20(p1d)]
    low_vals = [x for x in low_candidates if x is not None]
    low_ref = min(low_vals) if low_vals else current
    e20_4 = ema20(p4h)
    e50_4 = ema50(p4h)
    e20_1d = ema20(p1d)

    entry = None
    invalidation = None
    stop = None
    target_1 = None
    target_2 = None
    notes = []

    if direction == "long":
        v_entry = [v for v in [current, e20_4, e50_4] if v is not None]
        entry = max(v_entry) if v_entry else current
        
        v_inv = [v for v in [low20(p4h), e50_4, e20_1d] if v is not None]
        invalidation = min(v_inv) if v_inv else current
        
        stop = invalidation
        risk = entry - stop if entry is not None and stop is not None else None
        if risk is not None and risk > 0:
            target_1 = entry + (risk * 1.5)
            target_2 = min(high_ref, entry + (risk * 3.0)) if high_ref is not None else entry + (risk * 3.0)
        notes.append("Entrada ideal tras confirmación de continuidad sobre estructura 4H")
        notes.append("Invalidación por pérdida de soporte 4H / EMA estructural")
    elif direction == "short":
        v_entry = [v for v in [current, e20_4, e50_4] if v is not None]
        entry = min(v_entry) if v_entry else current
        
        v_inv = [v for v in [high20(p4h), e50_4, e20_1d] if v is not None]
        invalidation = max(v_inv) if v_inv else current
        
        stop = invalidation
        risk = stop - entry if entry is not None and stop is not None else None
        if risk is not None and risk > 0:
            target_1 = entry - (risk * 1.5)
            target_2 = max(low_ref, entry - (risk * 3.0)) if low_ref is not None else entry - (risk * 3.0)
        notes.append("Entrada ideal en rechazo o continuación bajista bajo estructura 4H")
        notes.append("Invalidación por recuperación de resistencia/EMA estructural")
    else:
        notes.append("Sin ventaja estructural clara en 4H")

    return {
        "entry_ideal": round_or_none(entry, 8),
        "invalidation_level": round_or_none(invalidation, 8),
        "stop_loss_technical": round_or_none(stop, 8),
        "target_1": round_or_none(target_1, 8),
        "target_2": round_or_none(target_2, 8),
        "execution_notes": notes,
    }


def summarize_context(asset_name: str, asset_pack: Dict[str, Any], market_bias: Dict[str, Any]) -> Dict[str, Any]:
    prices = asset_pack.get("prices", {})
    funding = asset_pack.get("funding", {})

    basis = prices.get("basis_pct_perp_minus_spot")
    funding_rate = funding.get("last_funding_rate")

    context_notes = []
    if basis is not None:
        if basis > 0.15:
            context_notes.append("Perpetuo cotiza con prima relevante sobre spot")
        elif basis < -0.15:
            context_notes.append("Perpetuo cotiza con descuento frente a spot")
        else:
            context_notes.append("Base spot/perp relativamente neutra")

    if funding_rate is not None:
        if funding_rate > 0.0008:
            context_notes.append("Funding elevado al lado long, riesgo de squeeze/mean reversion")
        elif funding_rate < -0.0008:
            context_notes.append("Funding muy negativo, posible presión excesiva del lado short")
        else:
            context_notes.append("Funding en rango razonable")

    if market_bias["state"] == "risk_on_relativo":
        context_notes.append("BTC aporta contexto interno relativamente favorable")
    elif market_bias["state"] == "risk_off_relativo":
        context_notes.append("BTC aporta contexto interno relativamente adverso")
    else:
        context_notes.append("BTC aporta contexto interno mixto")

    return {
        "asset": asset_name,
        "spot_price": prices.get("spot_price"),
        "perp_last_price": prices.get("perp_last_price"),
        "mark_price": prices.get("mark_price"),
        "index_price": prices.get("index_price"),
        "basis_pct_perp_minus_spot": basis,
        "funding_rate": funding_rate,
        "notes": context_notes,
    }


def analyze_asset(asset_name: str, asset_pack: Dict[str, Any], market_bias: Dict[str, Any]) -> Dict[str, Any]:
    intervals = asset_pack["intervals"]

    micro_scores = [
        score_direction(intervals["3m"]),
        score_direction(intervals["5m"]),
        score_direction(intervals["15m"]),
        score_direction(intervals["1h"]),
    ]
    macro_scores = [
        score_direction(intervals["1h"]),
        score_direction(intervals["4h"]),
        score_direction(intervals["1d"]),
    ]

    micro_agg = {
        "bull_score": sum(s["bull_score"] for s in micro_scores),
        "bear_score": sum(s["bear_score"] for s in micro_scores),
        "bias": "alcista" if sum(s["bull_score"] for s in micro_scores) > sum(s["bear_score"] for s in micro_scores)
        else "bajista" if sum(s["bear_score"] for s in micro_scores) > sum(s["bull_score"] for s in micro_scores)
        else "neutral",
        "reasons": [r for s in micro_scores for r in s["reasons"]],
    }

    macro_agg = {
        "bull_score": sum(s["bull_score"] for s in macro_scores),
        "bear_score": sum(s["bear_score"] for s in macro_scores),
        "bias": "alcista" if sum(s["bull_score"] for s in macro_scores) > sum(s["bear_score"] for s in macro_scores)
        else "bajista" if sum(s["bear_score"] for s in macro_scores) > sum(s["bull_score"] for s in macro_scores)
        else "neutral",
        "reasons": [r for s in macro_scores for r in s["reasons"]],
    }

    micro_strength = derive_signal_strength(micro_agg, market_bias)
    macro_strength = derive_signal_strength(macro_agg, market_bias)

    micro_levels = choose_levels_micro(intervals, micro_strength["bias"])
    macro_levels = choose_levels_macro(intervals, macro_strength["bias"])

    context = summarize_context(asset_name, asset_pack, market_bias)

    return {
        "context": context,
        "micro": {
            "timeframes_used": ["3m", "5m", "15m", "1h"],
            "semaphore": micro_strength["semaphore"],
            "bias": micro_strength["bias"],
            "probability_pct": micro_strength["probability_pct"],
            "technical_reasoning": micro_agg["reasons"][:18],
            "macro_context_alignment": market_bias["state"],
            **micro_levels,
            "discipline_note": (
                "El stop loss es técnico. Si el PnL/riesgo es demasiado alto para el tamaño deseado, "
                "se reduce tamaño o no se entra."
            ),
        },
        "macro": {
            "timeframes_used": ["1h", "4h", "1d"],
            "semaphore": macro_strength["semaphore"],
            "bias": macro_strength["bias"],
            "probability_pct": macro_strength["probability_pct"],
            "technical_reasoning": macro_agg["reasons"][:18],
            "macro_context_alignment": market_bias["state"],
            **macro_levels,
            "discipline_note": (
                "El stop loss es técnico. Si el stop estructural correcto implica demasiada pérdida, "
                "se reduce tamaño o no se entra."
            ),
        },
    }


def build_analysis(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    market_bias = infer_market_bias(snapshot)

    result = {
        "meta": {
            "generator": "trade_analysis_from_snapshot.py",
            "source_snapshot_captured_at_utc": snapshot.get("meta", {}).get("captured_at_utc"),
            "methodology": [
                "MICRO se construye con 3m, 5m, 15m y 1h.",
                "MACRO se construye con 1h, 4h y 1d.",
                "El análisis utiliza EMA, RSI, MACD, volumen, OI y relación spot/perp.",
                "La probabilidad es operativa y depende de alineación técnica + contexto interno BTC.",
                "Este módulo no reemplaza DXY, US10Y ni noticias; esos datos deben integrarse aparte para la versión final del análisis."
            ],
        },
        "market_internal_bias": market_bias,
        "assets": {},
    }

    for asset_name, asset_pack in snapshot.get("assets", {}).items():
        result["assets"][asset_name] = analyze_asset(asset_name, asset_pack, market_bias)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convierte snapshot de Binance en análisis operativo JSON.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Ruta del snapshot JSON.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Ruta del archivo JSON de salida.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        snapshot = load_json(args.input)
        analysis = build_analysis(snapshot)
        save_json(args.output, analysis)
        print(f"Archivo generado correctamente: {args.output}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
