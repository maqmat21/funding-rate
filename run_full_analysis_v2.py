#!/usr/bin/env python3
"""
run_full_analysis_v2.py

Versión 2 del script maestro.
Ejecuta todo el pipeline y genera una salida textual extensa para análisis.
Incluye un bucle de actualización de 1 minuto, compatible con GitHub Actions (--once).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# Añadimos el directorio actual al path para que los imports estándar funcionen sin fallos
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    import binance_market_snapshot
    import trade_analysis_from_snapshot
    import final_trade_orchestrator
except ImportError as e:
    print(f"Error importando módulos locales: {e}")
    sys.exit(1)

DEFAULT_SYMBOLS = ["BTCUSDT", "SIRENUSDT"]
DEFAULT_INTERVALS = ["3m", "5m", "15m", "1h", "4h", "1d"]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_text(path: Path, text: str) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(text)


def fmt(v: Any) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.4f}"
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return str(v)


def block_line(title: str, value: Any) -> str:
    return f"{title}: {fmt(value)}"


def list_to_lines(items: List[str], prefix: str = "- ") -> List[str]:
    out = []
    for item in items:
        if item:
            out.append(f"{prefix}{item}")
    return out


def summarize_context(context: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(block_line("Spot price", context.get("spot_price")))
    lines.append(block_line("Perp last price", context.get("perp_last_price")))
    lines.append(block_line("Mark price", context.get("mark_price")))
    lines.append(block_line("Index price", context.get("index_price")))
    lines.append(block_line("Basis % perp-spot", context.get("basis_pct_perp_minus_spot")))
    lines.append(block_line("Funding rate", context.get("funding_rate")))
    notes = context.get("notes", [])
    if notes:
        lines.append("Notas de contexto:")
        lines.extend(list_to_lines(notes))
    return lines


def summarize_trade_block(label: str, block: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(f"{label}")
    lines.append(block_line("Semáforo", block.get("semaphore")))
    lines.append(block_line("Sesgo", block.get("bias")))
    lines.append(block_line("Probabilidad %", block.get("probability_pct")))
    
    timeframes = block.get("timeframes_used", [])
    if isinstance(timeframes, list):
        lines.append(block_line("Temporalidades", ", ".join(timeframes)))
        
    lines.append(block_line("Entrada ideal", block.get("entry_ideal")))
    lines.append(block_line("Nivel de invalidación", block.get("invalidation_level")))
    lines.append(block_line("Stop loss técnico", block.get("stop_loss_technical")))
    lines.append(block_line("Target 1", block.get("target_1")))
    lines.append(block_line("Target 2", block.get("target_2")))

    technical = block.get("technical_reasoning", [])
    if technical:
        lines.append("Razones técnicas:")
        lines.extend(list_to_lines(technical[:20]))

    macro_overlay = block.get("macro_overlay", [])
    if macro_overlay:
        lines.append("Overlay macro:")
        lines.extend(list_to_lines(macro_overlay))

    exec_notes = block.get("execution_notes", [])
    if exec_notes:
        lines.append("Notas de ejecución:")
        lines.extend(list_to_lines(exec_notes))

    discipline = block.get("discipline_note")
    if discipline:
        lines.append(f"Disciplina: {discipline}")

    return lines


def summarize_macro_context(macro: Dict[str, Any]) -> List[str]:
    lines = []
    lines.append(block_line("Timestamp", macro.get("timestamp")))
    lines.append(block_line("Macro bias", macro.get("macro_bias")))
    lines.append(block_line("Macro score", macro.get("macro_score")))

    dxy = macro.get("dxy", {})
    if dxy:
        lines.append("DXY:")
        lines.append(f"  - Price: {fmt(dxy.get('price'))}")
        lines.append(f"  - EMA20: {fmt(dxy.get('ema20'))}")
        lines.append(f"  - EMA50: {fmt(dxy.get('ema50'))}")
        lines.append(f"  - EMA trend: {fmt(dxy.get('ema_trend'))}")
        lines.append(f"  - Structure 1H: {fmt(dxy.get('structure_1h'))}")
        lines.append(f"  - Structure 4H: {fmt(dxy.get('structure_4h'))}")
        lines.append(f"  - Momentum slope: {fmt(dxy.get('momentum_slope'))}")
        lines.append(f"  - Momentum state: {fmt(dxy.get('momentum_state'))}")
        lines.append(f"  - Breakout 24H: {fmt(dxy.get('breakout_24h'))}")
        lines.append(f"  - State summary: {fmt(dxy.get('state_summary'))}")

    us10y = macro.get("us10y", {})
    if us10y:
        lines.append("US10Y:")
        lines.append(f"  - Price: {fmt(us10y.get('price'))}")
        lines.append(f"  - Direction 1H: {fmt(us10y.get('direction_1h'))}")
        lines.append(f"  - Momentum state: {fmt(us10y.get('momentum_state'))}")
        lines.append(f"  - EMA20: {fmt(us10y.get('ema20'))}")
        lines.append(f"  - EMA50: {fmt(us10y.get('ema50'))}")
        lines.append(f"  - Structure 4H: {fmt(us10y.get('structure_4h'))}")
        lines.append(f"  - State summary: {fmt(us10y.get('state_summary'))}")

    risk = macro.get("risk_environment", {})
    if risk:
        lines.append("Risk environment:")
        lines.append(f"  - BTC probability bias: {fmt(risk.get('btc_probability_bias'))}")
        lines.append(f"  - Altcoins probability bias: {fmt(risk.get('altcoins_probability_bias'))}")
        lines.append(f"  - Confidence level: {fmt(risk.get('confidence_level'))}")

    alerts = macro.get("alerts", {})
    if alerts:
        lines.append("Alerts:")
        lines.append(f"  - Macro shift: {fmt(alerts.get('macro_shift'))}")
        lines.append(f"  - Volatility expansion: {fmt(alerts.get('volatility_expansion'))}")

    return lines


def build_detailed_report(final_payload: Dict[str, Any]) -> str:
    lines: List[str] = []

    lines.append("REPORTE DETALLADO - FULL ANALYSIS V2")
    lines.append("=" * 72)
    lines.append(block_line("Generated at UTC", final_payload.get("meta", {}).get("generated_at_utc")))
    lines.append(block_line("Source snapshot captured at UTC", final_payload.get("meta", {}).get("source_snapshot_captured_at_utc")))
    lines.append("")

    lines.append("BLOQUE MACRO AUTOMÁTICO")
    lines.append("-" * 72)
    lines.extend(summarize_macro_context(final_payload.get("macro_context_auto", {})))
    lines.append("")

    event_titles = final_payload.get("events_considered_titles", [])
    lines.append("TÍTULOS DE EVENTOS MACRO CONSIDERADOS")
    lines.append("-" * 72)
    if event_titles:
        lines.extend(list_to_lines(event_titles))
    else:
        lines.append("- No se detectaron títulos de eventos en esta ejecución.")
    lines.append("")

    assets = final_payload.get("assets", {})
    for symbol, payload in assets.items():
        lines.append(f"ACTIVO: {symbol}")
        lines.append("-" * 72)

        context = payload.get("context", {})
        lines.append("CONTEXTO DEL ACTIVO")
        lines.extend(summarize_context(context))
        lines.append("")

        lines.extend(summarize_trade_block("MICRO", payload.get("final_micro", {})))
        lines.append("")
        lines.extend(summarize_trade_block("MACRO", payload.get("final_macro", {})))
        lines.append("")

        local_titles = payload.get("events_considered_titles", [])
        if local_titles:
            lines.append("EVENTOS CONSIDERADOS PARA ESTE ACTIVO")
            lines.extend(list_to_lines(local_titles))
            lines.append("")

    lines.append("NOTA OPERATIVA")
    lines.append("-" * 72)
    lines.append("Este reporte está diseñado para copy/paste y análisis posterior.")
    lines.append("Si se identifica un evento relevante no contemplado, debe analizarse fuera del script sobre la base de este resultado.")
    lines.append("El stop loss siempre debe leerse como técnico; si el riesgo PnL es excesivo, se reduce tamaño o no se entra.")
    lines.append("")

    return "\n".join(lines)


def run_pipeline(
    workdir: Path,
    symbols: List[str],
    intervals: List[str],
    keep_intermediate: bool,
) -> Dict[str, Path]:
    ensure_dir(workdir)

    snapshot_path = workdir / "binance_snapshot.json"
    analysis_path = workdir / "trade_analysis.json"
    final_path = workdir / "final_trade_plan.json"
    report_path = workdir / "final_trade_plan_report.txt"

    # 1. Snapshot
    snapshot_payload = binance_market_snapshot.create_payload(symbols, intervals)
    save_json(snapshot_path, snapshot_payload)

    # 2. Análisis
    analysis_payload = trade_analysis_from_snapshot.build_analysis(snapshot_payload)
    save_json(analysis_path, analysis_payload)

    # 3. Orquestador y Macro (CON PROTECCIÓN YAHOO FINANCE)
    session = final_trade_orchestrator.build_session()
    
    try:
        macro_payload = final_trade_orchestrator.get_macro_market_data(session)
    except Exception as e:
        print(f"\n[!] Aviso: Falló la obtención de Macro. Generando reporte sin DXY/US10Y. Error: {e}")
        macro_payload = {}
        
    event_titles = final_trade_orchestrator.get_macro_event_titles(session, limit_total=8)

    # 4. Plan Final
    final_payload = final_trade_orchestrator.build_final_plan(
        snapshot_payload,
        analysis_payload,
        macro_payload,
        event_titles,
    )
    save_json(final_path, final_payload)

    # 5. Generación de Reporte TXT
    report_text = build_detailed_report(final_payload)
    save_text(report_path, report_text)

    # Limpieza de archivos intermedios
    if not keep_intermediate:
        snapshot_path.unlink(missing_ok=True)
        analysis_path.unlink(missing_ok=True)

    return {
        "workdir": workdir,
        "snapshot_json": snapshot_path,
        "analysis_json": analysis_path,
        "final_json": final_path,
        "report_txt": report_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el análisis completo y genera salida JSON + reporte TXT detallado cada 1 minuto."
    )
    parser.add_argument(
        "--workdir",
        default="full_analysis_output_v2",
        help="Directorio de salida para los archivos generados.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help=f"Símbolos a consultar. Default: {' '.join(DEFAULT_SYMBOLS)}",
    )
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=DEFAULT_INTERVALS,
        help=f"Temporalidades a usar. Default: {' '.join(DEFAULT_INTERVALS)}",
    )
    parser.add_argument(
        "--keep-intermediate",
        action="store_true",
        help="Conserva binance_snapshot.json y trade_analysis.json además del final.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    workdir = Path(args.workdir).resolve()

    print("Iniciando Pipeline V2...")
    
    try:
        print(f"\n--- Ejecutando actualización: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        results = run_pipeline(
            workdir=workdir,
            symbols=args.symbols,
            intervals=args.intervals,
            keep_intermediate=args.keep_intermediate,
        )

        print(f"Directorio de salida: {results['workdir']}")
        if args.keep_intermediate:
            print(f"Snapshot: {results['snapshot_json']}")
            print(f"Análisis técnico: {results['analysis_json']}")
        print(f"Plan final JSON: {results['final_json']}")
        print(f"Reporte TXT actualizado: {results['report_txt']}")
        
    except Exception as exc:
        print(f"Error en run_full_analysis_v2.py: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())