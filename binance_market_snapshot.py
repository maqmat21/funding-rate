#!/usr/bin/env python3
"""
binance_market_snapshot.py

Genera un archivo JSON con toda la información necesaria para sustituir las
gráficas de BTC y SIREN dentro del flujo de análisis operativo.

Incluye, por símbolo:
- Fecha y hora de corte
- Precio spot
- Precio perp
- Mark price
- Index price
- Funding rate actual
- Próximo funding time
- Base spot vs perp
- Klines de futures por temporalidad
- Indicadores por temporalidad:
    - EMA
    - RSI
    - MACD
    - Volumen
    - Open interest actual
    - Open interest histórico (histograma)
- Resumen de estructura útil para análisis

Fuentes:
- Binance Spot REST API
- Binance USDⓈ-M Futures REST API

Uso:
    python binance_market_snapshot.py

Opcional:
    python binance_market_snapshot.py --symbols BTCUSDT SIRENUSDT --output snapshot.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


SPOT_BASE = "https://data-api.binance.vision"
FUTURES_BASE = "https://fapi.binance.com"

DEFAULT_SYMBOLS = ["BTCUSDT", "SIRENUSDT"]
DEFAULT_INTERVALS = ["3m", "5m", "15m", "1h", "4h", "1d"]

EMA_FAST = 12
EMA_SLOW = 26
EMA_SIGNAL = 9
RSI_PERIOD = 14

KLINE_LIMIT = 250
OI_HIST_LIMIT = 30
HTTP_TIMEOUT = 20


class BinanceAPIError(RuntimeError):
    pass


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return ((current - previous) / previous) * 100.0


def round_or_none(value: Optional[float], digits: int = 8) -> Optional[float]:
    if value is None:
        return None
    return round(value, digits)


@dataclass
class HttpClient:
    session: requests.Session

    def get_json(self, base_url: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{base_url}{path}"
        response = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if response.status_code != 200:
            raise BinanceAPIError(
                f"Error {response.status_code} consultando {url} con params={params}: {response.text}"
            )
        return response.json()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; BinanceMarketSnapshot/1.0)",
            "Accept": "application/json",
        }
    )
    return session


def get_server_times(client: HttpClient) -> Dict[str, Any]:
    spot = client.get_json(SPOT_BASE, "/api/v3/time")
    futures = client.get_json(FUTURES_BASE, "/fapi/v1/time")
    return {
        "captured_at_utc": now_utc_iso(),
        "spot_server_time_ms": spot.get("serverTime"),
        "spot_server_time_utc": ms_to_iso(spot.get("serverTime")),
        "futures_server_time_ms": futures.get("serverTime"),
        "futures_server_time_utc": ms_to_iso(futures.get("serverTime")),
    }


def get_exchange_info(client: HttpClient, market: str) -> Dict[str, Any]:
    if market == "spot":
        return client.get_json(SPOT_BASE, "/api/v3/exchangeInfo")
    if market == "futures":
        return client.get_json(FUTURES_BASE, "/fapi/v1/exchangeInfo")
    raise ValueError("market debe ser 'spot' o 'futures'")


def symbol_exists(exchange_info: Dict[str, Any], symbol: str) -> bool:
    return any(item.get("symbol") == symbol for item in exchange_info.get("symbols", []))


def get_spot_price(client: HttpClient, symbol: str) -> Optional[float]:
    try:
        data = client.get_json(SPOT_BASE, "/api/v3/ticker/price", {"symbol": symbol})
        return safe_float(data.get("price"))
    except BinanceAPIError:
        return None


def get_futures_last_price(client: HttpClient, symbol: str) -> Optional[float]:
    data = client.get_json(FUTURES_BASE, "/fapi/v2/ticker/price", {"symbol": symbol})
    return safe_float(data.get("price"))


def get_premium_index(client: HttpClient, symbol: str) -> Dict[str, Any]:
    data = client.get_json(FUTURES_BASE, "/fapi/v1/premiumIndex", {"symbol": symbol})
    return {
        "symbol": data.get("symbol"),
        "mark_price": safe_float(data.get("markPrice")),
        "index_price": safe_float(data.get("indexPrice")),
        "estimated_settle_price": safe_float(data.get("estimatedSettlePrice")),
        "last_funding_rate": safe_float(data.get("lastFundingRate")),
        "interest_rate": safe_float(data.get("interestRate")),
        "next_funding_time_ms": data.get("nextFundingTime"),
        "next_funding_time_utc": ms_to_iso(data.get("nextFundingTime")),
        "time_ms": data.get("time"),
        "time_utc": ms_to_iso(data.get("time")),
    }


def get_latest_funding_history(client: HttpClient, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
    rows = client.get_json(FUTURES_BASE, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit})
    result = []
    for row in rows:
        result.append(
            {
                "symbol": row.get("symbol"),
                "funding_rate": safe_float(row.get("fundingRate")),
                "funding_time_ms": row.get("fundingTime"),
                "funding_time_utc": ms_to_iso(row.get("fundingTime")),
                "mark_price": safe_float(row.get("markPrice")),
            }
        )
    return result


def get_open_interest_current(client: HttpClient, symbol: str) -> Dict[str, Any]:
    data = client.get_json(FUTURES_BASE, "/fapi/v1/openInterest", {"symbol": symbol})
    return {
        "symbol": data.get("symbol"),
        "open_interest": safe_float(data.get("openInterest")),
        "time_ms": data.get("time"),
        "time_utc": ms_to_iso(data.get("time")),
    }


def get_open_interest_hist(client: HttpClient, symbol: str, period: str, limit: int = OI_HIST_LIMIT) -> List[Dict[str, Any]]:
    rows = client.get_json(
        FUTURES_BASE,
        "/futures/data/openInterestHist",
        {"symbol": symbol, "period": period, "limit": limit},
    )
    result = []
    for row in rows:
        result.append(
            {
                "symbol": row.get("symbol"),
                "sum_open_interest": safe_float(row.get("sumOpenInterest")),
                "sum_open_interest_value": safe_float(row.get("sumOpenInterestValue")),
                "timestamp_ms": row.get("timestamp"),
                "timestamp_utc": ms_to_iso(row.get("timestamp")),
            }
        )
    return result


def get_futures_klines(client: HttpClient, symbol: str, interval: str, limit: int = KLINE_LIMIT) -> List[Dict[str, Any]]:
    rows = client.get_json(
        FUTURES_BASE,
        "/fapi/v1/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    result = []
    for row in rows:
        result.append(
            {
                "open_time_ms": row[0],
                "open_time_utc": ms_to_iso(row[0]),
                "open": safe_float(row[1]),
                "high": safe_float(row[2]),
                "low": safe_float(row[3]),
                "close": safe_float(row[4]),
                "volume": safe_float(row[5]),
                "close_time_ms": row[6],
                "close_time_utc": ms_to_iso(row[6]),
                "quote_asset_volume": safe_float(row[7]),
                "number_of_trades": int(row[8]),
                "taker_buy_base_asset_volume": safe_float(row[9]),
                "taker_buy_quote_asset_volume": safe_float(row[10]),
            }
        )
    return result


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    result: List[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result
    sma = sum(values[:period]) / period
    result[period - 1] = sma
    prev_ema = sma
    for i in range(period, len(values)):
        prev_ema = ((values[i] - prev_ema) * multiplier) + prev_ema
        result[i] = prev_ema
    return result


def rsi(values: List[float], period: int = RSI_PERIOD) -> List[Optional[float]]:
    result: List[Optional[float]] = [None] * len(values)
    if len(values) <= period:
        return result

    gains = []
    losses = []
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        gain = max(delta, 0.0)
        loss = abs(min(delta, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100.0 - (100.0 / (1.0 + rs))

    return result


def macd(values: List[float], fast: int = EMA_FAST, slow: int = EMA_SLOW, signal: int = EMA_SIGNAL) -> Dict[str, List[Optional[float]]]:
    ema_fast = ema(values, fast)
    ema_slow = ema(values, slow)

    macd_line: List[Optional[float]] = [None] * len(values)
    macd_values_for_signal: List[float] = []
    macd_indexes: List[int] = []

    for i, (fast_v, slow_v) in enumerate(zip(ema_fast, ema_slow)):
        if fast_v is not None and slow_v is not None:
            value = fast_v - slow_v
            macd_line[i] = value
            macd_values_for_signal.append(value)
            macd_indexes.append(i)

    signal_line_partial = ema(macd_values_for_signal, signal)
    signal_line: List[Optional[float]] = [None] * len(values)

    for idx, original_index in enumerate(macd_indexes):
        signal_line[original_index] = signal_line_partial[idx]

    histogram: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if macd_line[i] is not None and signal_line[i] is not None:
            histogram[i] = macd_line[i] - signal_line[i]

    return {
        "macd_line": macd_line,
        "signal_line": signal_line,
        "histogram": histogram,
    }


def last_non_none(values: List[Optional[float]]) -> Optional[float]:
    for value in reversed(values):
        if value is not None:
            return value
    return None


def infer_slope(current: Optional[float], previous: Optional[float]) -> Optional[str]:
    if current is None or previous is None:
        return None
    if current > previous:
        return "alcista"
    if current < previous:
        return "bajista"
    return "plana"


def volume_summary(volumes: List[float]) -> Dict[str, Any]:
    if not volumes:
        return {"state": None, "current": None, "avg_20": None, "ratio_to_avg20": None}
    current = volumes[-1]
    recent = volumes[-21:-1] if len(volumes) >= 21 else volumes[:-1]
    avg_20 = sum(recent) / len(recent) if recent else None
    ratio = (current / avg_20) if avg_20 not in (None, 0) else None

    state = None
    if ratio is not None:
        if ratio >= 1.5:
            state = "alto"
        elif ratio >= 0.9:
            state = "normal"
        else:
            state = "bajo"

    return {
        "state": state,
        "current": round_or_none(current, 8),
        "avg_20": round_or_none(avg_20, 8) if avg_20 is not None else None,
        "ratio_to_avg20": round_or_none(ratio, 4) if ratio is not None else None,
    }


def compute_interval_package(klines: List[Dict[str, Any]], oi_hist: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [row["close"] for row in klines if row["close"] is not None]
    highs = [row["high"] for row in klines if row["high"] is not None]
    lows = [row["low"] for row in klines if row["low"] is not None]
    volumes = [row["volume"] for row in klines if row["volume"] is not None]

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, RSI_PERIOD)
    macd_pack = macd(closes)

    current_close = closes[-1] if closes else None
    prev_close = closes[-2] if len(closes) >= 2 else None

    last_ema20 = last_non_none(ema20)
    prev_ema20 = last_non_none(ema20[:-1]) if len(ema20) > 1 else None
    last_ema50 = last_non_none(ema50)
    prev_ema50 = last_non_none(ema50[:-1]) if len(ema50) > 1 else None
    last_rsi = last_non_none(rsi14)
    last_macd = last_non_none(macd_pack["macd_line"])
    last_macd_signal = last_non_none(macd_pack["signal_line"])
    last_macd_hist = last_non_none(macd_pack["histogram"])

    high_20 = max(highs[-20:]) if len(highs) >= 20 else (max(highs) if highs else None)
    low_20 = min(lows[-20:]) if len(lows) >= 20 else (min(lows) if lows else None)

    oi_values = [row["sum_open_interest"] for row in oi_hist if row["sum_open_interest"] is not None]
    oi_current = oi_values[-1] if oi_values else None
    oi_prev = oi_values[-2] if len(oi_values) >= 2 else None
    oi_slope = infer_slope(oi_current, oi_prev)
    oi_change_pct = pct_change(oi_current, oi_prev)

    return {
        "last_candle": klines[-1] if klines else None,
        "price_summary": {
            "close": round_or_none(current_close, 8),
            "previous_close": round_or_none(prev_close, 8),
            "close_change_pct": round_or_none(pct_change(current_close, prev_close), 4),
            "high_20": round_or_none(high_20, 8),
            "low_20": round_or_none(low_20, 8),
        },
        "ema": {
            "ema20": round_or_none(last_ema20, 8),
            "ema50": round_or_none(last_ema50, 8),
            "ema20_slope": infer_slope(last_ema20, prev_ema20),
            "ema50_slope": infer_slope(last_ema50, prev_ema50),
            "price_vs_ema20": (
                "above" if current_close is not None and last_ema20 is not None and current_close > last_ema20
                else "below" if current_close is not None and last_ema20 is not None and current_close < last_ema20
                else None
            ),
            "price_vs_ema50": (
                "above" if current_close is not None and last_ema50 is not None and current_close > last_ema50
                else "below" if current_close is not None and last_ema50 is not None and current_close < last_ema50
                else None
            ),
        },
        "rsi": {
            "period": RSI_PERIOD,
            "value": round_or_none(last_rsi, 4),
            "state": (
                "overbought" if last_rsi is not None and last_rsi >= 70
                else "oversold" if last_rsi is not None and last_rsi <= 30
                else "neutral" if last_rsi is not None
                else None
            ),
        },
        "macd": {
            "fast": EMA_FAST,
            "slow": EMA_SLOW,
            "signal": EMA_SIGNAL,
            "macd_line": round_or_none(last_macd, 8),
            "signal_line": round_or_none(last_macd_signal, 8),
            "histogram": round_or_none(last_macd_hist, 8),
            "state": (
                "bullish" if last_macd is not None and last_macd_signal is not None and last_macd > last_macd_signal
                else "bearish" if last_macd is not None and last_macd_signal is not None and last_macd < last_macd_signal
                else None
            ),
        },
        "volume": volume_summary(volumes),
        "open_interest_histogram": {
            "current": round_or_none(oi_current, 8),
            "previous": round_or_none(oi_prev, 8),
            "change_pct": round_or_none(oi_change_pct, 4),
            "slope": oi_slope,
            "hist": oi_hist,
        },
        "raw_klines": klines,
    }


def build_symbol_snapshot(
    client: HttpClient,
    symbol: str,
    spot_info: Dict[str, Any],
    futures_info: Dict[str, Any],
    intervals: List[str],
) -> Dict[str, Any]:
    futures_exists = symbol_exists(futures_info, symbol)
    if not futures_exists:
        raise BinanceAPIError(f"El símbolo {symbol} no existe en USDⓈ-M Futures de Binance.")

    spot_exists = symbol_exists(spot_info, symbol)

    spot_price = get_spot_price(client, symbol) if spot_exists else None
    perp_price = get_futures_last_price(client, symbol)
    premium = get_premium_index(client, symbol)
    funding_history = get_latest_funding_history(client, symbol, limit=10)
    oi_current = get_open_interest_current(client, symbol)

    basis_abs = None
    basis_pct = None
    if spot_price not in (None, 0) and perp_price is not None:
        basis_abs = perp_price - spot_price
        basis_pct = (basis_abs / spot_price) * 100.0

    interval_data: Dict[str, Any] = {}
    for interval in intervals:
        klines = get_futures_klines(client, symbol, interval, limit=KLINE_LIMIT)
        oi_hist_period = interval if interval in {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"} else ("5m" if interval == "3m" else interval)
        oi_hist = get_open_interest_hist(client, symbol, oi_hist_period)
        interval_data[interval] = compute_interval_package(klines, oi_hist)

    return {
        "symbol": symbol,
        "market_availability": {
            "spot_exists": spot_exists,
            "futures_exists": futures_exists,
        },
        "cutoff": {
            "captured_at_utc": now_utc_iso(),
        },
        "prices": {
            "spot_price": round_or_none(spot_price, 8),
            "perp_last_price": round_or_none(perp_price, 8),
            "mark_price": round_or_none(premium.get("mark_price"), 8),
            "index_price": round_or_none(premium.get("index_price"), 8),
            "estimated_settle_price": round_or_none(premium.get("estimated_settle_price"), 8),
            "basis_abs_perp_minus_spot": round_or_none(basis_abs, 8),
            "basis_pct_perp_minus_spot": round_or_none(basis_pct, 6),
        },
        "funding": {
            "last_funding_rate": round_or_none(premium.get("last_funding_rate"), 8),
            "interest_rate": round_or_none(premium.get("interest_rate"), 8),
            "next_funding_time_ms": premium.get("next_funding_time_ms"),
            "next_funding_time_utc": premium.get("next_funding_time_utc"),
            "history": funding_history,
        },
        "open_interest_current": oi_current,
        "intervals": interval_data,
    }


def create_payload(symbols: List[str], intervals: List[str]) -> Dict[str, Any]:
    session = build_session()
    client = HttpClient(session=session)

    spot_info = get_exchange_info(client, "spot")
    futures_info = get_exchange_info(client, "futures")

    payload: Dict[str, Any] = {
        "meta": {
            "generator": "binance_market_snapshot.py",
            "version": "1.0.0",
            "captured_at_utc": now_utc_iso(),
            "symbols_requested": symbols,
            "intervals_requested": intervals,
            "notes": [
                "Los indicadores se calculan sobre klines de USDⓈ-M Futures para alinear la lectura con el contrato perpetuo.",
                "El precio spot se consulta en Binance Spot cuando el símbolo existe en ese mercado.",
                "Para open interest histogram se usa openInterestHist. Binance soporta periodos 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h y 1d.",
                "Si una temporalidad no existe para OI histórico (por ejemplo 3m), se aproxima usando 5m.",
            ],
        },
        "server_times": get_server_times(client),
        "assets": {},
    }

    for symbol in symbols:
        payload["assets"][symbol] = build_symbol_snapshot(client, symbol, spot_info, futures_info, intervals)

    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera snapshot JSON de BTC y SIREN usando Binance API.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help=f"Símbolos futures/spot a consultar. Default: {' '.join(DEFAULT_SYMBOLS)}",
    )
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=DEFAULT_INTERVALS,
        help=f"Temporalidades. Default: {' '.join(DEFAULT_INTERVALS)}",
    )
    parser.add_argument(
        "--output",
        default="binance_snapshot.json",
        help="Ruta del archivo JSON de salida.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = create_payload(args.symbols, args.intervals)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Archivo generado correctamente: {args.output}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())