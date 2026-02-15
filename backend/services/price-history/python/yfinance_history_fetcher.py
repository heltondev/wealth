#!/usr/bin/env python3
"""
Daily price history fetcher for yfinance.

Input JSON (stdin):
  - ticker: str
  - start_date: optional YYYY-MM-DD
  - period: optional yfinance period string (e.g. max, 5y)
  - interval: optional (default 1d)

Output JSON (stdout):
  {"ok": true, "payload": {...}} or {"ok": false, "error": "...", "code": "..."}
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from typing import Any


def _emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    sys.stdout.flush()


def _read_input() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return value
        return {}
    except Exception:
        return {}


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
        if number != number:  # NaN
            return None
        return number
    except Exception:
        return None


def _index_to_utc_date(value: Any) -> str | None:
    # pandas Timestamp can be converted through to_pydatetime()
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
    except Exception:
        pass

    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        else:
            value = value.astimezone(dt.timezone.utc)
        return value.date().isoformat()

    if isinstance(value, dt.date):
        return value.isoformat()

    parsed = str(value).strip()
    if not parsed:
        return None
    # Keep only date component when incoming is full datetime string.
    return parsed[:10]


def main() -> int:
    args = _read_input()
    ticker = str(args.get("ticker", "")).strip()
    start_date = str(args.get("start_date", "")).strip() or None
    period = str(args.get("period", "")).strip() or None
    interval = str(args.get("interval", "1d")).strip() or "1d"

    if not ticker:
        _emit({"ok": False, "code": "invalid_input", "error": "ticker is required"})
        return 0

    try:
        import yfinance as yf  # type: ignore
    except Exception:
        _emit(
            {
                "ok": False,
                "code": "dependency_missing",
                "error": "python package 'yfinance' is not installed",
            }
        )
        return 0

    try:
        asset = yf.Ticker(ticker)
        history_kwargs: dict[str, Any] = {
            "interval": interval,
            "auto_adjust": False,
            "actions": True,
        }
        if start_date:
            history_kwargs["start"] = start_date
        elif period:
            history_kwargs["period"] = period
        else:
            history_kwargs["period"] = "max"

        history = asset.history(**history_kwargs)

        rows: list[dict[str, Any]] = []
        if history is not None and not history.empty:
            for index, row in history.iterrows():
                date = _index_to_utc_date(index)
                if not date:
                    continue

                rows.append(
                    {
                        "date": date,
                        "open": _to_number(row.get("Open")),
                        "high": _to_number(row.get("High")),
                        "low": _to_number(row.get("Low")),
                        "close": _to_number(row.get("Close")),
                        "adjusted_close": _to_number(row.get("Adj Close")),
                        "volume": _to_number(row.get("Volume")),
                        "dividends": _to_number(row.get("Dividends")) or 0.0,
                        "stock_splits": _to_number(row.get("Stock Splits")) or 0.0,
                    }
                )

        info = getattr(asset, "info", {}) or {}
        currency = info.get("currency")

        _emit(
            {
                "ok": True,
                "payload": {
                    "ticker": ticker,
                    "currency": currency,
                    "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "rows": rows,
                },
            }
        )
        return 0
    except Exception as error:
        _emit({"ok": False, "code": "runtime_error", "error": str(error)})
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
