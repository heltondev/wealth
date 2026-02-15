#!/usr/bin/env python3
"""
Standalone yfinance fetcher used by the Node.js market-data service.

Input: JSON on stdin with keys:
  - ticker: str
  - history_days: int

Output: JSON on stdout:
  - {"ok": true, "payload": {...}}
  - {"ok": false, "error": "...", "code": "..."}
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sys
from typing import Any


def _to_json_compatible(value: Any) -> Any:
    """Recursively convert pandas/numpy objects to plain JSON-safe values."""
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        # JSON.parse in Node does not accept NaN/Infinity literals.
        return value if math.isfinite(value) else None

    # numpy float/int subclasses should also be normalized before dumps.
    if isinstance(value, complex):
        return None

    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()

    # pandas / numpy scalars usually expose item()
    if hasattr(value, "item"):
        try:
            return _to_json_compatible(value.item())
        except Exception:
            pass

    if isinstance(value, dict):
        return {str(k): _to_json_compatible(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(v) for v in value]

    if hasattr(value, "tolist"):
        try:
            return _to_json_compatible(value.tolist())
        except Exception:
            pass

    return str(value)


def _df_to_payload(df: Any) -> dict[str, Any] | None:
    if df is None:
        return None
    try:
        if getattr(df, "empty", False):
            return {"columns": [], "index": [], "data": []}
        split = df.to_dict(orient="split")
        return _to_json_compatible(split)
    except Exception:
        return None


def _series_to_payload(series: Any) -> Any:
    if series is None:
        return None
    try:
        if getattr(series, "empty", False):
            return []
        if hasattr(series, "to_dict"):
            payload = []
            raw_dict = series.to_dict()
            for idx, value in raw_dict.items():
                payload.append(
                    {
                        "date": _to_json_compatible(idx),
                        "value": _to_json_compatible(value),
                    }
                )
            return payload
        return _to_json_compatible(series)
    except Exception:
        return None


def _load_input() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return payload
        return {}
    except Exception:
        return {}


def _emit(payload: dict[str, Any]) -> None:
    # Enforce strict JSON to avoid NaN/Infinity leaking to Node JSON.parse.
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, allow_nan=False))
    sys.stdout.flush()


def main() -> int:
    args = _load_input()
    ticker = str(args.get("ticker", "")).strip()
    history_days = int(args.get("history_days", 30) or 30)

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

        # Keep this payload broad and unfiltered to preserve newly introduced fields.
        info = _to_json_compatible(getattr(asset, "info", {}) or {})
        fast_info = _to_json_compatible(getattr(asset, "fast_info", {}) or {})
        history_df = asset.history(period=f"{max(history_days, 1)}d", auto_adjust=False)
        dividends = _series_to_payload(getattr(asset, "dividends", None))

        payload = {
            "ticker": ticker,
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "info": info,
            "fast_info": fast_info,
            "history": _df_to_payload(history_df),
            "dividends": dividends,
            "financials": _df_to_payload(getattr(asset, "financials", None)),
            "quarterly_financials": _df_to_payload(
                getattr(asset, "quarterly_financials", None)
            ),
            "balance_sheet": _df_to_payload(getattr(asset, "balance_sheet", None)),
            "quarterly_balance_sheet": _df_to_payload(
                getattr(asset, "quarterly_balance_sheet", None)
            ),
            "cashflow": _df_to_payload(getattr(asset, "cashflow", None)),
            "quarterly_cashflow": _df_to_payload(
                getattr(asset, "quarterly_cashflow", None)
            ),
            "recommendations": _df_to_payload(getattr(asset, "recommendations", None)),
            "institutional_holders": _df_to_payload(
                getattr(asset, "institutional_holders", None)
            ),
            "major_holders": _df_to_payload(getattr(asset, "major_holders", None)),
            "calendar": _to_json_compatible(getattr(asset, "calendar", None)),
        }

        _emit({"ok": True, "source": "yfinance", "payload": payload})
        return 0
    except Exception as error:
        _emit(
            {
                "ok": False,
                "code": "runtime_error",
                "error": str(error),
            }
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
