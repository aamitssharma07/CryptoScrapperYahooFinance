#!/usr/bin/env python3
"""
Fetch Yahoo Finance historical data for tickers from a file (TXT/CSV/TSV/JSON/JSONL).

✅ Handles numbered/bulleted TXT lines like:
   1. BTC-USD
   2) ETH-USD
   $3: USDT-USD
   - BNB-USD
   • SOL-USD
   and also "AAPL - Apple Inc." (takes the left token).

Features
--------
- Reads tickers from TXT (flexible), CSV/TSV (--col optional), JSON (list or list of objects), JSONL.
- Date range via --start / --end (YYYY-MM-DD). Default: start=2000-01-01, end=today.
- Interval (1d default). Use 1wk/1mo for faster downloads, intraday may be restricted by Yahoo.
- Writes per-ticker CSV/JSON and an optional merged JSON.
- Retries with exponential backoff; polite sleep between requests.
- --dry-run to preview parsed tickers before downloading.

Install
-------
python3 -m venv .venv && source .venv/bin/activate
pip install yfinance pandas
"""
import argparse
import json
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd


# ---------------- yfinance import ----------------
def _import_yfinance():
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        print(
            "Error: yfinance is not installed.\n"
            "Use a virtual env, then:\n"
            "  python3 -m venv .venv && source .venv/bin/activate\n"
            "  pip install yfinance pandas",
            file=sys.stderr,
        )
        raise
    return yf


# ---------------- CLI ----------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch Yahoo Finance history for tickers from flexible file formats."
    )
    p.add_argument("--tickers-file", required=True, help="Input file (txt/csv/tsv/json/jsonl).")
    p.add_argument("--col", help="For CSV/TSV: column name OR 0-based index containing tickers.")
    p.add_argument("--outdir", default="Yahoo_Crypto_Data", help="Base output directory (default: Yahoo_Crypto_Data).")
    p.add_argument("--start", default="2000-01-01", help="Start date YYYY-MM-DD (default 2000-01-01).")
    p.add_argument("--end", default=None, help="End date YYYY-MM-DD (default today).")
    p.add_argument(
        "--interval",
        default="1d",
        choices=["1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"],
        help="Download interval (default 1d)."
    )
    p.add_argument("--auto-adjust", action="store_true", help="Auto-adjust prices (dividends/splits).")
    p.add_argument("--csv", action="store_true", help="Write per-ticker CSV files.")
    p.add_argument("--json", action="store_true", help="Write per-ticker JSON files.")
    p.add_argument("--merged-json", action="store_true", help="Write merged JSON with all tickers.")
    p.add_argument("--retries", type=int, default=3, help="Retry attempts on failure (default 3).")
    p.add_argument("--sleep", type=float, default=0.5, help="Base sleep seconds between requests (default 0.5).")
    p.add_argument("--dry-run", action="store_true", help="Only parse and print tickers, then exit.")
    p.add_argument("--max-print", type=int, default=50, help="When --dry-run, limit printed tickers (default 50).")
    return p.parse_args()


# ---------------- Validation & paths ----------------
def valid_dates(start_s: str, end_s: Optional[str]) -> Tuple[str, str]:
    def _parse(d: str) -> str:
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date '{d}'. Use YYYY-MM-DD.") from e
        return d
    start = _parse(start_s)
    end = _parse(end_s or date.today().strftime("%Y-%m-%d"))
    if start > end:
        raise ValueError(f"--start must be <= --end (got start={start}, end={end})")
    return start, end


def ensure_dirs(base: Path) -> dict:
    csv_dir = base / "CSV"
    json_dir = base / "JSONs"
    merged_dir = base / "JSONs" / "merged"
    for d in (csv_dir, json_dir, merged_dir):
        d.mkdir(parents=True, exist_ok=True)
    return {"base": base, "csv": csv_dir, "json": json_dir, "merged": merged_dir}


# ---------------- Input parsing ----------------
# Numbered prefixes like "1.", "2)", "$3:", "4 -"
_NUM_PREFIX = re.compile(r"^\s*\$?\d+([.)\:])?\s*(-\s*)?")
# Bullets -, * or •
_BULLET_PREFIX = re.compile(r"^\s*[-*\u2022]\s*")
# Accept Yahoo symbols: starts with letter/^, then letters/digits or . - =
_TICKER_RE = re.compile(r"[A-Za-z\^][A-Za-z0-9.\-=]*")

COMMON_COLS = [
    "ticker", "tickers", "symbol", "symbols",
    "Ticker", "Tickers", "Symbol", "Symbols",
    "Ticker Symbol", "SYMBOL", "TICKER"
]

def _normalize_symbol(sym: str) -> str:
    return sym.strip().upper()

def _dedupe(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def load_tickers_generic(path: Path, col_hint: Optional[str]) -> List[str]:
    suffix = path.suffix.lower().lstrip(".")
    if suffix in {"csv", "tsv"}:
        sep = "," if suffix == "csv" else "\t"
        return load_tickers_from_table(path, sep, col_hint)
    if suffix in {"json"}:
        return load_tickers_from_json(path)
    if suffix in {"jsonl"}:
        return load_tickers_from_jsonl(path)
    # default to text
    return load_tickers_from_text(path)

def load_tickers_from_table(path: Path, sep: str, col_hint: Optional[str]) -> List[str]:
    df = pd.read_csv(path, sep=sep)
    if df.empty:
        return []
    if col_hint is not None:
        try:
            idx = int(col_hint)
            series = df.iloc[:, idx]
        except ValueError:
            if col_hint not in df.columns:
                raise ValueError(f"--col '{col_hint}' not found. Available: {list(df.columns)}")
            series = df[col_hint]
    else:
        col_name = next((c for c in df.columns if c in COMMON_COLS), None)
        series = df[col_name] if col_name else df.iloc[:, 0]
    syms = [_normalize_symbol(str(x)) for x in series.astype(str).tolist() if str(x).strip()]
    return _dedupe(syms)

def load_tickers_from_json(path: Path) -> List[str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        if all(isinstance(x, str) for x in data):
            return _dedupe([_normalize_symbol(x) for x in data])
        if all(isinstance(x, dict) for x in data):
            return _dedupe(_extract_from_dicts(data))
    raise ValueError("Unsupported JSON structure. Expect a list of strings or list of objects.")

def load_tickers_from_jsonl(path: Path) -> List[str]:
    syms: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, str):
            syms.append(_normalize_symbol(obj))
        elif isinstance(obj, dict):
            syms.extend(_extract_from_dicts([obj]))
    return _dedupe(syms)

def _extract_from_dicts(objs: Iterable[dict]) -> List[str]:
    keys = COMMON_COLS + ["TickerSymbol", "RIC", "Yahoo", "YahooSymbol"]
    out: List[str] = []
    for o in objs:
        for k in keys:
            if k in o and isinstance(o[k], str) and o[k].strip():
                out.append(_normalize_symbol(o[k]))
                break
    return out

def load_tickers_from_text(path: Path) -> List[str]:
    syms: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # Remove numbering/bullets
        line = _NUM_PREFIX.sub("", line)
        line = _BULLET_PREFIX.sub("", line)

        # Support "AAPL - Apple Inc." by taking left side only
        if " - " in line:
            left = line.split(" - ", 1)[0].strip()
            if left:
                line = left

        # If separators found, split and collect
        for sep in [",", "|", ";", "\t"]:
            if sep in line:
                parts = [p.strip() for p in line.split(sep) if p.strip()]
                syms.extend(_normalize_symbol(p) for p in parts if _TICKER_RE.fullmatch(p))
                break
        else:
            # Otherwise pick the first token that matches ticker pattern
            for tok in line.split():
                if _TICKER_RE.fullmatch(tok):
                    syms.append(_normalize_symbol(tok))
                    break

    return _dedupe(syms)


# ---------------- Download helpers ----------------
def df_to_json_records(df: pd.DataFrame) -> List[dict]:
    out = df.reset_index().rename(columns={"index": "Date"})
    if "Date" in out.columns:
        out["Date"] = out["Date"].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x))
    return json.loads(out.to_json(orient="records", date_format="iso"))

def fetch_history(yf, ticker: str, start: str, end: str, interval: str,
                  auto_adjust: bool, retries: int, base_sleep: float) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            df = yf.Ticker(ticker).history(
                start=start, end=end, interval=interval, auto_adjust=auto_adjust
            )
            if not df.empty:
                df.columns = [c.title().replace(" ", "") for c in df.columns]
            return df
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (2 ** (attempt - 1)))
    assert last_err is not None
    raise last_err


# ---------------- Main ----------------
def main() -> int:
    args = parse_args()
    yf = _import_yfinance()

    tickers_path = Path(args.tickers_file)
    if not tickers_path.exists():
        print(f"Tickers file not found: {tickers_path}", file=sys.stderr)
        return 2

    try:
        tickers = load_tickers_generic(tickers_path, args.col)
    except Exception as e:
        print(f"Error while reading tickers: {e}", file=sys.stderr)
        return 2

    if not tickers:
        print("No tickers found. Nothing to do.", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"Parsed {len(tickers)} tickers from {tickers_path.name}:")
        for i, sym in enumerate(tickers[: args.max_print], 1):
            print(f"  {i}. {sym}")
        if len(tickers) > args.max_print:
            print(f"... and {len(tickers) - args.max_print} more")
        return 0

    try:
        start, end = valid_dates(args.start, args.end)
    except Exception as e:
        print(f"Date error: {e}", file=sys.stderr)
        return 2

    outdirs = ensure_dirs(Path(args.outdir))
    print(f"Found {len(tickers)} tickers from {tickers_path.name}. Range: {start}->{end}. Interval: {args.interval}")
    merged_records: List[dict] = []

    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] {t} ...", end="", flush=True)
        try:
            df = fetch_history(yf, t, start, end, args.interval, args.auto_adjust, args.retries, args.sleep)
            if df.empty:
                print(" empty")
                continue
            if args.csv:
                (outdirs["csv"] / f"{t}.csv").write_text(df.to_csv(index=True), encoding="utf-8")
            if args.json:
                with open(outdirs["json"] / f"{t}.json", "w", encoding="utf-8") as f:
                    json.dump(df_to_json_records(df), f, ensure_ascii=False, indent=2)
            if args.merged_json:
                recs = df_to_json_records(df)
                for r in recs:
                    r["symbol"] = t
                merged_records.extend(recs)
            print(" ok")
        except Exception as e:
            print(f" error: {e}")
        time.sleep(args.sleep)

    if args.merged_json and merged_records:
        merged_path = outdirs["merged"] / f"merged_{start}_to_{end}_{args.interval}.json"
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(merged_records, f, ensure_ascii=False, indent=2)
        print(f"Wrote merged JSON: {merged_path}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
