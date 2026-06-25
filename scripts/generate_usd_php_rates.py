import sys
from datetime import date, timedelta
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("ERROR: Missing dependencies. Run: pip install yfinance pandas")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

TICKER = "USDPHP=X"
OUTPUT_PATH = Path(__file__).parent.parent / "dbt" / \
    "seeds" / "usd_php_rates.csv"

END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=730)

# ── Fetch ─────────────────────────────────────────────────────────────────────


def fetch_usd_php(start: date, end: date) -> pd.DataFrame:
    """Download USDPHP=X OHLCV from Yahoo Finance and return a clean DataFrame."""
    print(f"Fetching {TICKER}  {start} → {end}  …")

    ticker = yf.Ticker(TICKER)
    raw = ticker.history(start=str(start), end=str(end),
                         interval="1d", auto_adjust=False)

    if raw.empty:
        raise RuntimeError(
            f"yfinance returned no data for {TICKER}. "
            "Check your internet connection or try again later."
        )

    df = raw.reset_index()

    # yfinance may return a tz-aware DatetimeTZDtype; normalise to plain date
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    df = df.rename(columns={
        "Date":   "rate_date",
        "Open":   "usd_php_open",
        "High":   "usd_php_high",
        "Low":    "usd_php_low",
        "Close":  "usd_php_close",
        "Volume": "usd_php_volume",
    })

    # Keep only the columns we care about
    keep = ["rate_date", "usd_php_open", "usd_php_high",
            "usd_php_low", "usd_php_close", "usd_php_volume"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Round FX rates to 4 decimal places (standard FX precision)
    for col in ["usd_php_open", "usd_php_high", "usd_php_low", "usd_php_close"]:
        if col in df.columns:
            df[col] = df[col].round(4)

    df["usd_php_volume"] = df.get("usd_php_volume", 0).fillna(0).astype(int)
    df["source"] = f"yahoo_finance / {TICKER}"

    # Sort chronologically and drop any duplicate dates
    df = df.sort_values("rate_date").drop_duplicates(subset=["rate_date"])
    df = df.reset_index(drop=True)

    return df


# ── Validate ──────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> None:
    """Sanity-check the fetched data before writing."""
    assert not df.empty, "DataFrame is empty after fetch"
    assert df["rate_date"].is_monotonic_increasing, "Dates are not sorted"

    # PHP has historically traded between 45 and 65 per USD (last 10 years)
    lo, hi = df["usd_php_close"].min(), df["usd_php_close"].max()
    print(f"  Close range: {lo:.4f} – {hi:.4f} PHP/USD")
    if not (40.0 <= lo <= 75.0 and 40.0 <= hi <= 75.0):
        print(
            f"  WARNING: Close values outside expected 40–75 range. "
            "Verify USDPHP=X is the correct ticker."
        )

    nulls = df[["usd_php_open", "usd_php_close"]].isna().sum().sum()
    if nulls > 0:
        print(
            f"  WARNING: {nulls} null values in open/close — check raw Yahoo data")

    print(
        f"  Rows: {len(df)}  |  First: {df['rate_date'].iloc[0]}  |  Last: {df['rate_date'].iloc[-1]}")


# ── Write ─────────────────────────────────────────────────────────────────────

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  Written → {path}  ({len(df)} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    df = fetch_usd_php(START_DATE, END_DATE)
    validate(df)
    write_csv(df, OUTPUT_PATH)
    print("Done. Re-run daily to keep seeds/usd_php_rates.csv current.")


if __name__ == "__main__":
    main()
