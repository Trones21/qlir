# breakouts

🚀 **QLIR Quickstart Scaffold**

This is a starter layout for a project that will use the QLIR library.

## ⚙️ Setup

1. Clone or install QLIR somewhere accessible.
2. Update the QLIR dependency in `pyproject.toml` before running install.

```toml
# Option A: use local sibling path (edit as needed)
# qlir = { path = "../qlir", develop = true }

# Option B: use a git reference (edit as needed)
# qlir = { git = "https://github.com/your-org/qlir.git", rev = "main" }
```

3. Install dependencies and run:
```bash
poetry install
poetry run python breakouts/main.py
```

## 🧩 Structure
```
breakouts/
  main.py      # entry point
  analysis/    # your analysis modules
  data/        # optional data inputs
  tests/
```

# breakouts

Detect **up/down breakout zones** on a single timeframe (start with 5-minute bars).
Outputs two nullable-boolean columns on your candles:

* `breakout_up` – True where price has broken out upward
* `breakout_down` – True where price has broken out downward

This is intentionally minimal so you can iterate quickly (thesis → tests → refinements).

---

## Why

You want a reproducible way to **tag breakout zones** directly on your candle DataFrame and then answer questions downstream (Tableau, notebooks, SQL):

* “How many breakouts last month?”
* “Average breakout size? Average number of candles?”
* “Does size correlate with duration?”

---

## Input data

A pandas DataFrame with at least:

```
index: DatetimeIndex (UTC recommended)
columns: open, high, low, close, volume
```

You can load from CSV like:

```python
df = _pd.read_csv("candles_5m.csv", parse_dates=["ts"])
df = df.set_index("ts").sort_index()
```

---

## Core rule (v0)

Single-parameter breakout detection:

* **Up breakout** at time *t* if:
  [
  \frac{close_t}{close_{t-M}} - 1 \ge \text{min_move}
  ]
* **Down breakout** at time *t* if:
  [
  \frac{close_{t-M}}{close_t} - 1 \ge \text{min_move}
  ]

Where:

* `min_move` is an absolute percentage (e.g., `0.05` = 5%)
* `M` is the number of candles (lookback window)

No regime filters, no band squeezes, no multi-timeframe coherence (yet). Keep it simple first.

---

## Quick start

```python
import pandas as _pd
from breakouts.detect import tag_breakouts_simple

df = _pd.read_parquet("sol_5m.parquet")  # or read_csv
df = df.sort_index()

df2 = tag_breakouts_simple(
    df,
    price_col="close",
    lookback=5,        # M
    min_move=0.05,     # 5%
    inplace=False
)

print(df2[["close","breakout_up","breakout_down"]].tail(15))
```

**Output columns**

* `breakout_up` (nullable boolean)
* `breakout_down` (nullable boolean)

`NaN`/undefined rows are coerced to False for determinism.

---

## CLI

```bash
poetry run python -m breakouts.cli \
  --csv data/sol_5m.csv \
  --lookback 5 \
  --min-move 0.05
```

Prints a simple table with timestamps flagged as breakouts.

---

## API

```python
def tag_breakouts_simple(
    df: _pd.DataFrame,
    price_col: str = "close",
    *,
    lookback: int = 5,
    min_move: float = 0.05,
    up_col: str = "breakout_up",
    down_col: str = "breakout_down",
    inplace: bool = False,
) -> _pd.DataFrame:
    """
    Adds two columns:
      - up_col:  True where (close_t / close_{t-lookback} - 1) >= min_move
      - down_col: True where (close_{t-lookback} / close_t - 1) >= min_move
    """
```

### Example (programmatic)

```python
from breakouts.detect import tag_breakouts_simple

df = tag_breakouts_simple(df, lookback=8, min_move=0.03)
monthly = (
    df.resample("30D")
      [["breakout_up","breakout_down"]]
      .sum(numeric_only=True)
      .rename(columns={"breakout_up":"ups","breakout_down":"downs"})
)
print(monthly)
```

---

## Notes & conventions

* **Deterministic NaN policy**: comparisons that involve NaN evaluate to False.
* **Types**: booleans use pandas nullable boolean dtype (`boolean`).
* **Naming**: use clear, stable column names by default; make them configurable via args.

---

## Tests

Minimal pytest coverage:

* Up/down triggering at exact thresholds
* Lookback alignment (no off-by-one)
* NaN handling
* `inplace=True` behavior

Run tests:

```bash
poetry run pytest -q
```

---

## Roadmap

* **Termination logic**: end-of-episode tagging (start/end windows, duration)
* **Magnitude features**: cumulative move, max runup, max drawdown
* **Multi-timeframe**: coherence filters (e.g., 1m + 5m agree)
* **“Recipes”**: Bollinger squeeze, VWAP distance, volatility regimes
* **Adapters**: Drift/Helius loader instead of CSV

---

## Design philosophy

1. **Start embarrassingly simple** → see signal → iterate.
2. **Keep outputs columnar** so you can pivot/aggregate anywhere (Tableau, pandas, SQL).
3. **Stable naming & dtypes** to make downstream work painless.

---

### Appendix: tiny reference implementation

```python
# breakouts/detect.py
from __future__ import annotations
import pandas as _pd

def tag_breakouts_simple(
    df: _pd.DataFrame,
    price_col: str = "close",
    *,
    lookback: int = 5,
    min_move: float = 0.05,
    up_col: str = "breakout_up",
    down_col: str = "breakout_down",
    inplace: bool = False,
) -> _pd.DataFrame:
    out = df if inplace else df.copy()
    s = out[price_col]
    ref = s.shift(lookback)

    up = (s / ref - 1.0) >= min_move
    dn = (ref / s - 1.0) >= min_move

    # nullable boolean and deterministic fill
    out[up_col] = up.astype("boolean").fillna(False)
    out[down_col] = dn.astype("boolean").fillna(False)
    return out
```
