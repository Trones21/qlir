Here’s an updated `README.md` for your `resampling/` folder — rewritten to reflect the full picture:
1-minute base data, `in_unit`/`out_unit` logic, offsets as a separate concern, and practical memory notes.

---

````markdown
# Resampling Utilities

Utilities for generating custom candle data from high-resolution (typically 1-minute) datasets.

This module provides two complementary functions:

- **`generate_candles` / `generate_candles_from_1m`** – Create arbitrary higher-timeframe aggregates (e.g. 7-minute, 23-minute, 2-hour candles) directly from uniform base data.
- **`generate_offset_candles`** – Produce *phase-shifted* versions of a single candle size (e.g. all 7 possible alignments of a 7-minute candle).

Both are intended for **in-memory research**, not long-term persistence.

---

## 📘 `generate_candles_from_1m(df, out_unit, counts, ...)`

Generates higher-timeframe candles from a strictly 1-minute DataFrame.

### Parameters
| name | type | description |
|------|------|--------------|
| `df` | DataFrame | Must represent homogeneous 1-minute candles. |
| `out_unit` | str | `"minute"`, `"hour"`, or `"day"`. |
| `counts` | iterable[int] | The set of target multipliers (e.g. `[5, 7, 23]` or `range(1,25)`). |
| `dt_col` | str | Datetime column name (default `"timestamp"`). |

### Behavior
1. Runs `infer_freq`, `ensure_homogeneous_candle_set`, and `detect_gaps` at the top.
2. Raises an error if the inferred frequency is not `1min`.
3. Resamples to each target frequency using standard OHLCV aggregation.

### Example
```python
frames = generate_candles_from_1m(df_1m, out_unit="minute", counts=[5, 7, 23, 55])
candles_23m = frames["23min"]
````

---

## 📘 `generate_candles(df, in_unit, out_unit, counts, ...)`

Generalized version for cases where the base data is *not* 1-minute (e.g. you already have 5-minute or 1-hour candles).

### Example

```python
frames = generate_candles(df_1h, in_unit="hour", out_unit="hour", counts=[2, 4, 6, 12])
candles_4h = frames["4H"]
```

This form saves memory when resampling large datasets, since you avoid expanding 1-minute data unnecessarily.

---

## 📘 `generate_offset_candles(df, period, unit="minute", ...)`

Produces **phase-shifted** versions of a single candle size.
Each offset starts one base unit later than the previous alignment.

### Example

```python
offsets = generate_offset_candles(df_1m, period=7, unit="minute")
offsets["7min@3"]
```

### Notes

* The maximum number of offsets = `period`.
* Useful for studying **timeframe coherence**, where you test whether a pattern persists across all possible alignments.
* Returns a dict keyed like `"7min@0"`, `"7min@1"`, etc.

---

## 🧠 Implementation Details

### Validation

Every function calls:

* `infer_freq(df)` → detect base frequency.
* `ensure_homogeneous_candle_set(df, freq)` → verify consistent spacing.
* `detect_gaps(df, freq)` → identify missing intervals.

This ensures clean data before aggregation.

### Aggregation

Uses a standard OHLCV map:

```python
ohlc_map = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum",
}
```

---

## ⚙️ Memory Considerations

* **1-minute candles:** 1,440 per day → ~525,600 per year per instrument.
* **Approximate footprint:** ~40–50 MB/year for one instrument (5 numeric columns + datetime).
* Even dozens of resampled variants are comfortably under a few hundred MB in memory.

If generating very large combinatorial sets (e.g. all offsets × all periods × many instruments),
prefer the iterator form (`for freq, df in iter_candles(...)`) or persist intermediate results to disk.

---

## 📁 Folder Structure

```
resampling/
 ├── generate_candles.py          # core resampling (1m → multi-TF)
 ├── generate_offset_candles.py   # offset / phase alignment views
 ├── __init__.py
 └── README.md                    # you are here
```

---

### Future Extensions

* Volume/tick-based bars (`volume_bar`, `tick_bar`)
* Rolling aggregation windows for volatility studies
* Coherence scoring across offsets or mixed frequencies

```

---

Would you like me to include the short code snippets for both `generate_candles` and `generate_offset_candles` inline in that README (so it’s self-contained), or do you prefer keeping it more like an API reference summary?
```
