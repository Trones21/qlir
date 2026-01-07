# `qlir.df.labeling`

Annotate a DataFrame with **contextual labels** instead of removing rows.

These helpers add columns or boolean flags — they *enrich* the dataset — so you can compare behavior across sessions, dates, or events without losing continuity.

---

### Core idea

Filtering *reduces*, labeling *marks*.

Typical exploratory flow:

1. Load all September candles.
2. Label which ones fall in the NY cash session.
3. Mark which rows are within ±2h of an Elon tweet.
4. Compare metrics between labeled groups.

This keeps the full dataset intact while letting you slice and aggregate by tags.

---

### Modules

| Module | Purpose |
|---------|----------|
| `date_labels.py` | Add calendar attributes (`year`, `month`, `dow`, `hour`, `quarter`, …) |
| `session_labels.py` | Boolean or categorical columns for sessions (`is_ny_cash`, `session_name`, …) |
| `event_labels.py` | Event-window marks (`is_in_event_window`, `event_id`, `seconds_from_event`, …) |
| `utils.py` | Shared timestamp utilities (ensure UTC, dedupe, merge helpers) |

---

### Example

```python
from datetime import timedelta
from qlir.df.labeling import date_labels as dlab, session_labels as slab, event_labels as elab

# Calendar info
df = dlab.add_calendar_labels(df)

# Sessions
df = slab.mark_in_session(df, tz="America/New_York", start=time(9,30), end=time(16,0), label="ny_cash")

# Events (e.g. Elon tweets)
df = elab.mark_around_events(df, elon_tweets, before=timedelta(hours=1), after=timedelta(hours=2))

# Now you can do:
df.groupby("session")["return"].mean()
df.groupby("event_id")["volume"].sum()
```
---

### Conventions

* Default timestamp column: **`tz_start`**
* All comparisons in **UTC** unless a timezone is specified
* Labeling functions usually add one or more of:

  * `is_<something>` (boolean flag)
  * `<something>_id` (categorical id)
  * `<something>_label` (human-readable tag)
* Labeling never drops rows — it just adds context

---

### When to use labeling vs filtering

| Goal                                    | Use                                          |
| --------------------------------------- | -------------------------------------------- |
| “Keep only NY session”                  | `filtering.session.ny_cash_session()`        |
| “Mark which rows are NY session”        | `labeling.session_labels.mark_in_session()`  |
| “Slice around an event”                 | `filtering.events.around_anchors()`          |
| “Tag rows near an event for comparison” | `labeling.event_labels.mark_around_events()` |

---

### Future extensions

* `mark_volatility_regime()`
* `mark_breakout_phase()`
* `mark_trend_strength()`

Labeling is the foundation for downstream analytics, model features, and conditional studies.
