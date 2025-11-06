# `qlir.df.filtering`

Subset a DataFrame by **time** or **event** semantics.

These helpers return smaller DataFrames — they *reduce* rows — so you can focus on relevant periods, sessions, or windows.

---

### Core idea

You usually start broad and then narrow:

1. “Show me just September.”
2. “Then only NY cash session.”
3. “Then ±2 hours around Elon’s tweets.”
4. “Done — now I have only what I care about.”

Filtering is about **exclusion** — throw away what’s irrelevant.

---

### Modules

| Module | Purpose |
|---------|----------|
| `date.py` | Calendar-style and intraday filters (`in_month`, `in_hour_of_day`, `in_day_of_week`, …) |
| `session.py` | Market-session filters (`ny_cash_session`, `ny_extended`, `frankfurt_session`, `london_newyork_overlap`, …) |
| `events.py` | Event-anchored windows — pick events, build time ranges, subset the DataFrame |

---

### Typical workflow

```python
from datetime import timedelta
from qlir.df.filtering import date as fdate, session as fses, events as fev

# 1. Time slice
sept = fdate.in_month_of_year(df, 2024, 9)

# 2. Market context
ny = fses.ny_cash_session(sept)

# 3. Event focus
around_tweets = fev.around_anchors(
    ny,
    elon_tweets,
    before=timedelta(hours=1),
    after=timedelta(hours=2),
)
````

Now `around_tweets` contains only rows inside the chosen month, session, and ±window around your event list.

---

### Conventions

* Default timestamp column: **`tz_start`**
* All timestamps stored in **UTC**
* Session filters convert to the local market timezone internally
* `around_anchors()` / `before_anchors()` / `after_anchors()` expect UTC events
* Helpers like `where_all`, `where_any`, and `combine_events` let you select subsets of events before windowing

---

### When *not* to filter

If you just want to **mark** rows (“inside session?”, “within event window?”) instead of removing them,
see [`qlir.df.labeling`](../labeling/___README.md).