## 1️⃣ Analysis server directory structure

```
analysis_server/
  server.py                 # main infinite loop

  io/
    parquet_window.py       # load_latest_parquet_window()

  state/
    progress.py             # last_processed_data_ts

  checks/
    data_freshness.py       # now_ts vs data_ts

  emit/
    alert.py                # write { ts, data } JSON
```

This is intentionally **flat, boring, and explicit**.

---

## 2️⃣ File responsibilities (crystal clear)

### **`server.py`**

* Owns the infinite loop
* Orchestrates everything
* No I/O logic
* No Parquet logic
* No alert formatting

---

### **`io/parquet_window.py`**

* Filesystem-level Parquet logic
* No timestamps
* No analysis meaning
* Returns DataFrames only

Contains:

* `load_latest_parquet_window(...)`

---

### **`state/progress.py`**

* Tracks *only*:

* last processed `data_ts`
* No alert knowledge
* No filesystem scanning

---

### **`checks/data_freshness.py`**

* Time math only
* Knows:

  * `now_ts`
  * `data_ts`
  * allowed lag
* Emits booleans / metrics

---

### **`emit/alert.py`**

* Writes alert JSON to outbox
* Enforces `{ ts, data }` contract
* No delivery knowledge

---

## 3️⃣ Analysis server control flow (this is the invariant)

Every loop iteration:

```
1. Load latest parquet window
2. If empty → sleep
3. Extract last row
4. Extract data_ts
5. Compare with last_processed_data_ts
   └─ if not newer → sleep
6. Check data freshness
   └─ if stale → emit stale-data alert
7. Evaluate trigger column
   └─ if True → emit signal alert
8. Persist last_processed_data_ts
9. Sleep
```

No branching beyond that.


## 6️⃣ Mental model

> The analysis server is just:
>
> **“Take the most recent derived data,
> evaluate one row,
> emit facts.”**

Everything else is plumbing.
