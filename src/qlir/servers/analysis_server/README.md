Refactored a bit (adding alert types, but the core flow is essentially still the same)


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
