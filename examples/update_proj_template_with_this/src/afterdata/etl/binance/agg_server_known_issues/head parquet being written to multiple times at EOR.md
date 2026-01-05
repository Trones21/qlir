

head.parquet is being written to multiple times 
----
[agg] todo=234 used=2600 parts=26 head_slices=0
2026-01-02 06:51:57,535 [INFO] qlir.data.agg.engine: [agg] sealed part-000027.parquet | slices=100 rows=100000 open_time=[1753125600000,1759125540000] head_remaining_slices=0
2026-01-02 06:51:57,535 [DEBUG] qlir.data.agg.engine: manifest updated
new frames path
2026-01-02 06:51:57,552 [INFO] qlir.data.agg.engine: Slices in raw manifest: 2837
[agg] todo=134 used=2700 parts=27 head_slices=0
2026-01-02 06:51:58,006 [INFO] qlir.data.agg.engine: [agg] sealed part-000028.parquet | slices=100 rows=100000 open_time=[1759125600000,1765125540000] head_remaining_slices=0
2026-01-02 06:51:58,006 [DEBUG] qlir.data.agg.engine: manifest updated
new frames path
2026-01-02 06:51:58,019 [INFO] qlir.data.agg.engine: Slices in raw manifest: 2837
[agg] todo=34 used=2800 parts=28 head_slices=0
2026-01-02 06:51:58,160 [DEBUG] qlir.data.agg.engine: write head.parquet
2026-01-02 06:51:58,180 [DEBUG] qlir.data.agg.engine: manifest updated
new frames path
2026-01-02 06:51:58,196 [INFO] qlir.data.agg.engine: Slices in raw manifest: 2837
[agg] todo=34 used=2800 parts=28 head_slices=34
2026-01-02 06:51:58,346 [DEBUG] qlir.data.agg.engine: write head.parquet
2026-01-02 06:51:58,373 [DEBUG] qlir.data.agg.engine: manifest updated
new frames path
2026-01-02 06:51:58,387 [INFO] qlir.data.agg.engine: Slices in raw manifest: 2837
[agg] todo=34 used=2800 parts=28 head_slices=68
2026-01-02 06:51:58,573 [INFO] qlir.data.agg.engine: [agg] sealed part-000029.parquet | slices=100 rows=99430 open_time=[1765125600000,1767148440000] head_remaining_slices=2
2026-01-02 06:51:58,574 [DEBUG] qlir.data.agg.engine: write head.parquet
2026-01-02 06:51:58,577 [DEBUG] qlir.data.agg.engine: manifest updated


this can be proven with poetry run main (basic etl pipeline)
...it hangs for quite some time to drop those candles
2026-01-02 06:55:09,814 [INFO] qlir.data.quality.candles.candles: Candles Summary: 
   n_rows=2832271 
   freq=count: 1 unit: minute pandas_offset: None 
   range=[2020-08-11 06:00:00+00:00 -> 2025-12-31 02:34:00+00:00] 

2026-01-02 06:55:09,814 [WARNING] qlir.data.quality.candles.candles: Dropped 67430 duplicate candles
2026-01-02 06:55:09,814 [WARNING] qlir.data.quality.candles.candles:  Detected 10 gaps which represent 1444 missing candles
2026-01-02 06:55:09,816 [INFO] qlir.logdf: 
📊 Candle Gaps (shape=(10, 3)):
| gap_start                 | gap_end                   |   missing_count |
|---------------------------|---------------------------|-----------------|
| 2021-04-25 04:02:00+00:00 | 2021-04-25 08:44:00+00:00 |             283 |
| 2021-08-13 02:00:00+00:00 | 2021-08-13 06:29:00+00:00 |             270 |
| 2020-12-21 13:48:00+00:00 | 2020-12-21 17:59:00+00:00 |             252 |
| 2021-04-20 02:00:00+00:00 | 2021-04-20 04:29:00+00:00 |             150 |
| 2021-09-29 07:00:00+00:00 | 2021-09-29 08:59:00+00:00 |             120 |
| 2021-03-06 02:00:00+00:00 | 2021-03-06 03:29:00+00:00 |              90 |
| 2023-03-24 12:40:00+00:00 | 2023-03-24 13:59:00+00:00 |              80 |
| 2021-02-11 03:41:00+00:00 | 2021-02-11 04:59:00+00:00 |              79 |
| 2020-11-30 06:00:00+00:00 | 2020-11-30 06:59:00+00:00 |              60 |
| 2020-12-25 02:00:00+00:00 | 2020-12-25 02:59:00+00:00 |              60 |
