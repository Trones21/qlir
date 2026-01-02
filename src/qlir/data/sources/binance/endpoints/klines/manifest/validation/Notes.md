Some of the validation is now broken due to upgrades

This is due to the fact that we dont write the meta contract when we seed the slices
----
2026-01-01 08:23:09,230 [WARNING:MANIFEST:VALIDATION:STRUCTURE] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: Manifest contains slices with different top-level structures
2026-01-01 08:23:09,230 [WARNING:MANIFEST:VALIDATION:STRUCTURE:DETAILS] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'slice_keys_count': '\x1b[1m37\x1b[0m', 'slice_keys': "['BTCUSDT:1m:1502942400000:1000', 'BTCUSDT:1m:1503002400000:1000'] ..."}
2026-01-01 08:23:09,230 [WARNING] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'structure': ['__meta_contract', 'completed_at', 'error', 'first_ts', 'http_status', 'last_ts', 'n_items', 'relative_path', 'request_count', 'requested_at', 'requested_url', 'slice_id', 'slice_status', 'slice_status_reason', 'ts']}
2026-01-01 08:23:09,230 [WARNING:MANIFEST:VALIDATION:STRUCTURE:DETAILS] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'slice_keys_count': '\x1b[1m4369\x1b[0m', 'slice_keys': "['BTCUSDT:1m:1505162400000:1000', 'BTCUSDT:1m:1505222400000:1000'] ..."}
2026-01-01 08:23:09,230 [WARNING] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'structure': ['completed_at', 'error', 'first_ts', 'http_status', 'last_ts', 'n_items', 'relative_path', 'requested_at', 'slice_id', 'slice_status']}
2026-01-01 08:23:09,237 [INFO] qlir.logdf: 
📊 Manifest Objs Counts by Structure (shape=(2, 2)):
|   slice_keys_count | structure                                                                                          |
|--------------------|----------------------------------------------------------------------------------------------------|
|                 37 | ['__meta_contract', 'completed_at', 'error', 'first_ts', 'http_status', 'last_ts', 'n_items', 're… |
|               4369 | ['completed_at', 'error', 'first_ts', 'http_status', 'last_ts', 'n_items', 'relative_path', 'requ… |



This is due to the batch writes via the delta log, will need to coordinate otherwise this mismatch may occur on startup
----
2026-01-01 08:23:09,238 [DEBUG:MANIFEST:VALIDATION:STRUCTURE] qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity: Found 38 relative path entries in manifest
2026-01-01 08:23:09,238 [DEBUG:MANIFEST:VALIDATION:MANIFEST_FS_INTEGRITY] qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity: Found 44 json files in /home/tjr/qlir_data/binance/klines/raw/BTCUSDT/1m/limit=1000/responses
2026-01-01 08:23:09,238 [DEBUG] qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity: {'count_mismatch': {'manifest': 38, 'filesystem': 44}, 'missing_response_files': [None], 'orphan_response_files': ['responses/57eb9413e3b903b01fd7b433c2f0ba1f.json', 'responses/6a66f5c29ec5aec3283742503e214b6b.json', 'responses/6ccb0e9c0958b027e47756b5e0a732f2.json', 'responses/78b0ef230e6a67ead9f30cbf96489c40.json', 'responses/d59b08c061d08fefdbf7d6d724f1fa60.json', 'responses/d8e654accd6f654c5959b1662a3ad1db.json', 'responses/ded97ab39353b49959437533d7f86c78.json']}
2026-01-01 08:23:09,238 [WARNING] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: Manifest / filesystem integrity issues detected
2026-01-01 08:23:09,239 [WARNING:MANIFEST:VALIDATION:MANIFEST_FS_TEGRIDY:DETAILS] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'count_mismatch': {'manifest': 38, 'filesystem': 44}, 'missing_response_files': [None], 'orphan_response_files': ['responses/57eb9413e3b903b01fd7b433c2f0ba1f.json', 'responses/6a66f5c29ec5aec3283742503e214b6b.json', 'responses/6ccb0e9c0958b027e47756b5e0a732f2.json', 'responses/78b0ef230e6a67ead9f30cbf96489c40.json', 'responses/d59b08c061d08fefdbf7d6d724f1fa60.json', 'responses/d8e654accd6f654c5959b1662a3ad1db.json', 'responses/ded97ab39353b49959437533d7f86c78.json']}


 
17037 v. 17027 
Not sure whats going on here, i restarted and its still off by 10.(maybe because of the 1ms...)
Next Step: See if BTC suffers this same issue
---
2026-01-01 22:57:08,002 [DEBUG] qlir.data.sources.common.claims: Claim acquired | slice_id=1b000e1f9121f9bfdca6969cf3a55c32 path=/home/tjr/qlir_data/binance/klines/raw/SOLUSDT/1s/limit=1000/claims/1b000e1f9121f9bfdca6969cf3a55c32.lock
2026-01-01 22:57:08,717 [WROTE - SLICE]: /home/tjr/qlir_data/binance/klines/raw/SOLUSDT/1s/limit=1000/responses/1b000e1f9121f9bfdca6969cf3a55c32.json
2026-01-01 22:57:08,717 [DEBUG] qlir.data.sources.binance.endpoints.klines.worker: [Fyi] - Not all keys returned as meta from fetch_and_persist_slice are written to disk. 
 Entry Keys: {'error', 'completed_at', 'slice_id', 'last_ts', 'slice_status_reason', 'slice_status', 'n_items', 'requested_url', 'slice_comp_key', 'first_ts', 'relative_path', 'http_status', 'request_count', 'requested_at'} 
 Meta Keys: dict_keys(['slice_id', 'relative_path', 'slice_comp_key', 'http_status', 'url', 'n_items', 'received_first_open', 'received_last_open', 'requested_first_open', 'requested_last_open_implicit', 'slice_status', 'slice_status_reason', 'request_param_startTime', 'request_param_endTime', 'requested_at', 'completed_at'])
2026-01-01 22:57:08,802 [WARNING] qlir.data.sources.binance.endpoints.klines.worker: Manifest summary mismatch: total=170137 accounted=170127 (complete=107295 partial=0 missing=62811 needs_refresh=3 in_progress=0 failed=18)
2026-01-01 22:57:08,802 [INFO] qlir.data.sources.binance.endpoints.klines.worker: Manifest Summary: {'total_slices': 170137, 'missing_slices': 62811, 'complete_slices': 107295, 'partial_slices': 0, 'needs_refresh': 3, 'in_progress': 0, 'failed_slices': 18, 'last_evaluated_at': '2026-01-01T20:57:08.802867+00:00'}
2026-01-01 22:57:08,803 [DEBUG] qlir.data.sources.common.claims: Claim released | slice_id=1b000e1f9121f9bfdca6969cf3a55c32 path=/home/tjr/qlir_data/binance/klines/raw/SOLUSDT/1s/limit=1000/claims/1b000e1f9121f9bfdca6969cf3a55c32.lock

