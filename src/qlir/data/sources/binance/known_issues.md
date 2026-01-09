2026-01-04 17:05:51,045 [WARNING] qlir.data.sources.binance.endpoints.klines.manifest.validation.report: {'structure': ['completed_at', 'error', 'first_ts', 'http_status', 'last_ts', 'n_items', 'relative_path', 'requested_at', 'slice_id', 'slice_status', 'slice_status_reason', 'ts']}
Process klines-worker:
Traceback (most recent call last):
  File "/usr/lib/python3.13/multiprocessing/process.py", line 313, in _bootstrap
    self.run()
    ~~~~~~~~^^
  File "/usr/lib/python3.13/multiprocessing/process.py", line 108, in run
    self._target(*self._args, **self._kwargs)
    ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/tjr/gh/qlir/src/qlir/data/sources/binance/server_config_models.py", line 36, in start_klines_worker
    run_klines_worker(
    ~~~~~~~~~~~~~~~~~^
            symbol=server_config.job_config.symbol,
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<2 lines>...
            data_root=data_root,
            ^^^^^^^^^^^^^^^^^^^^
        )
        ^
  File "/home/tjr/gh/qlir/src/qlir/data/sources/binance/endpoints/klines/worker.py", line 139, in run_klines_worker
    validate_manifest_and_fs_integrity(manifest, responses_dir)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/tjr/gh/qlir/src/qlir/data/sources/binance/endpoints/klines/manifest/validation/orchestrator.py", line 64, in validate_manifest_and_fs_integrity
    report.record_and_log_structure_validation(same_shape=same_shape, structures=structures)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/tjr/gh/qlir/src/qlir/data/sources/binance/endpoints/klines/manifest/validation/report.py", line 62, in record_and_log_structure_validation
    logdf(df_to_log, name="Manifest Objs Counts by Structure")
    ~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: logdf() got an unexpected keyword argument 'name'
Traceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "/home/tjr/afterdata/src/afterdata/etl/binance/data_server.py", line 139, in <module>
    main()
    ~~~~^^
  File "/home/tjr/afterdata/src/afterdata/etl/binance/data_server.py", line 136, in main
    start_data_server(data_server_cfg)
    ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/home/tjr/gh/qlir/src/qlir/data/sources/binance/server.py", line 102, in start_data_server
    raise RuntimeError(
        f"Child process exited unexpectedly: {p.name} (pid={p.pid})"
    )
RuntimeError: Child process exited unexpectedly: klines-worker (pid=69420)



Manifest Rebuild Plan:
1. Level 1 - Forced Rebuild on startup
 - if manifest.json is missing and responses_dir is non empty. 
 - this should be called during load or create manifest

2. Level 2 - Trust erosion
  - Triggered by aggregate fs integrity violations
  - pass a config to tune this 
  - violations must be VERY large to trigger (a mismatch will occur if the delta log hasnt been applied, and the data service should NEVER apply the delta log, because that would block the hot path)

3. Operator Inititiated (manual)
  - This is the smae code, but run outside the event loop.. uses a separate entrypoint to do just this one task
