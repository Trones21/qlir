# Decisions Already Made 

Program logic for enums should never use .value, always serialize/deserialize at read/write boundary
  (Logging is the exception, b/c we want to see something meaningful, not just an int)

Module arch / user import ux -> See MODULE_ARCH_AND_USER_IMPORT_UX.md

# Known Issues

Count mismatch (likely just slicestatus enum not gathering everything... ahhh this is an easy invariant to solve for, just do count ! in set(slicestatus) 

---
==============================================

# Decisions to be made 

    Later Feature Branch:

    Pattern for indicators, ops etc. 
    Return shape is different depending on whether the func takes one or list 
    -> tuple[DataFrame, tuple[str, ...]]
    -> tuple[DataFrame, str]

    but this could be applied to more than columns... if i allowed a list of window to be passed for example, you could calc all the windows at once rather than looop call the func (so maybe o(n) -> 0(1)))

    I already created a helper func to grab the single item when only one thing is passed core.ops._helpers.one

    there is also the question of naming (singular vs plural) sma v. smas. leaning toward singular b/c the pluralness comes from the shape of the params passed

    UPDATE: We will likely refactor many of the funcs now that we have the AnnotatedDF and ColRegistry 

---

 - Any function that creates a new column must declare its row semantics.
     -   I started with the pattern
     -   log.info(f"SMA(base_col={col}, window={window}, rows_used=[i-{window-1} .. i], write_col={name}, write_row=i)")
     - But then create the RowDerivationSpec core.semantics
     - Should also state that we are removing intermediate columns if we do 

     - I added log_column_event, this is quite ergonomic, but doesnt expose as much info (although we could overload it to do so)
---

Decide who owns the column registration and announcement... currently using a pracitce where every func owns the logging, but only the "column_bundlers" own the registry updates 

    def persistence(df: pd.DataFrame, condition_col: str , col_name_for_added_group_id_col: str) -> AnnotatedDF:
    assert df[condition_col].any(), "No True rows for persistence analysis"


    #fillna so that all downstream consumers of this column have a clean bool view
    df[condition_col] = df[condition_col].astype("boolean").fillna(False)

    df, group_ids_col = assign_condition_group_id(df=df, condition_col=condition_col, group_col=col_name_for_added_group_id_col)
    df, contig_true_rows = univariate.with_running_true(df, group_ids_col)

    max_run_col = f"{condition_col}_run_len"
    df[max_run_col] = df.groupby(group_ids_col)[contig_true_rows].transform("max")
    log_column_event(caller="persistence", ev=ColumnLifecycleEvent(key="persistence", col=max_run_col, event="created"))

    new_cols = ColRegistry()
    new_cols.add(key="group_ids_col", column=group_ids_col)
    new_cols.add(key="max_run_col", column=max_run_col)
    new_cols.add(key="contig_true_rows", column=contig_true_rows)

    return AnnotatedDF(df=df, new_cols=new_cols)
    

---
==============================================

# Priorities

- Figure out whether analysis funcs expect tz_start to be unix ts int or timestamp... 
    - need a conversion func or something... (maybe with logging to let the user know what they passed)

- rethink summarize_condition_paths (grouping is sumuggled in here ... but maybe ok maybe we just need two smaller funcs or something for clarity.. also need to think through params, ), maybe two top level funcs summarize_condition_paths
definitely a better doc string... 
    - but really these are such different operations (one is a reducer the other add a new column before the reducer... but then its on you to ensure the condiiton col is right... )

- implement arp (and variants, including the bundle) (and expose through the api)
- implement range shock (and expose via api) 

proj - pipelines cli 

etl funcs unit tests 

proj - prove that the cli works for 
        - creating the user pipeline 
        - list all pipelines
         
move all manifest validation and other "venue agnostic" code to the data.sources.common modules 

uklines - basically copy paste from klines worker
    - already setu pthe pyproject.toml in afterdata (but havent moved to template)

---
Mermaid diagram of project call usage (and maybe asscii)

bash script (entrypoint specified in pyproject.toml - there are currently 2 options (by-arg, all)) 
Note: The point of the bash script is to abstract away the params.. so you can just run `bash <something>.sh` and it does exactly what you want

by_arg: use args to specify the symbol/interval/limit combos
all: not yet implemented, but will call a get_symbols endpoint, then start one server for each

Each of these parse the args, then call _fetch_raw_impl which loops over the combos provided and launch a new sub proc for each.
which passes args and calls `__PROJECT_NAME.binance.etl.data_server`
 
---
==============================================

## Medium Term 
- Add KMMs to all the existing readmes
- prepend pid func in the setup logging
- refactor modules to follow the MODULE_ARCH_AND_USER_IMPORT_UX.md

## Possibly
- refactor project agg_server so that we can start one subproc per symbol/interval/endpoint just like we did with the data server)
- refactor project agg_server so that we can use a RuntimeConfig (with a log_profile) so that we  just like we did with the data server)
- what else do we want to add to RuntimeConfig
    - maybe the telemetry?


---
==============================================

## Long Term (Not a prio at all)
- add specific log formatting for when logging to file
    - Currently we redirect to stdout but then files get all the ANSI characters 
    - this will be used for things like prometheus/grafana, so the structure needs to be tight
- log to files in addition to console (remember that this should have the option to be split by PID or rather endpoint/symbol/interval)
- Add ability to write to a non-local location (e.g. S3 bucket)
- metrics/helth endpoints
- Add differentiate between binance_us and binance_com so that the data and agg are stored under separate paths (currently its just "binance")
    - and then of course update the param so that its binance_us or binance_com (also eventually InteractiveBrokers) then route to the correct server config based on this
    - and ensure the url is constructed properly
- Add Interactive Brokers as a data source (should be able to use everyhing from the binance data server, just need to figure out how to intergrate with minimal duplication. Almost everything in the while loop should be identical (wall clock time, manifest logic, etc.)
    - but the manifest checks may need like a config passed or something because non-24/7 markets dont have the same convenience where you can just use wall clock time... this will have to be thought about

---
==============================================

### Ta-lib integration


---
==============================================

### Viz_Demo

- Test viz_demo.py
- make instructions for viz-demo 
    - git clone 
    - cp viz demo to your analysis directory 
    - install deps (including qlir)
    - run

we also need to handle the cases where it touches multiple ... e.g. touch lower and touch mid, touch all, or touch mid and upper

 


