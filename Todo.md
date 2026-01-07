# Decisions Already Made 

Program logic for enums should never use .value, always serialize/deserialize at read/write boundary
  (Logging is the exception, b/c we want to see something meaningful, not just an int)

# Known Issues

Slices are getting marked as needs_refresh on restart... likely just some manifest schema issue
---

Ran datasol1m, new slices were fetched but count was not updating and it just kept refetching the same slices over and over again 
databtc1m - same issue
datasol1s - somehow decided there were 122 responses missing... (but when i started it up the were ~300)
    {'manifest': 170385, 'filesystem': 170504},
    there are all the orphan response files...
databtc1s - (need to get the full set first)

after a restart we are much closer {'count_mismatch': {'manifest': 170502, 'filesystem': 170504}, (but note that this shouldnt require a restart lol)
todo: 
---


Manifest Aggregator logs not being written to /logs 
    - first check if manifest_aggregator._setup_manifest_logging is actually being called - maybe just raise to short circuit
    - env variable does seem to be working  
---

# Decisions to be made 

come up with an import / export policy (qlir imports should look clean to the end user, no leaking of pandas etc.)
Options:

see chatgpt for implementation details: https://chatgpt.com/c/695b5d5d-9f74-8325-a6ec-fdc61066733a
---

Later Feature Branch:

Pattern for indicators, ops etc. 
Return shape is different depending on whether the func takes one or list 
  -> tuple[DataFrame, tuple[str, ...]]
  -> tuple[DataFrame, str]

but this could be applied to more than columns... if i allowed a list of window to be passed for example, you could calc all the windows at once rather than looop call the func (so maybe o(n) -> 0(1)))

I already created a helper func to grab the single item when only one thing is passed core.ops._helpers.one

there is also the question of naming (singular vs plural) sma v. smas. leaning toward singular b/c the pluralness comes from the shape of the params passed



# Priorities

qlir - data_server - ability to delete and rebuild manifest from scratch
    - try this manually (for like 1m data... delete the file then start up the server, see what happens)
    - sol1m ... the files were all marked as missing  
        - it did 54 requests... file system didnt add more (this is good, it means the determnisitic hashing func hasnt changed)
        - the question is why are they marked as missing... ahh maybe simply b/c the manifest was gone and it doesnt check the fs before starting to make requests...??

qlir - see data -> sources -> binance  (data server) -> known_issues.md

proj - Do the SMA study!!!!

proj - pipelines cli 

etl funcs unit tests 

proj - prove that the cli works for 
        - creating the user pipeline 
        - list all pipelines
         
move all manifest validation and other "venue agnostic" code to the data.sources.common modules 

manifest_validation
    - add log to df for the slice parse and open spacing violations

uklines - basically copy paste from klines worker
    - already setu pthe pyproject.toml in afterdata (but havent moved to template)

---
Mermaid diagram of project call usage (and maybe asscii)

bash script (entrypoint specified in pyproject.toml - there are currently 3 options (by-arg, all, file-def)) 
Note: The point of the bash script is to abstract away the params.. so you can just run `bash <something>.sh` and it does exactly what you want

by_arg: use args to specify the symbol/interval/limit combos
all: not yet implemented, but will call a get_symbols endpoint, then start one server for each

Each of these parse the args, then call _fetch_raw_impl which loops over the combos provided and launch a new sub proc for each.
which passes args and calls `__PROJECT_NAME.binance.etl.data_server`
 
---

## Medium Term 
- add dedicated logging to file (not just to the console)
    - this will be used for things like prometheus/grafana, so the structure needs to be tight
- prepend pid func in the setup logging
- refactor project agg_server so that we can start one subproc per symbol/interval/endpoint just like we did with the data server)
- refactor project agg_server so that we can use a RuntimeConfig (with a log_profile) so that we  just like we did with the data server)
- what else do we want to add to RuntimeConfig
    - maybe the telemetry?

## Long Term (Not a prio at all)
- log to files in addition to console (remember that this should have the option to be split by PID or rather endpoint/symbol/interval)
- Add ability to write to a non-local location (e.g. S3 bucket)
- metrics/helth endpoints

### Ta-lib integration



### Refactoring:



### Viz_Demo

- Test viz_demo.py
- make instructions for viz-demo 
    - git clone 
    - cp viz demo to your analysis directory 
    - install deps (including qlir)
    - run

we also need to handle the cases where it touches multiple ... e.g. touch lower and touch mid, touch all, or touch mid and upper

 


