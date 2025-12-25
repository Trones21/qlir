
# Priorities
manifest_validation
    - ensure that the chunks are the proper sizes

uklines - basically copy paste from klines worker
    - already setu pthe pyproject.toml in afterdata (but havent moved to template)

wrap fetch and persist slice with response timing logging (maybe use the same decorator pattern as in nocrud??)
    - possibly other funcs as well... like when we are doing something fror the entire manifest... thats ~170,000 items for the 1s interval (58MB manifest)
    - the 1s seems slow... not sure if this is my code or binance servers 

## Medium Term 
- add dedicated logging to file (not just to the console)
    - this will be used for things like prometheus/grafana, so the structure needs to be tight
- prepend pid func in the setup logging
- refactor project agg_server.py.tpl (and maybe main) so that we can start one subproc per symbol/interval/endpoint just like we did with the data server)

## Long Term (Not a prio at all)
- log to files in addition to console (remember that this should have the option to be split by PID or rather endpoint/symbol/interval)
- Add ability to write to a non-local location (e.g. S3 bucket)
- metrics/helth endpoints


## Data Integrations
   
- Binance uklines


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

 


