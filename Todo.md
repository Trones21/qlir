
# Priorities

uklines - basically copy paste from klines worker
    - will need to figure this out on the user facing side (pyproject.toml (how to specify enpoint))

partials are being saved, and never overwritten

## Medium Term 
- prepend pid func in the setup logging
- refactor project agg_server.py.tpl (and maybe main) so that we can start one subproc per symbol/interval/endpoint 9just like we did with the data server)

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

 


