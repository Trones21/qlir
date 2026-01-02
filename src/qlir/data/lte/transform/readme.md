intended structure 

transform/
├── gaps/
│   ├── __init__.py
│   ├── materialize.py          # already exists (orchestrator)
│   ├── blocks.py               # find contiguous missing regions
│   └── context.py              # build FillContext objects
│
├── policy/
│   ├── __init__.py
│   ├── base.py                 # FillPolicy + FillContext contract
│   ├── windowed_linear.py      # first real policy (what you described)
│   ├── constant.py             # optional but trivial + useful
│   └── registry.py             # optional: name → policy mapping




Flow:

materialize_missing_rows
        ↓
find_missing_blocks
        ↓
build_fill_context   ← structural traversal happens ONCE
        ↓
apply_fill_policy
        ↓
policy.generate      ← pure math, no structure