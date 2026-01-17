### Scalar-style, vector-safe helpers

QLIR scalar helpers (e.g. `delta_in_bps`, `delta_in_pct`) are written
using ordinary arithmetic expressions, but are fully compatible
with Pandas Series and NumPy arrays.

This works because Pandas and NumPy overload arithmetic operators
to perform element-wise, vectorized computation while preserving
index alignment (for Pandas).

As a result:

- The same function works for scalars, arrays, and Series
- No `.apply()` or Python loops are required
- Performance remains fully vectorized

This design keeps the API simple while remaining efficient
for large-scale DataFrame pipelines.
