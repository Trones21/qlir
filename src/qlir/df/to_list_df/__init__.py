"""
df.to_list_df

Transforms that change dataframe cardinality:

    DataFrame -> list[DataFrame]

Characteristics
---------------
- Outputs may overlap.
- Rows may appear in multiple outputs.
- Outputs are materialized copies.
- No partition guarantees.
- Intended for segment/window extraction workflows.
"""