"""Per-leg distance-to-trendline bundle.

NOT IMPLEMENTED. This file held a syntactically invalid sketch (`def f(...)`)
that no module imported, so nothing ever surfaced the breakage. The intended
composition is preserved below as the docstring; fill it in when the pieces it
calls exist.
"""


def distance_to_trendline_per_leg(df, leg_id, **kwargs):
    """
    Intended composition:

        df = add_leg_indexing(df, leg_id)
        df = add_distance_to_trendline(...)
        df = mark_leg_extrema(...)
        df = add_stretch_position(...)
        return df
    """
    raise NotImplementedError(
        "distance_to_trendline_per_leg is a design sketch; see the docstring for "
        "the intended composition."
    )
