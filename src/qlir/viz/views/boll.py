from qlir.viz.core import BandSpec, Panel, SeriesSpec, TableSpec, View
from qlir.viz.registry import register


@register("boll_validation")
def boll_validation_view(price_col="close", mid="bb_mid", upper="bb_upper", lower="bb_lower", regime="bb_regime", events_df=None) -> View:
    panels = [
        Panel(
            title="Price + Bollinger Bands",
            series=[
                SeriesSpec(price_col, "line"),
                SeriesSpec(mid, "line"),
                SeriesSpec(upper, "line"),
                SeriesSpec(lower, "line"),
            ],
            bands=[BandSpec(upper, lower)],
        ),
    ]
    if events_df is not None and not events_df.empty:
        panels.append(
            Panel(
                title="Bollinger Events Table",
                table=TableSpec(cols=list(events_df.columns), rows={"kind": "tail", "n": 50}),
            )
        )
    return View(title="Bollinger Validation", panels=panels)

