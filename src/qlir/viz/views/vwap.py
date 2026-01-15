from qlir.viz.core import BandSpec, Panel, SeriesSpec, TableSpec, View
from qlir.viz.registry import register


@register("vwap_distance")
def vwap_distance_view(prefix="vwap_", price_col="close", vwap_col="vwap") -> View:
    dist = f"{prefix}dist"
    z = f"{prefix}z"
    rel = "price_rel_vwap"
    return View(
        title="VWAP Distance",
        panels=[
            Panel(
                title="Price vs VWAP",
                series=[
                    SeriesSpec(price_col, "line", 0, "price"),
                    SeriesSpec(vwap_col, "line", 0, "vwap"),
                ],
            ),
            Panel(
                title="VWAP Distance (abs & z)",
                series=[SeriesSpec(dist, "line"), SeriesSpec(z, "line", 1, "z-score")],
                bands=[BandSpec(0.0, 0.0)],
            ),
            Panel(
                title="VWAP Summary Table",
                table=TableSpec(
                    cols=[price_col, vwap_col, dist, f"{prefix}dist_pct", z, rel],
                    rows={"kind": "tail", "n": 80},
                ),
            ),
        ],
    )

