import pandas as pd
from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.data.lte.transform.policy.base import FillContext, FillPolicy


class OrderedSourceFillPolicy(FillPolicy):
    name = "ordered_source_backfill"

    def __init__(
        self,
        *,
        sources: list[tuple[str, pd.DataFrame]],
        ohlc_cols: OHLC_Cols = DEFAULT_OHLC_COLS,
        source_col: str = "__filled_from_source__",
    ):
        """
        Parameters
        ----------
        sources
            Ordered list of (source_name, dataframe).
            The first entry is the primary source.
            Subsequent entries are fallbacks.
        ohlc_cols
            OHLC column names to copy.
        source_col
            Column used to annotate where a filled value came from.
        """
        self.sources = sources
        self.ohlc_cols = ohlc_cols
        self.source_col = source_col

    def generate(self, ctx: FillContext) -> pd.DataFrame:
        """
        Generate replacement OHLC values for missing rows using
        ordered fallback sources.

        Notes
        -----
        - Only rows in ctx.timestamps are considered
        - Existing primary values are never overwritten
        - Temporal context is ignored
        """
        primary_name, primary_df = self.sources[0]

        out = pd.DataFrame(
            index=ctx.timestamps,
            columns=list(self.ohlc_cols) + [self.source_col],
            dtype="float64",
        )

        for ts in ctx.timestamps:
            # Sanity check: primary must be missing here
            if not primary_df.loc[ts, self.ohlc_cols].isna().any():
                continue

            for source_name, src_df in self.sources[1:]:
                row = src_df.loc[ts, self.ohlc_cols]

                if not row.isna().any():
                    out.loc[ts, self.ohlc_cols] = row.values
                    out.loc[ts, self.source_col] = source_name
                    break

        return out
