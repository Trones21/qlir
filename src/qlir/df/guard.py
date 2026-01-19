class FrameViolation(RuntimeError):
    pass


def bind_frame(df, *, frozen=None):
    frozen = set(frozen or df.columns)
    rows = len(df)

    # attach guard metadata
    df._qlir_guard = {
        "rows": rows,
        "frozen": frozen,
    }

    # ---- __setitem__ guard ---------------------------------

    _setitem = df.__setitem__

    def guarded_setitem(key, value):
        if key in df._qlir_guard["frozen"]:
            raise FrameViolation(f"Frozen column overwrite: {key}")
        _setitem(key, value)
        if len(df) != df._qlir_guard["rows"]:
            raise FrameViolation("Row count changed")

    df.__setitem__ = guarded_setitem

    # ---- join guard ----------------------------------------

    _join = df.join

    def guarded_join(*args, **kwargs):
        out = _join(*args, **kwargs)
        if len(out) != rows:
            raise FrameViolation("Join changed row count")
        bind_frame(out, frozen=frozen)
        return out

    df.join = guarded_join

    return df
