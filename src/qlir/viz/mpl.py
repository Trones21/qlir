def render(view: View, df):
    import matplotlib.pyplot as plt
    figs = []
    for p in view.panels:
        fig, ax = plt.subplots(figsize=(10, p.height / 96))
        ax2 = None
        for s in p.series:
            target_ax = ax if s.yaxis == 0 else (ax2 or ax.twinx())
            if s.kind == "line":
                target_ax.plot(df.index, df[s.col], label=s.label or s.col)
            elif s.kind == "scatter":
                target_ax.scatter(df.index, df[s.col], s=8, label=s.label or s.col)
            elif s.kind == "bar":
                target_ax.bar(df.index, df[s.col], label=s.label or s.col)
        for b in p.bands:
            import numpy as np
            y0 = df[b.y0] if isinstance(b.y0, str) else b.y0
            y1 = df[b.y1] if isinstance(b.y1, str) else b.y1
            ax.fill_between(df.index, y0, y1, alpha=b.alpha, label=b.label)
        for e in p.events:
            hits = df.index[df[e.when_col].astype(bool)]
            for t in hits:
                ax.axvline(t, alpha=0.2)
        if p.table:
            ax.set_title(p.title + " (Table not shown in plot)")
        else:
            ax.set_title(p.title)
        ax.legend(loc="best")
        figs.append(fig)
    return figs
