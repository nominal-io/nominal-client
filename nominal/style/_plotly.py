"""The plotly style."""

from __future__ import annotations

from nominal.style._palette import COLORWAY, GRID, TEXT

TEMPLATE = "nominal"
"""Name the plotly style is registered under in ``plotly.io.templates``."""


def use_plotly() -> None:
    """Register the ``"nominal"`` plotly template and make it the default.

    The template is wide and transparent, with faint gridlines and grey text, so plots sit on
    the page in both dark and light themes. It is layered on ``plotly_white``. It applies to every
    plotly figure made afterwards, including ``df.nom.wide().plot(backend="plotly")``.
    """
    try:
        import plotly.io as pio
    except ImportError as e:
        raise ImportError("nominal.style needs plotly for this style: pip install nominal[plotly]") from e

    axis = dict(gridcolor=GRID, linecolor=GRID, zeroline=False)
    pio.templates[TEMPLATE] = dict(
        layout=dict(
            paper_bgcolor="rgba(0, 0, 0, 0)",
            plot_bgcolor="rgba(0, 0, 0, 0)",
            font=dict(color=TEXT, family="Geist, sans-serif"),
            colorway=list(COLORWAY),
            height=320,
            margin=dict(l=50, r=10, t=40, b=40),
            xaxis=axis,
            yaxis=axis,
            legend=dict(orientation="h", x=0, y=1.02, yanchor="bottom"),  # top left, clear of the toolbar
            hovermode="x unified",
        )
    )
    pio.templates.default = f"plotly_white+{TEMPLATE}"
