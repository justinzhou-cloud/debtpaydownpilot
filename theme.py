"""DoorDash-inspired palette — minimal neutrals, red accent, status-only semantic colors."""

from __future__ import annotations

import plotly.graph_objects as go

# Primary brand
DD_PRIMARY = "#FF3008"
DD_PRIMARY_MUTED = "rgba(255, 48, 8, 0.12)"

DD_RED_LINE = "#FF3008"
DD_BLACK = "#191919"
DD_GRAY_900 = "#1F2937"
DD_GRAY_600 = "#6B7280"
DD_GRAY_400 = "#9CA3AF"
DD_GRAY_200 = "#E5E7EB"
# Page shell — cool gray (Durable-style reference)
DD_PAGE_BG = "#EEF1F4"
DD_WHITE = "#FFFFFF"
# Durable-style full-width header (dark teal)
HEADER_TEAL = "#1B4D4A"
HEADER_TEAL_MUTED = "rgba(255,255,255,0.72)"

# Status (tables / emphasis)
STATUS_GREEN = "#15803D"
STATUS_GREEN_TINT = "#F0FDF4"
STATUS_RED = "#B91C1C"
STATUS_RED_TINT = "#FEF2F2"
# Net-adds reference chart — muted bar colors
CHART_GREEN_MUTED = "#689F84"
CHART_RED_MUTED = "#C47C7C"
CHART_LINE_TEAL = "#2D5C5A"
STATUS_AMBER = "#B45309"
STATUS_AMBER_TINT = "#FFFBEB"

# Chart bar (single neutral accent — not status)
CHART_BAR = "#374151"

# Durable-style health cards (teal when on track)
DURABLE_TEAL = "#2D6A65"
DURABLE_TEAL_TINT = "#E8F4F3"

FONT_STACK = (
    "'Nunito Sans', 'Proxima Nova', 'Helvetica Neue', Arial, sans-serif"
)


def style_plotly(
    fig: go.Figure,
    height: int | None = 400,
    *,
    show_legend: bool = False,
    compact_margins: bool = False,
    show_grid: bool = False,
) -> go.Figure:
    m = dict(l=48, r=24, t=28, b=48) if compact_margins else dict(l=56, r=32, t=40, b=56)
    grid_axis = dict(
        showgrid=show_grid,
        gridwidth=1,
        gridcolor=DD_GRAY_200,
        linecolor=DD_GRAY_200,
        zeroline=True,
        zerolinecolor=DD_GRAY_200,
        tickfont=dict(color=DD_GRAY_600, size=12),
        title_font=dict(color=DD_GRAY_900, size=12),
    )
    if not show_grid:
        grid_axis["zeroline"] = True
        grid_axis["zerolinecolor"] = DD_GRAY_200
        grid_axis["zerolinewidth"] = 1
    fig.update_layout(
        font=dict(family=FONT_STACK, size=13, color=DD_BLACK),
        paper_bgcolor=DD_WHITE,
        plot_bgcolor=DD_WHITE,
        colorway=[CHART_BAR, DD_GRAY_400, DD_GRAY_600],
        xaxis=dict(**grid_axis),
        yaxis=dict(**grid_axis),
        showlegend=show_legend,
        legend=dict(
            bgcolor="rgba(255,255,255,0.96)",
            bordercolor=DD_GRAY_200,
            borderwidth=1,
            font=dict(color=DD_BLACK, size=12),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        margin=m,
    )
    if height is not None:
        fig.update_layout(height=height)
    return fig


def durable_hover_style(fig: go.Figure) -> go.Figure:
    """Dark tooltip like Net adds reference (white label on charcoal)."""
    fig.update_layout(
        hoverlabel=dict(
            bgcolor="#1F2937",
            font=dict(color="#FFFFFF", family=FONT_STACK, size=13),
            bordercolor="#1F2937",
        )
    )
    return fig
