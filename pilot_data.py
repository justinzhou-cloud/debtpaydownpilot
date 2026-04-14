"""
Debt paydown pilot — data loading and HTML helpers (no UI framework).

Used by build_dashboard.py to generate dashboard/data.json.
"""

from __future__ import annotations

import html
import json
import math
from pathlib import Path

import pandas as pd

from snowflake_client import ROOT, connect_snowflake, load_env_file, query_to_dataframe
from theme import (
    CHART_GREEN_MUTED,
    CHART_LINE_TEAL,
    CHART_RED_MUTED,
    DD_BLACK,
    DD_GRAY_600,
    STATUS_GREEN,
    STATUS_RED,
)

load_env_file(ROOT / ".env")

SQL_COHORT = ROOT / "sql" / "pilot_dasher_weeks.sql"
SQL_ACTIVITY = ROOT / "sql" / "pilot_activity.sql"
SQL_ALLOCATION_EVENTS = ROOT / "sql" / "pilot_allocation_events.sql"
CACHE_DIR = ROOT / ".pilot_cache"
COHORT_CACHE = CACHE_DIR / "cohort.pkl"
ACTIVITY_CACHE = CACHE_DIR / "activity.pkl"
ALLOCATION_CACHE = CACHE_DIR / "allocation.pkl"
CACHE_META = CACHE_DIR / "meta.json"
PILOT_MIN_WEEK_START = pd.Timestamp("2026-03-30").normalize()
WEEK_TOTAL_KEY = "__total_since_mar30__"
HEALTH_THRESHOLD = 9
LARGE_WITHDRAWAL_TODAY_USD = 75.0

_snowflake_conn = None


def get_snowflake_connection():
    global _snowflake_conn
    if _snowflake_conn is None:
        _snowflake_conn = connect_snowflake()
    return _snowflake_conn


def normalize_snowflake_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().strip('"').lower() for c in out.columns]
    return out


def load_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def save_data_cache(
    cohort: pd.DataFrame,
    activity: pd.DataFrame,
    allocation_events: pd.DataFrame | None = None,
) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cohort.to_pickle(COHORT_CACHE)
    activity.to_pickle(ACTIVITY_CACHE)
    if allocation_events is not None:
        allocation_events.to_pickle(ALLOCATION_CACHE)
    CACHE_META.write_text(
        json.dumps({"saved_at": pd.Timestamp.now().isoformat()}),
        encoding="utf-8",
    )


def load_data_cache() -> tuple[pd.DataFrame, pd.DataFrame] | None:
    if not (COHORT_CACHE.is_file() and ACTIVITY_CACHE.is_file()):
        return None
    try:
        return pd.read_pickle(COHORT_CACHE), pd.read_pickle(ACTIVITY_CACHE)
    except Exception:
        return None


def load_allocation_cache() -> pd.DataFrame:
    if not ALLOCATION_CACHE.is_file():
        return pd.DataFrame()
    try:
        return pd.read_pickle(ALLOCATION_CACHE)
    except Exception:
        return pd.DataFrame()


def clear_data_cache_files() -> None:
    for p in (COHORT_CACHE, ACTIVITY_CACHE, ALLOCATION_CACHE, CACHE_META):
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def run_query(sql: str) -> pd.DataFrame:
    return normalize_snowflake_columns(query_to_dataframe(get_snowflake_connection(), sql))


def fmt_week_label(ts: pd.Timestamp) -> str:
    ts = pd.Timestamp(ts).normalize()
    return f"{ts:%b} {int(ts.day)}, {ts:%Y}"


def fmt_money(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"${float(x):,.2f}"


def fmt_net_cell(net: float) -> str:
    if net is None or (isinstance(net, float) and pd.isna(net)):
        return "—"
    v = float(net)
    if abs(v) < 0.005:
        return fmt_money(0.0)
    if v > 0:
        return f"+${v:,.2f}"
    return f"−${abs(v):,.2f}"


def _fin(x: float) -> float:
    if x is None or (isinstance(x, float) and (pd.isna(x) or math.isinf(x))):
        return 0.0
    return float(x)


def today_manual_withdraw_by_participant(act: pd.DataFrame) -> dict[str, float]:
    act = prepare_activity_df(act)
    if act.empty or "activity_day" not in act.columns:
        return {}
    today = pd.Timestamp.now().normalize()
    sub = act.loc[act["activity_day"] == today].copy()
    if sub.empty:
        return {}
    sub["ow"] = sub.apply(outflow_amount, axis=1)
    g = sub.groupby(sub["participant_name"].astype(str), as_index=True)["ow"].sum()
    return {str(k): float(v) for k, v in g.items() if float(v) > 0}


def monday_of(d: pd.Timestamp) -> pd.Timestamp:
    d = pd.Timestamp(d).normalize()
    return d - pd.Timedelta(days=int(d.weekday()))


def pct_to_display(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    x = float(v)
    if x <= 1.05:
        x *= 100.0
    return f"{x:.0f}%"


def pct_to_float(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    x = float(v)
    if x <= 1.05:
        x *= 100.0
    return x


def prepare_pilot_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_snowflake_columns(raw).copy()
    if "week_start" not in df.columns:
        return df
    df["week_start"] = pd.to_datetime(df["week_start"], utc=True, errors="coerce")
    df["week_start"] = df["week_start"].dt.tz_localize(None).dt.normalize()
    num_cols = [
        "hours_dashed",
        "dash_earnings",
        "manual_save_deposits",
        "auto_save_deposits",
        "jar_outflow",
        "end_balance",
        "recommended_dashing_hours",
        "planned_sj_allocation_pct",
        "card_balance",
        "allocation_start_of_wk",
        "allocation_end_of_wk",
        "allocation_min_in_wk",
        "allocation_max_in_wk",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    rh = df["recommended_dashing_hours"].replace(0, pd.NA) if "recommended_dashing_hours" in df.columns else pd.NA
    if "hours_dashed" in df.columns:
        df["hours_attainment_pct"] = (df["hours_dashed"] / rh * 100).fillna(0.0)
    else:
        df["hours_attainment_pct"] = 0.0
    m = df["manual_save_deposits"] if "manual_save_deposits" in df.columns else pd.Series(0.0, index=df.index)
    a = df["auto_save_deposits"] if "auto_save_deposits" in df.columns else pd.Series(0.0, index=df.index)
    df["total_deposits"] = m.fillna(0) + a.fillna(0)
    return df


def filter_eligible_weeks(df: pd.DataFrame) -> pd.DataFrame:
    if "week_start" not in df.columns:
        return df
    today = pd.Timestamp.now().normalize()
    this_monday = monday_of(today).normalize()
    ws = pd.to_datetime(df["week_start"]).dt.normalize()
    return df.loc[(ws >= PILOT_MIN_WEEK_START) & (ws <= this_monday)].copy()


def prepare_activity_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = normalize_snowflake_columns(raw).copy()
    need = {"created_at", "week_start", "participant_name", "transfer_type", "amount"}
    if not need.issubset(df.columns):
        return df
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce").dt.tz_localize(None)
    df["week_start"] = pd.to_datetime(df["week_start"], utc=True, errors="coerce").dt.tz_localize(None).dt.normalize()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["activity_day"] = df["created_at"].dt.normalize()
    return df


def inflow_amount(row) -> float:
    t = str(row.get("transfer_type", "") or "")
    a = float(row.get("amount") or 0)
    if t in ("ManualSave", "AutoSave"):
        return abs(a)
    return 0.0


def outflow_amount(row) -> float:
    t = str(row.get("transfer_type", "") or "")
    a = float(row.get("amount") or 0)
    if t == "ManualWithdraw":
        return abs(a)
    return 0.0


def _daily_activity_placeholder(
    week_start: pd.Timestamp | None,
    total_mode: bool,
) -> tuple[list[str], list[float], list[float]]:
    """Seven-day (or pilot-to-date) axes with zeros when activity feed is missing."""
    today = pd.Timestamp.now().normalize()
    if total_mode:
        days = pd.date_range(PILOT_MIN_WEEK_START.normalize(), today.normalize(), freq="D")
    else:
        if week_start is None:
            return [], [], []
        ws = pd.Timestamp(week_start).normalize()
        days = pd.date_range(ws, periods=7, freq="D")
    if len(days) == 0:
        return [], [], []
    labels = [f"{d:%b} {int(d.day)}" for d in days]
    z = [0.0] * len(labels)
    return labels, z, z


def build_daily_activity(
    act: pd.DataFrame,
    *,
    week_start: pd.Timestamp | None,
    participant: str,
    total_mode: bool,
) -> tuple[list[str], list[float], list[float]]:
    act = prepare_activity_df(act)
    if act.empty or "activity_day" not in act.columns:
        return _daily_activity_placeholder(week_start, total_mode)

    sub = act.copy()
    if participant and participant != "All participants":
        sub = sub[sub["participant_name"] == participant]

    today = pd.Timestamp.now().normalize()

    if total_mode:
        sub = sub[(sub["activity_day"] >= PILOT_MIN_WEEK_START.normalize()) & (sub["activity_day"] <= today)]
        days = pd.date_range(PILOT_MIN_WEEK_START.normalize(), today.normalize(), freq="D")
    else:
        if week_start is None:
            return [], [], []
        week_start = pd.Timestamp(week_start).normalize()
        week_end = week_start + pd.Timedelta(days=6)
        sub = sub[(sub["activity_day"] >= week_start) & (sub["activity_day"] <= week_end)]
        days = pd.date_range(week_start, periods=7, freq="D")

    sub["inflow_amt"] = sub.apply(inflow_amount, axis=1)
    sub["outflow_amt"] = sub.apply(outflow_amount, axis=1)

    daily = sub.groupby("activity_day", as_index=False).agg(
        inflow=("inflow_amt", "sum"),
        outflow=("outflow_amt", "sum"),
    )
    daily_map: dict[pd.Timestamp, tuple[float, float]] = {}
    for _, row in daily.iterrows():
        d = pd.Timestamp(row["activity_day"]).normalize()
        daily_map[d] = (float(row["inflow"]), float(row["outflow"]))

    labels: list[str] = []
    ins: list[float] = []
    outs: list[float] = []
    for d in days:
        d = pd.Timestamp(d).normalize()
        labels.append(f"{d:%b} {int(d.day)}")
        inf, ouf = daily_map.get(d, (0.0, 0.0))
        ins.append(inf)
        outs.append(ouf)
    return labels, ins, outs


def build_weekly_activity(
    act: pd.DataFrame,
    *,
    week_start: pd.Timestamp | None,
    participant: str,
    total_mode: bool,
) -> tuple[list[str], list[float], list[float]]:
    act = prepare_activity_df(act)
    if act.empty or "activity_day" not in act.columns:
        return [], [], []

    sub = act.copy()
    if participant and participant != "All participants":
        sub = sub[sub["participant_name"] == participant]

    today = pd.Timestamp.now().normalize()
    sub["week_monday"] = sub["activity_day"].apply(lambda d: monday_of(pd.Timestamp(d)))

    sub["inflow_amt"] = sub.apply(inflow_amount, axis=1)
    sub["outflow_amt"] = sub.apply(outflow_amount, axis=1)

    weekly = sub.groupby("week_monday", as_index=False).agg(
        inflow=("inflow_amt", "sum"),
        outflow=("outflow_amt", "sum"),
    )
    wmap: dict[pd.Timestamp, tuple[float, float]] = {}
    for _, row in weekly.iterrows():
        d = pd.Timestamp(row["week_monday"]).normalize()
        wmap[d] = (float(row["inflow"]), float(row["outflow"]))

    mondays: list[pd.Timestamp] = []
    if total_mode:
        start_m = monday_of(PILOT_MIN_WEEK_START.normalize())
        end_m = monday_of(today)
        d = start_m
        while d <= end_m:
            mondays.append(d)
            d = d + pd.Timedelta(days=7)
    else:
        if week_start is None:
            return [], [], []
        ws = pd.Timestamp(week_start).normalize()
        mondays = [ws]

    labels: list[str] = []
    ins: list[float] = []
    outs: list[float] = []
    for d in mondays:
        d = pd.Timestamp(d).normalize()
        labels.append(f"{d:%b} {int(d.day)}")
        inf, ouf = wmap.get(d, (0.0, 0.0))
        ins.append(inf)
        outs.append(ouf)
    return labels, ins, outs


def cohort_size(df: pd.DataFrame) -> int:
    if "dasher_id" in df.columns:
        return int(df["dasher_id"].nunique())
    return len(df)


def row_allocation_on(r: pd.Series) -> bool:
    end = r.get("allocation_end_of_wk")
    mx = r.get("allocation_max_in_wk")
    if pd.notna(end) and float(end) > 0:
        return True
    if pd.notna(mx) and float(mx) > 0:
        return True
    return False


def count_health_metrics(
    slice_df: pd.DataFrame, *, is_total_aggregate: bool
) -> tuple[int, int]:
    if slice_df.empty:
        return 0, 0
    if not is_total_aggregate:
        alloc = int(slice_df.apply(row_allocation_on, axis=1).sum())
        contrib = int((slice_df["total_deposits"].fillna(0) > 0).sum())
        return alloc, contrib
    alloc_n = contrib_n = 0
    for _, g in slice_df.groupby("dasher_id"):
        g = g.sort_values("week_start")
        a_on = (g["allocation_end_of_wk"].fillna(0).max() > 0) or (
            g["allocation_max_in_wk"].fillna(0).max() > 0
        )
        if a_on:
            alloc_n += 1
        if g["total_deposits"].sum() > 0:
            contrib_n += 1
    return alloc_n, contrib_n


def aggregate_by_dasher(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for did, g in df.groupby("dasher_id"):
        g = g.sort_values("week_start")
        goal_sum = g["recommended_dashing_hours"].fillna(0).sum()
        hrs = g["hours_dashed"].fillna(0).sum()
        pct = (hrs / goal_sum * 100) if goal_sum > 0 else 0.0
        end_bal = None
        if "end_balance" in g.columns:
            end_bal = g["end_balance"].iloc[-1]
        rows.append(
            {
                "dasher_id": did,
                "participant_name": g["participant_name"].iloc[-1],
                "total_deposits": g["total_deposits"].fillna(0).sum(),
                "jar_outflow": g["jar_outflow"].fillna(0).sum(),
                "hours_dashed": hrs,
                "recommended_dashing_hours": goal_sum,
                "hours_attainment_pct": pct,
                "allocation_start_of_wk": g["allocation_start_of_wk"].iloc[0],
                "allocation_end_of_wk": g["allocation_end_of_wk"].iloc[-1],
                "end_balance": end_bal,
            }
        )
    return pd.DataFrame(rows)


def allocation_change_block(
    start_raw, end_raw, week_context: str
) -> tuple[str, str]:
    ps = pct_to_float(start_raw)
    pe = pct_to_float(end_raw)
    pct_color = DD_BLACK
    if ps is None or pe is None:
        return "", pct_color
    if abs(ps - pe) < 0.05:
        return "", DD_GRAY_600
    ps_d = pct_to_display(start_raw)
    pe_d = pct_to_display(end_raw)
    if pe < ps:
        arrow = "&#9660;"
        tip = f"Reduced savings jar allocation from {ps_d} to {pe_d} ({week_context})"
        pct_color = STATUS_RED
    else:
        arrow = "&#9650;"
        tip = f"Increased savings jar allocation from {ps_d} to {pe_d} ({week_context})"
        pct_color = STATUS_GREEN
    tip_safe = html.escape(tip)
    frag = (
        f'<span class="sj-arrow-wrap">{arrow}'
        f'<span class="sj-tip">{tip_safe}</span></span>'
    )
    return frag, pct_color


def prepare_allocation_events_df(raw: pd.DataFrame | None) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["dasher_id", "participant_name", "event_at", "allocation_pct"])
    df = normalize_snowflake_columns(raw).copy()
    if "event_at" in df.columns:
        df["event_at"] = pd.to_datetime(df["event_at"], utc=True, errors="coerce").dt.tz_localize(None)
    if "allocation_pct" in df.columns:
        df["allocation_pct"] = pd.to_numeric(df["allocation_pct"], errors="coerce")
    if "dasher_id" in df.columns:
        df["dasher_id"] = pd.to_numeric(df["dasher_id"], errors="coerce")
    return df


def allocation_cell_html(
    events_df: pd.DataFrame,
    dasher_id: int,
    start_pct,
    end_pct,
    *,
    event_start: pd.Timestamp,
    event_end: pd.Timestamp,
) -> str:
    """Savings jar % with ▲/▼ and hover tip (day of change when events exist)."""
    ps = pct_to_float(start_pct)
    pe = pct_to_float(end_pct)
    disp = pct_to_display(end_pct) if pe is not None else "—"
    pct_cls = "sj-pct-val"
    arrow = ""
    if ps is not None and pe is not None:
        if pe > ps + 0.05:
            pct_cls = "sj-pct-val sj-pct-val--up"
            arrow = '<span class="sj-arrow-ind sj-arrow-up" aria-hidden="true">▲</span>'
        elif pe < ps - 0.05:
            pct_cls = "sj-pct-val sj-pct-val--down"
            arrow = '<span class="sj-arrow-ind sj-arrow-down" aria-hidden="true">▼</span>'
    base_val = f'<span class="{pct_cls}">{html.escape(disp)}</span>'

    tip_lines: list[str] = []
    ws = pd.Timestamp(event_start).normalize()
    we = pd.Timestamp(event_end).normalize()
    if events_df is not None and not events_df.empty:
        sub = events_df[events_df["dasher_id"].notna()].copy()
        sub["dasher_id"] = sub["dasher_id"].astype(int)
        sub = sub[
            (sub["dasher_id"] == int(dasher_id)) & (sub["event_at"] >= ws) & (sub["event_at"] < we)
        ].sort_values("event_at")
        prev = ps
        last_change: tuple[float | None, float, pd.Timestamp] | None = None
        for _, row in sub.iterrows():
            newp = pct_to_float(row.get("allocation_pct"))
            if newp is None:
                continue
            if prev is not None and abs(newp - prev) >= 0.05:
                last_change = (prev, newp, row["event_at"])
            elif prev is None:
                last_change = (None, newp, row["event_at"])
            prev = newp
        if last_change is not None:
            a, b, ts = last_change
            day_s = fmt_week_label(pd.Timestamp(ts).normalize())
            if a is None:
                tip_lines.append(f"Set to {pct_to_display(b)} on {day_s}")
            else:
                tip_lines.append(f"Changed from {pct_to_display(a)} to {pct_to_display(b)} on {day_s}")
        elif ps is not None and pe is not None and abs(ps - pe) >= 0.05:
            tip_lines.append(
                f"Snapshot: {pct_to_display(ps)} → {pct_to_display(pe)} (no granular event in window)"
            )
    elif ps is not None and pe is not None and abs(ps - pe) >= 0.05:
        tip_lines.append(f"Snapshot: {pct_to_display(ps)} → {pct_to_display(pe)}")

    if not tip_lines:
        return base_val + arrow
    tip_esc = html.escape(" ".join(tip_lines))
    return (
        f'<span class="sj-arrow-wrap">{base_val}{arrow}'
        f'<span class="sj-tip">{tip_esc}</span></span>'
    )


def participant_row_flags(r: pd.Series, *, week_outflow_threshold: float = 500.0) -> list[str]:
    flags: list[str] = []
    inf = float(r.get("total_deposits") or 0)
    ouf = float(r.get("jar_outflow") or 0)
    if inf <= 0:
        flags.append("no inflow")
    if ouf > 0:
        flags.append("withdrawal")
    if ouf >= week_outflow_threshold:
        flags.append("high outflow")
    return flags


def render_participant_master_table(
    wk: pd.DataFrame,
    events_df: pd.DataFrame,
    *,
    event_start: pd.Timestamp,
    event_end: pd.Timestamp,
    withdraw_today_by_name: dict[str, float] | None = None,
) -> str:
    """Participant snapshot: SJ %, in/out/net coloring, latest jar balance, flags."""
    rows: list[pd.Series] = [r for _, r in wk.iterrows()]
    rows.sort(key=lambda r: (-float(r.get("jar_outflow") or 0), str(r.get("participant_name", "")).lower()))

    body: list[str] = []
    wt = withdraw_today_by_name or {}
    for r in rows:
        pname = str(r.get("participant_name", ""))
        name = html.escape(pname)
        did = int(r["dasher_id"]) if pd.notna(r.get("dasher_id")) else 0
        inf = float(r.get("total_deposits") or 0)
        ouf = float(r.get("jar_outflow") or 0)
        net = inf - ouf
        jar_bal = r.get("end_balance")

        inf_html = (
            f'<span class="money-in">{html.escape("+" + fmt_money(inf))}</span>'
            if inf > 0
            else f'<span class="money-zero">{fmt_money(inf)}</span>'
        )
        if ouf > 0:
            out_html = f'<span class="money-out">{html.escape("−$" + fmt_money(ouf)[1:])}</span>'
        else:
            out_html = f'<span class="money-zero">{fmt_money(0.0)}</span>'
        tw = float(wt.get(pname, 0.0))
        if tw >= LARGE_WITHDRAWAL_TODAY_USD:
            tip = html.escape(f"Large withdrawal today: {fmt_money(tw)}")
            out_html += f'<span class="outflow-warn" title="{tip}">&#9888;</span>'

        net_class = "money-net-zero"
        if net > 0.005:
            net_class = "money-net-pos"
        elif net < -0.005:
            net_class = "money-net-neg"
        net_html = f'<span class="{net_class}">{html.escape(fmt_net_cell(net))}</span>'

        bal_html = fmt_money(jar_bal)
        alloc_html = allocation_cell_html(
            events_df,
            did,
            r.get("allocation_start_of_wk"),
            r.get("allocation_end_of_wk"),
            event_start=event_start,
            event_end=event_end,
        )

        flags = participant_row_flags(r)
        flags_html = " ".join(
            f'<span class="flag">{html.escape(f)}</span>' for f in flags
        )

        paydown_aria = html.escape(f"Debt paydown for {pname}")
        paydown_cell = (
            f'<td class="col-paydown">'
            f'<input type="checkbox" class="paydown-cb" data-dasher-id="{did}" '
            f'aria-label="{paydown_aria}" />'
            f"</td>"
        )

        body.append(
            "<tr>"
            f'<td class="col-name">{name}</td>'
            f'<td class="col-pct">{alloc_html}</td>'
            f'<td class="col-num">{inf_html}</td>'
            f'<td class="col-num">{out_html}</td>'
            f'<td class="col-num">{net_html}</td>'
            f'<td class="col-num col-bal">{html.escape(bal_html)}</td>'
            f"{paydown_cell}"
            f'<td class="col-flags">{flags_html}</td>'
            "</tr>"
        )

    thead = (
        "<thead><tr>"
        "<th>Participant</th>"
        "<th>Jar %</th>"
        "<th>Inflow</th>"
        "<th>Outflow</th>"
        "<th>Net</th>"
        "<th>Jar bal.</th>"
        "<th>Paydown</th>"
        "<th>Flags</th>"
        "</tr></thead>"
    )
    return (
        f'<div class="trend-table-wrap"><table class="trend-table trend-table--participants">'
        f"{thead}<tbody>{''.join(body)}</tbody></table></div>"
    )


def compact_kpi_row_html(n_alloc: int, n_contrib: int, n_cohort: int) -> str:
    da = "On track" if n_alloc >= HEALTH_THRESHOLD else "Below threshold"
    dc = "On track" if n_contrib >= HEALTH_THRESHOLD else "Below threshold"

    def cell(label: str, value: str, sub: str, mc_class: str) -> str:
        return (
            f'<div class="mc {mc_class}">'
            f'<div class="mc-label">{html.escape(label)}</div>'
            f'<div class="mc-count-md">{value}</div>'
            f'<div class="mc-meta">{html.escape(sub)}</div></div>'
        )

    return (
        '<div class="ga-row">'
        + cell("Allocation on", html.escape(f"{n_alloc} / {n_cohort}"), da, "mc-entries")
        + cell("Jar contributors", html.escape(f"{n_contrib} / {n_cohort}"), dc, "mc-entries")
        + "</div>"
    )


def render_savings_jar_table(
    wk: pd.DataFrame,
    week_context: str,
    *,
    withdraw_today_by_name: dict[str, float] | None = None,
) -> str:
    rows: list[dict] = []
    for _, r in wk.iterrows():
        ouf = float(r.get("jar_outflow") or 0)
        inf = float(r.get("total_deposits") or 0)
        net = inf - ouf
        rows.append(
            {
                "participant_name": str(r.get("participant_name", "")),
                "inf": inf,
                "ouf": ouf,
                "net": net,
                "sj_pct": r.get("allocation_end_of_wk"),
                "start_pct": r.get("allocation_start_of_wk"),
            }
        )

    rows.sort(key=lambda x: (-float(x["ouf"]), str(x["participant_name"]).lower()))

    body: list[str] = []
    gray = DD_GRAY_600
    wt = withdraw_today_by_name or {}
    for row in rows:
        pname = row["participant_name"]
        name = html.escape(pname)
        inf = row["inf"]
        ouf = row["ouf"]
        net = row["net"]
        inf_color = gray if inf == 0 else CHART_GREEN_MUTED
        out_color = gray if ouf == 0 else CHART_RED_MUTED
        net_color = gray if net == 0 else (CHART_GREEN_MUTED if net > 0 else CHART_RED_MUTED)
        out_cell = f"−{fmt_money(ouf)}" if ouf > 0 else fmt_money(0.0)
        tw = float(wt.get(pname, 0.0))
        if tw >= LARGE_WITHDRAWAL_TODAY_USD:
            tip = html.escape(f"Large withdrawal today: {fmt_money(tw)}")
            out_cell += f'<span class="outflow-warn" title="{tip}">&#9888;</span>'
        arrow_frag, pct_color = allocation_change_block(
            row["start_pct"], row["sj_pct"], week_context
        )
        pct_cell = f'<span style="color:{pct_color};font-weight:700;">{html.escape(pct_to_display(row["sj_pct"]))}</span>'
        if arrow_frag:
            pct_cell += arrow_frag
        body.append(
            "<tr>"
            f"<td style=\"color:{DD_BLACK};\">{name}</td>"
            f'<td style="text-align:right;">{pct_cell}</td>'
            f"<td style=\"text-align:right;font-weight:600;color:{inf_color};\">{fmt_money(inf)}</td>"
            f"<td style=\"text-align:right;font-weight:600;color:{out_color};\">{out_cell}</td>"
            f"<td style=\"text-align:right;font-weight:700;color:{net_color};\">{fmt_net_cell(net)}</td>"
            "</tr>"
        )

    thead = (
        "<thead><tr>"
        "<th>Participant</th>"
        "<th>Savings jar %</th>"
        "<th>Inflows</th>"
        "<th>Outflows</th>"
        "<th>Net</th>"
        "</tr></thead>"
    )
    return (
        f'<div class="trend-table-wrap"><table class="trend-table">{thead}<tbody>{"".join(body)}</tbody></table></div>'
    )


def attainment_chart_payload(wk: pd.DataFrame) -> dict:
    """Rows by hours_attainment_pct ascending (dashboard sorts for display: high → low top-down)."""
    if wk.empty or "hours_attainment_pct" not in wk.columns:
        return {
            "labels": [],
            "pcts": [],
            "hours": [],
            "goals": [],
            "xMax": 100,
            "chartHeight": 420,
        }
    wk = wk.sort_values("hours_attainment_pct", ascending=True)
    names = wk["participant_name"].astype(str).tolist()
    pcts_raw = wk["hours_attainment_pct"].fillna(0).astype(float).tolist()
    pcts = [_fin(min(max(x, 0.0), 500.0)) for x in pcts_raw]
    hours = [_fin(x) for x in wk["hours_dashed"].fillna(0).astype(float).tolist()]
    goals = [_fin(x) for x in wk["recommended_dashing_hours"].fillna(0).astype(float).tolist()]
    m = max(pcts) if pcts else 0.0
    x_max = float(max(100, int(math.ceil(max(m, 100.0) / 50.0)) * 50))
    n = len(names)
    chart_h = int(max(280, min(560, 20 * n + 64)))
    return {
        "labels": names,
        "pcts": pcts,
        "hours": hours,
        "goals": goals,
        "xMax": x_max,
        "chartHeight": chart_h,
    }
