#!/usr/bin/env python3
"""
Generate static dashboard: dashboard/index.html reads dashboard/data.json.

  .venv/bin/python build_dashboard.py           # Snowflake if no cache; else .pilot_cache (skips if SQL newer)
  .venv/bin/python build_dashboard.py --live    # always Snowflake + overwrite .pilot_cache/

Open via HTTP (e.g. python serve_dashboard.py); the UI fetches data.js with cache-busting.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pilot_data import (
    SQL_ACTIVITY,
    SQL_ALLOCATION_EVENTS,
    SQL_COHORT,
    WEEK_TOTAL_KEY,
    PILOT_MIN_WEEK_START,
    aggregate_by_dasher,
    attainment_chart_payload,
    cohort_size,
    compact_kpi_row_html,
    count_health_metrics,
    filter_eligible_weeks,
    fmt_week_label,
    load_allocation_cache,
    load_data_cache,
    load_sql,
    monday_of,
    prepare_activity_df,
    prepare_allocation_events_df,
    prepare_pilot_df,
    render_participant_master_table,
    run_query,
    save_data_cache,
    today_manual_withdraw_by_participant,
    build_daily_activity,
    clear_data_cache_files,
    data_cache_stale_vs_sql,
    read_snowflake_refreshed_at,
)
from snowflake_client import ROOT
from theme import CHART_GREEN_MUTED, CHART_LINE_TEAL, CHART_RED_MUTED, DD_GRAY_200, DD_GRAY_600

DASH_DIR = ROOT / "dashboard"
DATA_JSON = DASH_DIR / "data.json"
DATA_JS = DASH_DIR / "data.js"


def theme_payload() -> dict:
    """Light shell + chart colors (match dashboard.css)."""
    return {
        "teal": "#45818e",
        "tealDark": "#2e5f6a",
        "tealLight": "#c8e0e5",
        "bg": "#f3f7f8",
        "ink": "#1e2d31",
        "muted": "#7e9299",
        "border": "#d6e5e9",
        "chartGreen": CHART_GREEN_MUTED,
        "chartRed": CHART_RED_MUTED,
        "chartTeal": CHART_LINE_TEAL,
        "grid": "#f0f2f4",
        "refLine": DD_GRAY_200,
        "tickMuted": DD_GRAY_600,
    }


def load_frames(*, force_live: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    if force_live:
        clear_data_cache_files()
        cohort = run_query(load_sql(SQL_COHORT))
        activity = run_query(load_sql(SQL_ACTIVITY))
        alloc_raw = (
            run_query(load_sql(SQL_ALLOCATION_EVENTS))
            if SQL_ALLOCATION_EVENTS.is_file()
            else pd.DataFrame()
        )
        alloc_df = prepare_allocation_events_df(alloc_raw)
        save_data_cache(cohort, activity, alloc_df)
        return cohort, activity, alloc_df, False
    cached = load_data_cache()
    stale_vs_sql = data_cache_stale_vs_sql()
    if cached is not None and not stale_vs_sql:
        alloc = load_allocation_cache()
        return cached[0], cached[1], prepare_allocation_events_df(alloc), True
    if cached is not None and stale_vs_sql:
        print(
            "SQL file(s) newer than .pilot_cache — re-querying Snowflake (use --live to always refresh).",
            flush=True,
        )
    cohort = run_query(load_sql(SQL_COHORT))
    activity = run_query(load_sql(SQL_ACTIVITY))
    alloc_raw = (
        run_query(load_sql(SQL_ALLOCATION_EVENTS)) if SQL_ALLOCATION_EVENTS.is_file() else pd.DataFrame()
    )
    a_df = prepare_allocation_events_df(alloc_raw)
    save_data_cache(cohort, activity, a_df)
    return cohort, activity, a_df, False


def main() -> None:
    ap = argparse.ArgumentParser(description="Build dashboard/data.json for static HTML dashboard.")
    ap.add_argument(
        "--live",
        action="store_true",
        help="Always re-query Snowflake (use when warehouse data changed but SQL files did not)",
    )
    args = ap.parse_args()

    DASH_DIR.mkdir(parents=True, exist_ok=True)

    if not SQL_COHORT.is_file() or not SQL_ACTIVITY.is_file():
        raise SystemExit("Missing sql/pilot_dasher_weeks.sql or sql/pilot_activity.sql")

    cohort_raw, activity_raw, events_df, _ = load_frames(force_live=args.live)
    if cohort_raw is None or cohort_raw.empty:
        raise SystemExit("No cohort data returned from Snowflake.")

    df = prepare_pilot_df(cohort_raw)
    df = filter_eligible_weeks(df)
    if df.empty or "week_start" not in df.columns:
        raise SystemExit("No rows in allowed week range.")

    adf = activity_raw if activity_raw is not None else pd.DataFrame()

    weeks_sorted = sorted({pd.Timestamp(x).normalize() for x in df["week_start"].dropna()})
    today = pd.Timestamp.now().normalize()
    this_monday = monday_of(today).normalize()
    if this_monday in weeks_sorted:
        default_i = weeks_sorted.index(this_monday)
    else:
        past = [w for w in weeks_sorted if pd.Timestamp(w).normalize() <= this_monday]
        default_i = weeks_sorted.index(past[-1]) if past else len(weeks_sorted) - 1

    week_keys = [w.strftime("%Y-%m-%d") for w in weeks_sorted] + [WEEK_TOTAL_KEY]

    def week_label(k: str) -> str:
        if k == WEEK_TOTAL_KEY:
            return "Total"
        return fmt_week_label(pd.Timestamp(k))

    pnames: list[str] = []
    if not adf.empty:
        pnames = sorted(prepare_activity_df(adf)["participant_name"].dropna().unique().tolist())

    participants = ["All participants"] + pnames
    withdraw_today = today_manual_withdraw_by_participant(adf)

    n_cohort = cohort_size(df)
    views: dict = {}

    for sel_key in week_keys:
        is_total = sel_key == WEEK_TOTAL_KEY
        if is_total:
            wk_view = aggregate_by_dasher(df)
            week_context = "Total"
            n_alloc, n_contrib = count_health_metrics(df, is_total_aggregate=True)
            act_week = None
            total_mode_chart = True
        else:
            selected_week = pd.Timestamp(sel_key).normalize()
            _ws = pd.to_datetime(df["week_start"]).dt.normalize()
            wk_view = df.loc[_ws == selected_week].copy()
            if wk_view.empty:
                continue
            week_context = f"Week of {fmt_week_label(selected_week)}"
            n_alloc, n_contrib = count_health_metrics(wk_view, is_total_aggregate=False)
            act_week = selected_week
            total_mode_chart = False

        kpi_html = compact_kpi_row_html(n_alloc, n_contrib, n_cohort)

        if is_total:
            ev_start = pd.Timestamp(PILOT_MIN_WEEK_START).normalize()
            ev_end = today + pd.Timedelta(days=1)
        else:
            ev_start = pd.Timestamp(sel_key).normalize()
            ev_end = ev_start + pd.Timedelta(days=7)

        participant_html = render_participant_master_table(
            wk_view,
            events_df,
            event_start=ev_start,
            event_end=ev_end,
            withdraw_today_by_name=withdraw_today,
        )
        att = attainment_chart_payload(wk_view)

        flows_daily: dict = {}
        for pname in participants:
            labels, ins, outs = build_daily_activity(
                adf,
                week_start=act_week,
                participant=pname,
                total_mode=total_mode_chart,
            )
            flows_daily[pname] = {
                "labels": labels,
                "ins": ins,
                "outs": outs,
            }

        # Min card height ≈ attainment chart + header/hint/padding (avoid huge empty band).
        attain_h = int(att.get("chartHeight") or 280)
        card_min = max(260, attain_h + 120)
        views[sel_key] = {
            "week_context": week_context,
            "kpi_html": kpi_html,
            "participant_table_html": participant_html,
            "attainment": att,
            "flows": flows_daily,
            "card_min_px": card_min,
        }

    week_options = [{"key": k, "label": week_label(k)} for k in week_keys if k in views]
    default_key = week_keys[min(default_i, len(week_keys) - 1)]
    if default_key not in views:
        default_key = week_options[0]["key"] if week_options else default_key

    payload = {
        "title": "Debt Paydown Pilot Participant Tracker",
        "theme": theme_payload(),
        "week_options": week_options,
        "default_week_key": default_key,
        "participants": participants,
        "cohort_size": n_cohort,
        "views": views,
    }
    sf_at = read_snowflake_refreshed_at()
    if sf_at:
        payload["snowflake_refreshed_at"] = sf_at

    json_str = json.dumps(payload, indent=2, default=str)
    DATA_JSON.write_text(json_str, encoding="utf-8")
    DATA_JS.write_text(
        "window.__PILOT_DASHBOARD__ = " + json_str + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {DATA_JSON.resolve()}")
    print(f"Wrote {DATA_JS.resolve()}")
    print("Preview: python serve_dashboard.py  (data.js is loaded over HTTP, not as file://)")
    print("—" * 52)
    if sf_at:
        print(f"snowflake_refreshed_at: {sf_at}")
    else:
        print("snowflake_refreshed_at: (none — no .pilot_cache/meta.json)")
    print("—" * 52)


if __name__ == "__main__":
    main()
