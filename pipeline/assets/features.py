"""Feature engineering for NCAA T&F championship placement prediction.

Built on top of final_athletes, season_results, and athletes_prs.
Each feature is added as a column to a base DataFrame keyed on
(athlete_id, event) — one row per athlete-event combination at nationals.
Relay events (4x100, 4x400) are included as single rows with NaN features.
"""

from __future__ import annotations

import re
from pathlib import Path

import dagster as dg
import pandas as pd

# Events where wind readings appear and marks > +2.0 m/s are wind-illegal
_WIND_EVENTS = {"100", "200", "100H", "110H", "LJ", "TJ"}

# Field events where higher mark = better (everyone else: lower = better)
_FIELD_EVENTS = {"HJ", "PV", "LJ", "TJ", "SP", "DT", "HT", "JT"}

# Relay events — no individual athlete features
_RELAY_EVENTS = {"4x100", "4x400"}

# Map our standard event key → athletes_prs.csv column name
_EVENT_PR_COL = {
    "100":   "pr_100",
    "200":   "pr_200",
    "400":   "pr_400",
    "800":   "pr_800",
    "1500":  "pr_1500",
    "3000S": "pr_3000S",
    "5000":  "pr_5000",
    "10k":   "pr_10,000",
    "110H":  "pr_110H",
    "100H":  "pr_100H",
    "400H":  "pr_400H",
    "HJ":    "pr_HJ",
    "PV":    "pr_PV",
    "LJ":    "pr_LJ",
    "TJ":    "pr_TJ",
    "SP":    "pr_SP",
    "DT":    "pr_DT",
    "HT":    "pr_HT",
    "JT":    "pr_JT",
}

# TFRRS uses inconsistent names for some events — map aliases to our standard key
_EVENT_ALIASES = {
    "10,000": "10k",
}


def _extract_wind(mark: str) -> float | None:
    m = re.search(r"\(([+-]?\d+\.?\d*)\)", mark)
    return float(m.group(1)) if m else None


def _strip_wind(mark: str) -> str:
    return re.sub(r"\s*\([^)]*\)", "", mark).strip()


def _to_seconds(mark: str) -> float | None:
    mark = _strip_wind(mark).strip()
    if not mark or re.search(r"[a-df-z]", mark, re.IGNORECASE):
        return None
    try:
        if ":" in mark:
            parts = mark.split(":")
            return sum(float(p) * 60 ** (len(parts) - 1 - i) for i, p in enumerate(parts))
        return float(mark)
    except ValueError:
        return None


def _to_meters(mark: str) -> float | None:
    m = re.search(r"(\d+\.?\d*)m", mark)
    if m:
        return float(m.group(1))
    bare = _strip_wind(mark).strip()
    try:
        return float(bare)
    except ValueError:
        return None


def _parse_mark(mark: str, event: str) -> float | None:
    if pd.isna(mark):
        return None
    return _to_meters(mark) if event in _FIELD_EVENTS else _to_seconds(mark)


def _is_wind_legal(mark: str, event: str) -> bool:
    if event not in _WIND_EVENTS:
        return True
    wind = _extract_wind(mark)
    return wind is None or wind <= 2.0


def _filter_results(results: pd.DataFrame, year: int = 2026) -> pd.DataFrame:
    """Year + wind filter for mark-based features (season_best, season_avg, pr)."""
    year_mask = results["date"].str.contains(str(year), na=False)
    wind_mask = results.apply(
        lambda r: _is_wind_legal(str(r["mark"]), str(r["event"])), axis=1
    )
    df = results[year_mask & wind_mask].copy()
    df["event"] = df["event"].replace(_EVENT_ALIASES)
    return df


def _filter_results_for_place(results: pd.DataFrame, year: int = 2026) -> pd.DataFrame:
    """Year-only filter for place-based features — wind legality is irrelevant for place."""
    df = results[results["date"].str.contains(str(year), na=False)].copy()
    df["event"] = df["event"].replace(_EVENT_ALIASES)
    return df


def _season_best(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    ev = results[results["event"] == event].copy()
    ev["numeric"] = pd.to_numeric(ev["mark"].apply(lambda m: _parse_mark(m, event)), errors="coerce")
    ev = ev.dropna(subset=["numeric"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]
    if event in _FIELD_EVENTS:
        return ev.groupby("athlete_id")["numeric"].max().reindex(athlete_ids)
    return ev.groupby("athlete_id")["numeric"].min().reindex(athlete_ids)


def _season_avg(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    ev = results[results["event"] == event].copy()
    ev["numeric"] = pd.to_numeric(ev["mark"].apply(lambda m: _parse_mark(m, event)), errors="coerce")
    ev = ev.dropna(subset=["numeric"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]
    return ev.groupby("athlete_id")["numeric"].mean().reindex(athlete_ids)


def _cross_event_avg_place(results: pd.DataFrame, athlete_ids: list[str]) -> pd.Series:
    """Average finals place across ALL events for each athlete — captures general competitive level."""
    ev = results[results["place"].str.contains(r"\(F\)", na=False)].copy()
    ev["place_num"] = ev["place"].str.extract(r"^(\d+)").astype(float)
    ev = ev.dropna(subset=["place_num"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]
    return ev.groupby("athlete_id")["place_num"].mean().reindex(athlete_ids)


# Events with a single round at regionals — no separate final, so (P) is the only result
_SINGLE_ROUND_EVENTS = {"5000", "10k"}


def _avg_place(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    ev_f = results[(results["event"] == event) & results["place"].str.contains(r"\(F\)", na=False)].copy()
    ev_f["place_num"] = ev_f["place"].str.extract(r"^(\d+)").astype(float)
    ev_f = ev_f.dropna(subset=["place_num"])
    ev_f = ev_f[ev_f["athlete_id"].isin(athlete_ids)]
    avg = ev_f.groupby("athlete_id")["place_num"].mean().reindex(athlete_ids)

    if event in _SINGLE_ROUND_EVENTS:
        missing = avg[avg.isna()].index.tolist()
        if missing:
            ev_p = results[(results["event"] == event) & results["place"].str.contains(r"\(P\)", na=False)].copy()
            ev_p["place_num"] = ev_p["place"].str.extract(r"^(\d+)").astype(float)
            ev_p = ev_p.dropna(subset=["place_num"])
            ev_p = ev_p[ev_p["athlete_id"].isin(missing)]
            avg_p = ev_p.groupby("athlete_id")["place_num"].mean()
            avg = avg.fillna(avg_p)

    return avg


def _pr_feature(prs: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    """Return all-time PR for each athlete in the given event, parsed to numeric."""
    pr_col = _EVENT_PR_COL.get(event)
    if not pr_col or pr_col not in prs.columns:
        return pd.Series(index=athlete_ids, dtype=float)
    sub = prs.set_index("athlete_id")[pr_col].reindex(athlete_ids)
    return pd.to_numeric(
        sub.apply(lambda m: _parse_mark(m, event) if pd.notna(m) else None),
        errors="coerce",
    )


_CONF_CHAMP_RE = re.compile(r"outdoor.*champ|champ.*outdoor", re.IGNORECASE)


def _conf_champ_place(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    conf = results[
        results["meet"].str.contains(_CONF_CHAMP_RE, na=False)
        & results["place"].str.contains(r"\(F\)", na=False)
        & (results["event"] == event)
        & results["athlete_id"].isin(athlete_ids)
    ].copy()
    conf["place_num"] = conf["place"].str.extract(r"^(\d+)").astype(float)
    conf = conf.dropna(subset=["place_num"])
    return conf.groupby("athlete_id")["place_num"].min().reindex(athlete_ids)


@dg.asset(
    group_name="data_processing",
    deps=["final_athletes", "flattened_dataframes"],
    description="Engineer features for each (athlete, event) at nationals.",
)
def features() -> pd.DataFrame:
    logger = dg.get_dagster_logger()

    final = pd.read_csv("data/final_athletes.csv", dtype=str)
    results = pd.read_csv("data/flattened_dataframes/season_results.csv", dtype=str)
    prs = pd.read_csv("data/flattened_dataframes/athletes_prs.csv", dtype=str)

    results_marks = _filter_results(results)
    results_place = _filter_results_for_place(results)
    logger.info(f"Filtered results: {len(results_marks)} rows (mark-based), {len(results_place)} rows (place-based)")

    # Compute cross-event avg place once across all individual athletes
    all_individual_ids = final[~final["event"].isin(_RELAY_EVENTS)]["athlete_id"].dropna().unique().tolist()
    cross_ap = _cross_event_avg_place(results_place, all_individual_ids)

    rows = []
    for event, group in final.groupby("event"):
        is_relay = event in _RELAY_EVENTS

        if is_relay:
            # One row per relay team — no individual features
            for _, athlete in group.iterrows():
                rows.append({
                    "athlete_id": None,
                    "athlete_name": None,
                    "school": athlete["school"],
                    "event": event,
                    "gender": athlete["gender"],
                    "region": athlete["region"],
                    "qualifier": athlete["qualifier"],
                    "season_best": None,
                    "season_avg": None,
                    "avg_place": None,
                    "conf_champ_place": None,
                    "cross_event_avg_place": None,
                })
            continue

        athlete_ids = group["athlete_id"].dropna().tolist()
        sb  = _season_best(results_marks, event, athlete_ids)
        avg = _season_avg(results_marks, event, athlete_ids)
        ap  = _avg_place(results_place, event, athlete_ids)
        cp  = _conf_champ_place(results_place, event, athlete_ids)
        pr  = _pr_feature(prs, event, athlete_ids)

        for _, athlete in group.iterrows():
            aid = athlete["athlete_id"]
            rows.append({
                "athlete_id": aid,
                "athlete_name": athlete["athlete_name"],
                "school": athlete["school"],
                "event": event,
                "gender": athlete["gender"],
                "region": athlete["region"],
                "qualifier": athlete["qualifier"],
                "season_best": sb.get(aid),
                "season_avg": avg.get(aid),
                "avg_place": ap.get(aid),
                "conf_champ_place": cp.get(aid),
                "pr": pr.get(aid),
                "cross_event_avg_place": cross_ap.get(aid),
            })

    df = pd.DataFrame(rows)
    out = Path("data/features.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    total = len(df)
    for col in ["season_best", "season_avg", "avg_place", "conf_champ_place"]:
        n = df[col].notna().sum()
        logger.info(f"{col}: {n}/{total} ({100*n/total:.1f}%)")
    return df


assets = [features]
