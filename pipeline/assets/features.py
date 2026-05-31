"""Feature engineering for NCAA T&F championship placement prediction.

Built on top of final_athletes, season_results, and athletes_prs.
Each feature is added as a column to a base DataFrame keyed on
(athlete_id, event) — one row per athlete-event combination at nationals.
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


def _extract_wind(mark: str) -> float | None:
    """Return wind reading from a mark string, or None if absent."""
    m = re.search(r"\(([+-]?\d+\.?\d*)\)", mark)
    return float(m.group(1)) if m else None


def _strip_wind(mark: str) -> str:
    return re.sub(r"\s*\([^)]*\)", "", mark).strip()


def _to_seconds(mark: str) -> float | None:
    """Convert a track mark string to total seconds. Returns None if unparseable."""
    mark = _strip_wind(mark).strip()
    if not mark or re.search(r"[a-df-z]", mark, re.IGNORECASE):
        # Contains letters other than 'e' (scientific notation) → DNF/DNS/DQ/etc.
        return None
    try:
        if ":" in mark:
            parts = mark.split(":")
            return sum(float(p) * 60 ** (len(parts) - 1 - i) for i, p in enumerate(parts))
        return float(mark)
    except ValueError:
        return None


def _to_meters(mark: str) -> float | None:
    """Extract metric distance/height from a field event mark. Returns None if unparseable."""
    m = re.search(r"(\d+\.?\d*)m", mark)
    if m:
        return float(m.group(1))
    # Bare number with no unit (rare) — try direct float
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
    """Return False for wind-aided marks (> +2.0 m/s) in wind-sensitive events."""
    if event not in _WIND_EVENTS:
        return True
    wind = _extract_wind(mark)
    return wind is None or wind <= 2.0


def _filter_results(results: pd.DataFrame) -> pd.DataFrame:
    """Keep only 2026 results with legal wind."""
    year_mask = results["date"].str.contains("2026", na=False)
    wind_mask = results.apply(
        lambda r: _is_wind_legal(str(r["mark"]), str(r["event"])), axis=1
    )
    return results[year_mask & wind_mask].copy()


def _season_best(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    """
    For each athlete_id, return their season best numeric mark in the given event.
    Lower is better for track; higher is better for field.
    """
    ev = results[results["event"] == event].copy()
    ev["numeric"] = pd.to_numeric(ev["mark"].apply(lambda m: _parse_mark(m, event)), errors="coerce")
    ev = ev.dropna(subset=["numeric"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]

    if event in _FIELD_EVENTS:
        best = ev.groupby("athlete_id")["numeric"].max()
    else:
        best = ev.groupby("athlete_id")["numeric"].min()

    return best.reindex(athlete_ids)


def _avg_place(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    """Average final-round place across all 2026 results in the event."""
    ev = results[(results["event"] == event) & results["place"].str.contains(r"\(F\)", na=False)].copy()
    ev["place_num"] = ev["place"].str.extract(r"^(\d+)").astype(float)
    ev = ev.dropna(subset=["place_num"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]
    return ev.groupby("athlete_id")["place_num"].mean().reindex(athlete_ids)


_CONF_CHAMP_RE = re.compile(r"outdoor.*champ|champ.*outdoor", re.IGNORECASE)


def _conf_champ_place(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    """Place in the outdoor conference championship final for the given event."""
    conf = results[
        results["meet"].str.contains(_CONF_CHAMP_RE, na=False)
        & results["place"].str.contains(r"\(F\)", na=False)
        & (results["event"] == event)
        & results["athlete_id"].isin(athlete_ids)
    ].copy()
    conf["place_num"] = conf["place"].str.extract(r"^(\d+)").astype(float)
    conf = conf.dropna(subset=["place_num"])
    # One conference champ per athlete — take best place in case of duplicate rows
    return conf.groupby("athlete_id")["place_num"].min().reindex(athlete_ids)


def _season_avg(results: pd.DataFrame, event: str, athlete_ids: list[str]) -> pd.Series:
    """Average 2026 mark across all completed (non-DNF/DQ/DNS) results in the event."""
    ev = results[results["event"] == event].copy()
    ev["numeric"] = pd.to_numeric(ev["mark"].apply(lambda m: _parse_mark(m, event)), errors="coerce")
    ev = ev.dropna(subset=["numeric"])
    ev = ev[ev["athlete_id"].isin(athlete_ids)]
    return ev.groupby("athlete_id")["numeric"].mean().reindex(athlete_ids)


@dg.asset(
    group_name="data_processing",
    deps=["final_athletes", "flattened_dataframes"],
    description="Engineer features for each (athlete, event) at nationals.",
)
def features() -> pd.DataFrame:
    logger = dg.get_dagster_logger()

    final = pd.read_csv("data/final_athletes.csv", dtype=str)
    results = pd.read_csv("data/flattened_dataframes/season_results.csv", dtype=str)

    results = _filter_results(results)
    logger.info(f"Filtered results: {len(results)} rows (2026, wind-legal)")

    rows = []
    for event, group in final.groupby("event"):
        athlete_ids = group["athlete_id"].tolist()
        sb = _season_best(results, event, athlete_ids)
        avg = _season_avg(results, event, athlete_ids)
        ap = _avg_place(results, event, athlete_ids)
        cp = _conf_champ_place(results, event, athlete_ids)

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
                "is_auto_qualifier": int(athlete["qualifier"] == "Q"),
            })

    df = pd.DataFrame(rows)
    out = Path("data/features.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    total = len(df)
    populated = df["season_best"].notna().sum()
    logger.info(f"season_best populated: {populated}/{total} ({100*populated/total:.1f}%)")
    return df


assets = [features]
