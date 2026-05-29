"""Step 3: Flatten raw athlete profile JSON into two analysis DataFrames.

Outputs:
  data/athletes_prs.csv      — one row per athlete, PR columns (features)
  data/season_results.csv    — one row per (athlete × meet × event) (training data)
"""

from __future__ import annotations

import json
from pathlib import Path

import dagster as dg
import pandas as pd


def _load_profiles(profiles_dir: Path) -> list[dict]:
    profiles = []
    for p in profiles_dir.glob("*.json"):
        try:
            profiles.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            pass
    return profiles


@dg.asset(
    group_name="data_processing",
    description="Flatten cached athlete JSON profiles into athletes_prs.csv and season_results.csv.",
    deps=["athlete_profiles"],
)
def flattened_dataframes() -> dict[str, pd.DataFrame]:
    logger = dg.get_dagster_logger()

    gender_map = {}
    for csv_path in Path("data/qualifying_athletes").glob("*.csv"):
        df_q = pd.read_csv(csv_path, dtype=str)
        for _, row in df_q.iterrows():
            gender_map[str(row["athlete_id"])] = row["gender"]

    profiles_dir = Path("data/profiles")
    profiles = _load_profiles(profiles_dir)
    logger.info(f"Loaded {len(profiles)} athlete profiles")

    pr_rows: list[dict] = []
    result_rows: list[dict] = []

    for raw in profiles:
        profile = raw.get("data") or {}
        athlete_id = profile.get("athlete_id", "")
        name = profile.get("name", "")
        school = profile.get("school", "")
        gender = gender_map.get(athlete_id, "")
        prs = profile.get("prs", {})

        pr_row = {"athlete_id": athlete_id, "name": name, "school": school, "gender": gender}
        pr_row.update({f"pr_{event.replace(' ', '_')}": mark for event, mark in prs.items()})
        pr_rows.append(pr_row)

        for season_entry in profile.get("meets", []):
            meet = season_entry.get("meet", "")
            date = season_entry.get("date", "")
            for result in season_entry.get("results", []):
                result_rows.append(
                    {
                        "athlete_id": athlete_id,
                        "name": name,
                        "school": school,
                        "gender": gender,
                        "meet": meet,
                        "date": date,
                        "event": result.get("event", ""),
                        "mark": result.get("mark", ""),
                        "place": result.get("place", ""),
                    }
                )

    df_prs = pd.DataFrame(pr_rows)
    df_results = pd.DataFrame(result_rows)

    out = Path("data/flattened_dataframes")
    out.mkdir(parents=True, exist_ok=True)
    df_prs.to_csv(out / "athletes_prs.csv", index=False)
    df_results.to_csv(out / "season_results.csv", index=False)

    logger.info(f"athletes_prs.csv: {len(df_prs)} rows")
    logger.info(f"season_results.csv: {len(df_results)} rows")

    return {"athletes_prs": df_prs, "season_results": df_results}


assets = [flattened_dataframes]
