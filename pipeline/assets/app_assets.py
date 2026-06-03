"""Dagster app group — prepares JSON data files for the web frontend.

Assets:
  app_metrics            → data/app/metrics.json
  app_team_standings     → data/app/team_standings.json
  app_event_predictions  → data/app/event_predictions.json
  frontend_build         → builds the React app (app/frontend/dist/)
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import dagster as dg
import pandas as pd

_APP_DATA = Path("app/frontend/public/data")

_EVENT_ORDER = [
    "100", "200", "400", "800", "1500", "3000S", "5000", "10k",
    "110H", "100H", "400H",
    "HJ", "PV", "LJ", "TJ", "SP", "DT", "HT", "JT",
    "4x100", "4x400",
]

_EVENT_CATEGORY = {
    "100": "sprints", "200": "sprints", "400": "sprints",
    "100H": "sprints", "110H": "sprints", "400H": "sprints",
    "800": "distance", "1500": "distance", "3000S": "distance",
    "5000": "distance", "10k": "distance",
    "HJ": "field", "PV": "field", "LJ": "field", "TJ": "field",
    "SP": "field", "DT": "field", "HT": "field", "JT": "field",
    "4x100": "relays", "4x400": "relays",
}

_EVENT_LABELS = {
    "100": "100m", "200": "200m", "400": "400m", "800": "800m",
    "1500": "1500m", "3000S": "3000m SC", "5000": "5000m", "10k": "10,000m",
    "110H": "110m H", "100H": "100m H", "400H": "400m H",
    "4x100": "4x100m Relay", "4x400": "4x400m Relay",
    "HJ": "High Jump", "PV": "Pole Vault", "LJ": "Long Jump", "TJ": "Triple Jump",
    "SP": "Shot Put", "DT": "Discus", "HT": "Hammer", "JT": "Javelin",
}

_SCORING_MAP = {1: 10, 2: 8, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}

# School name → ESPN CDN logo ID
_ESPN_ID = {
    "Alabama": 333, "Arkansas": 8, "Auburn": 2, "Baylor": 239,
    "BYU": 252, "Clemson": 228, "Colorado": 38, "Colorado St.": 36,
    "Duke": 150, "East Carolina": 151, "Florida": 57, "Florida State": 52,
    "Georgetown": 46, "Georgia": 61, "Georgia Tech": 59, "Houston": 248,
    "Iowa": 2294, "Iowa State": 66, "Kansas": 2305, "Kansas State": 2306,
    "Kentucky": 96, "LSU": 99, "Louisville": 97, "Michigan": 130,
    "Michigan State": 127, "Minnesota": 135, "Miss State": 344,
    "Nebraska": 158, "NC State": 152, "North Carolina": 153,
    "Notre Dame": 87, "Ohio State": 194, "Oklahoma": 201,
    "Oklahoma State": 197, "Ole Miss": 145, "Oregon": 2483,
    "Penn State": 213, "Princeton": 163, "Purdue": 2509,
    "South Carolina": 2579, "Stanford": 24, "Syracuse": 183,
    "Tennessee": 2633, "Texas": 251, "Texas A&M": 245, "Texas Tech": 2641,
    "UCLA": 26, "USC": 30, "Virginia": 258, "Virginia Tech": 259,
    "Washington": 264, "Washington St.": 265, "Wisconsin": 275,
    "Utah": 254, "Arizona": 12, "Arizona State": 9, "Harvard": 108,
    "Penn": 219, "Columbia": 171, "Yale": 43, "Brown": 225,
    "Indiana": 84, "Maryland": 120, "Northwestern": 77, "Illinois": 356,
    "Miami (Fla.)": 2390, "Pittsburgh": 221, "California": 25,
    "Oregon State": 204, "Akron": 2006, "Northern Arizona": 2464,
}


def _logo_url(school: str) -> str | None:
    espn_id = _ESPN_ID.get(school)
    return f"https://a.espncdn.com/i/teamlogos/ncaa/500/{espn_id}.png" if espn_id else None


@dg.asset(
    group_name="app",
    deps=["predictions"],
    description="Export model validation metrics to data/app/metrics.json.",
)
def app_metrics() -> None:
    src = Path("data/predictions/metrics.json")
    if not src.exists():
        raise FileNotFoundError("data/predictions/metrics.json not found — re-materialize predictions first.")

    raw = json.loads(src.read_text())
    train = pd.read_csv("data/training/training_dataset.csv", dtype=str)
    years = sorted(train["year"].dropna().unique().tolist())

    output = {
        **raw,
        "training_rows":  int(train["place"].notna().sum()),
        "years_trained":  [int(y) for y in years],
        "relay_excluded": False,
        "events_count":   int(train["event"].nunique()),
        "model_type":     "XGBoost (Gradient Boosted Trees)",
        "model_note":     "Gradient boosted decision trees trained on historical NCAA championship results (2022–2025). Predicts finishing place for each athlete based on their 2026 season performance features.",
    }

    _APP_DATA.mkdir(parents=True, exist_ok=True)
    (_APP_DATA / "metrics.json").write_text(json.dumps(output, indent=2))


@dg.asset(
    group_name="app",
    deps=["predictions"],
    description="Build top-3 team standings with individual scorer breakdowns for tooltip display.",
)
def app_team_standings() -> None:
    preds = pd.read_csv("data/predictions/predictions.csv", dtype=str)
    scores = pd.read_csv("data/predictions/team_scores.csv", dtype=str)
    scores["total_points"]   = pd.to_numeric(scores["total_points"],   errors="coerce")
    scores["predicted_rank"] = pd.to_numeric(scores["predicted_rank"], errors="coerce")
    preds["predicted_rank"]  = pd.to_numeric(preds["predicted_rank"],  errors="coerce")

    preds["points"] = preds["predicted_rank"].map(_SCORING_MAP).fillna(0)

    result = []
    for gender in ["M", "W"]:
        top3 = scores[scores["gender"] == gender].nsmallest(3, "predicted_rank")
        for _, row in top3.iterrows():
            school = row["school"]
            school_preds = (
                preds[(preds["school"] == school) & (preds["gender"] == gender) & (preds["points"] > 0)]
                .sort_values("points", ascending=False)
            )
            scorers = [
                {
                    "event":        _EVENT_LABELS.get(r["event"], r["event"]),
                    "athlete_name": r["athlete_name"] if pd.notna(r["athlete_name"]) else None,
                    "points":       int(r["points"]),
                }
                for _, r in school_preds.iterrows()
            ]
            result.append({
                "gender":       gender,
                "rank":         int(row["predicted_rank"]),
                "school":       school,
                "logo_url":     _logo_url(school),
                "total_points": float(row["total_points"]),
                "scorers":      scorers,
            })

    _APP_DATA.mkdir(parents=True, exist_ok=True)
    (_APP_DATA / "team_standings.json").write_text(json.dumps(result, indent=2))


@dg.asset(
    group_name="app",
    deps=["predictions"],
    description="Structure per-event top-8 predictions for the frontend and save to data/app/event_predictions.json.",
)
def app_event_predictions() -> None:
    preds = pd.read_csv("data/predictions/predictions.csv", dtype=str)
    preds["predicted_rank"]  = pd.to_numeric(preds["predicted_rank"],  errors="coerce")
    preds["predicted_place"] = pd.to_numeric(preds["predicted_place"], errors="coerce")

    events_data: dict = {}
    for (event, gender), grp in preds.groupby(["event", "gender"]):
        grp = grp.sort_values("predicted_rank")
        athletes = [
            {
                "rank":            int(row["predicted_rank"]),
                "bar_height":      9 - int(row["predicted_rank"]),
                "athlete_name":    row["athlete_name"] if pd.notna(row["athlete_name"]) else None,
                "school":          row["school"],
                "predicted_place": round(float(row["predicted_place"]), 3),
            }
            for _, row in grp.iterrows()
        ]
        if event not in events_data:
            events_data[event] = {
                "event":    event,
                "label":    _EVENT_LABELS.get(event, event),
                "category": _EVENT_CATEGORY.get(event, "other"),
                "M": [], "W": [],
            }
        events_data[event][gender] = athletes

    ordered = sorted(
        events_data.values(),
        key=lambda e: _EVENT_ORDER.index(e["event"]) if e["event"] in _EVENT_ORDER else 99,
    )

    _APP_DATA.mkdir(parents=True, exist_ok=True)
    (_APP_DATA / "event_predictions.json").write_text(json.dumps(ordered, indent=2))


_FEATURE_LABELS = {
    "avg_place":                  "Avg Place",
    "conf_champ_place":           "Conf Champ Place",
    "conf_champ_place_any_event": "Conf Champ (Any Event)",
    "cross_event_avg_place":      "Cross-Event Avg Place",
    "pr":                         "Personal Record",
    "season_best":                "Season Best",
    "season_avg":                 "Season Avg",
    "relay_qualifying_time":      "Relay Qualifying Time",
    "relay_season_best":          "Relay Season Best",
    "relay_qualifying_place":     "Relay Qualifying Place",
}


@dg.asset(
    group_name="app",
    deps=["predictions"],
    description="Compute analytics: feature importances, team category breakdown, regional split.",
)
def app_analytics() -> None:
    preds = pd.read_csv("data/predictions/predictions.csv", dtype=str)
    preds["predicted_rank"] = pd.to_numeric(preds["predicted_rank"], errors="coerce")
    preds["points"] = preds["predicted_rank"].map(_SCORING_MAP).fillna(0)

    # Feature importances
    fi_path = Path("data/predictions/feature_importances.json")
    feature_importances = []
    if fi_path.exists():
        raw = json.loads(fi_path.read_text())
        feature_importances = [
            {"feature": r["feature"], "importance": r["importance"],
             "label": _FEATURE_LABELS.get(r["feature"], r["feature"])}
            for r in raw
        ]

    # Team category breakdown (top 3 per gender)
    scores = pd.read_csv("data/predictions/team_scores.csv", dtype=str)
    scores["predicted_rank"] = pd.to_numeric(scores["predicted_rank"], errors="coerce")
    preds["category"] = preds["event"].map(_EVENT_CATEGORY).fillna("other")

    team_breakdown = []
    for gender in ["M", "W"]:
        top3 = scores[scores["gender"] == gender].nsmallest(3, "predicted_rank")
        for _, row in top3.iterrows():
            school = row["school"]
            sp = preds[(preds["school"] == school) & (preds["gender"] == gender)]
            cat_pts = sp.groupby("category")["points"].sum().to_dict()
            team_breakdown.append({
                "gender":       gender,
                "rank":         int(row["predicted_rank"]),
                "school":       school,
                "logo_url":     _logo_url(school),
                "total_points": float(row["total_points"]),
                "sprints":      float(cat_pts.get("sprints", 0)),
                "distance":     float(cat_pts.get("distance", 0)),
                "field":        float(cat_pts.get("field", 0)),
                "relays":       float(cat_pts.get("relays", 0)),
            })

    # Regional split — count of top-8 predicted athletes by region (all events, both genders)
    ind = preds[~preds["event"].isin(["4x100", "4x400"])]
    # region is already in predictions.csv — no join needed
    east = int(ind[ind["region"] == "east"].shape[0])
    west = int(ind[ind["region"] == "west"].shape[0])
    regional_split = {"east": east, "west": west, "total": east + west}

    output = {
        "feature_importances": feature_importances,
        "team_category_breakdown": team_breakdown,
        "regional_split": regional_split,
    }

    _APP_DATA.mkdir(parents=True, exist_ok=True)
    (_APP_DATA / "analytics.json").write_text(json.dumps(output, indent=2))


@dg.asset(
    group_name="app",
    deps=["app_metrics", "app_team_standings", "app_event_predictions", "app_analytics"],
    description="Build the React frontend (npm run build) so it picks up the latest data.",
)
def frontend_build(context) -> None:
    frontend_dir = Path(__file__).parents[2] / "app" / "frontend"
    if not frontend_dir.exists():
        context.log.warning(f"Frontend directory not found at {frontend_dir} — skipping build.")
        return

    context.log.info("Installing npm dependencies...")
    subprocess.run(["npm", "install"], cwd=frontend_dir, check=True)

    context.log.info("Building React app...")
    subprocess.run(["npm", "run", "build"], cwd=frontend_dir, check=True)
    context.log.info(f"Build complete → {frontend_dir / 'dist'}")


assets = [app_metrics, app_team_standings, app_event_predictions, app_analytics, frontend_build]
