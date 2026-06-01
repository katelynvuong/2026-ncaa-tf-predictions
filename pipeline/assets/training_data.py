"""ML training data pipeline.

Assets:
  championship_results  — scrape 2022–2025 NCAA championship final placements
  historical_profiles   — fetch TFRRS profiles for any athlete missing from data/profiles/
  training_features     — compute the same 5 features per (athlete, event, year)
  training_dataset      — join results + features into one ML-ready CSV
"""

from __future__ import annotations

import html as html_lib
import json
import re
import time
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from bs4 import BeautifulSoup

from sports_skills.xctf import get_athlete_profile
from pipeline.assets.features import (
    _filter_results,
    _filter_results_for_place,
    _season_best,
    _season_avg,
    _avg_place,
    _conf_champ_place,
    _conf_champ_place_any_event,
    _cross_event_avg_place,
    _pr_feature,
    _RELAY_EVENTS,
)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_CHAMP_URLS = {
    2025: "https://tf.tfrrs.org/results/92668/NCAA_Division_I_Outdoor_Track__Field_Championships",
    2024: "https://tf.tfrrs.org/results/87017/NCAA_Division_I_Outdoor_Track__Field_Championships",
    2023: "https://tf.tfrrs.org/results/81300/NCAA_Division_I_Outdoor_Track__Field_Championships",
    2022: "https://tf.tfrrs.org/results/75224/NCAA_Division_I_Outdoor_Track__Field_Championships",
}

# URL slug (after stripping Mens-/Womens- prefix, lowercased) → standard event key
_SLUG_EVENT_MAP = {
    "100-meters":        "100",
    "200-meters":        "200",
    "400-meters":        "400",
    "800-meters":        "800",
    "1500-meters":       "1500",
    "5000-meters":       "5000",
    "10000-meters":      "10k",
    "110-hurdles":       "110H",
    "100-hurdles":       "100H",
    "400-hurdles":       "400H",
    "3000-steeplechase": "3000S",
    "4-x-100-relay":     "4x100",
    "4-x-400-relay":     "4x400",
    "high-jump":         "HJ",
    "pole-vault":        "PV",
    "long-jump":         "LJ",
    "triple-jump":       "TJ",
    "shot-put":          "SP",
    "discus":            "DT",
    "hammer":            "HT",
    "javelin":           "JT",
}

_SKIP_SLUGS = {"decathlon", "heptathlon"}
_SCORING_POINTS = {"10", "8", "6", "5", "4", "3", "2", "1"}
_ATHLETE_ID_RE = re.compile(r"/athletes/(\d+)/")
_MEET_ID_RE = re.compile(r"/results/(\d+)/")


def _get(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": _UA}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _event_links(index_url: str) -> list[tuple[str, str, str]]:
    """Return (event_url, event_key, gender) for every scoreable event."""
    soup = BeautifulSoup(_get(index_url), "html.parser")
    meet_id = _MEET_ID_RE.search(index_url).group(1)
    seen: set[str] = set()
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Match event result links: /results/{meet_id}/{event_id}/.../{slug}
        if not re.search(rf"/results/{meet_id}/\d+/", href):
            continue
        slug = href.rstrip("/").split("/")[-1]
        if slug in seen:
            continue
        seen.add(slug)

        # Determine gender and strip prefix
        slug_lower = slug.lower()
        if slug_lower.startswith("mens-"):
            gender = "M"
            event_slug = slug_lower[len("mens-"):]
        elif slug_lower.startswith("womens-"):
            gender = "W"
            event_slug = slug_lower[len("womens-"):]
        else:
            continue

        if any(skip in event_slug for skip in _SKIP_SLUGS):
            continue
        if "para" in event_slug:
            continue

        event_key = _SLUG_EVENT_MAP.get(event_slug)
        if not event_key:
            continue

        full_url = href if href.startswith("http") else f"https://tf.tfrrs.org{href}"
        links.append((full_url, event_key, gender))

    return links


def _parse_results(event_url: str, event_key: str, gender: str, year: int) -> list[dict]:
    """Extract final-round placements from a championship event page."""
    soup = BeautifulSoup(_get(event_url), "html.parser")
    is_relay = event_key in _RELAY_EVENTS
    rows = []

    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        # Filter to final-round scorers via points column
        points = tds[-1].get_text(strip=True)
        if points not in _SCORING_POINTS:
            continue

        place_text = tds[0].get_text(strip=True)
        try:
            place = int(place_text)
        except ValueError:
            continue

        mark = tds[4].get_text(strip=True)

        athlete_id = ""
        athlete_name = ""
        school = ""

        athlete_link = tds[1].find("a", href=True)
        if athlete_link:
            id_match = _ATHLETE_ID_RE.search(athlete_link["href"])
            if id_match and not is_relay:
                athlete_id = id_match.group(1)
                athlete_name = athlete_link.get_text(strip=True)

        school_link = tds[3].find("a", href=True)
        if school_link:
            school = school_link.get_text(strip=True)

        rows.append({
            "year":         year,
            "event":        event_key,
            "gender":       gender,
            "place":        place,
            "athlete_id":   athlete_id,
            "athlete_name": athlete_name,
            "school":       school,
            "mark":         mark,
        })

    # Field events show each attempt as a separate row — keep only the best place per athlete
    seen_athletes: set[str] = set()
    deduped = []
    for row in sorted(rows, key=lambda r: r["place"]):
        key = row["athlete_id"] or row["school"]
        if key not in seen_athletes:
            seen_athletes.add(key)
            deduped.append(row)
    return deduped


@dg.asset(
    group_name="ml",
    description="Scrape NCAA championship final placements (2022–2025) from TFRRS.",
)
def championship_results() -> pd.DataFrame:
    logger = dg.get_dagster_logger()
    all_rows: list[dict] = []

    for year, index_url in _CHAMP_URLS.items():
        events = _event_links(index_url)
        logger.info(f"{year}: {len(events)} events found")

        for event_url, event_key, gender in events:
            try:
                rows = _parse_results(event_url, event_key, gender, year)
                logger.info(f"  {year} {gender} {event_key}: {len(rows)} finalists")
                all_rows.extend(rows)
            except Exception as exc:
                logger.warning(f"  {event_url}: failed — {exc}")

    df = pd.DataFrame(all_rows)
    out = Path("data/training/championship_results.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info(f"Saved {len(df)} rows → {out}")
    return df


@dg.asset(
    group_name="ml",
    deps=["championship_results"],
    description="Fetch TFRRS profiles for championship athletes not already in data/profiles/.",
)
def historical_profiles(context) -> None:
    champ = pd.read_csv("data/training/championship_results.csv", dtype=str)
    profiles_dir = Path("data/profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # Only individual athletes (relays have no athlete_id)
    athletes = champ.dropna(subset=["athlete_id"]).drop_duplicates("athlete_id")
    athletes = athletes[athletes["athlete_id"] != ""]

    saved = skipped = errors = 0
    for _, row in athletes.iterrows():
        athlete_id = str(row["athlete_id"])
        if (profiles_dir / f"{athlete_id}.json").exists():
            skipped += 1
            continue

        # Reconstruct name slug from "First Last" → "Last_First"
        parts = str(row["athlete_name"]).strip().split()
        name_slug = f"{parts[-1]}_{parts[0]}" if len(parts) >= 2 else parts[0]
        school_slug = str(row["school"]).replace(" ", "_")

        result = get_athlete_profile(
            athlete_id=athlete_id,
            school=school_slug,
            name=name_slug,
        )

        if isinstance(result, dict) and result.get("error"):
            context.log.warning(f"Error {athlete_id} {name_slug}: {result.get('message')}")
            errors += 1
        else:
            (profiles_dir / f"{athlete_id}.json").write_text(json.dumps(result, indent=2))
            saved += 1

        time.sleep(1)

    context.log.info(f"Done: {saved} saved, {skipped} skipped, {errors} errors")


@dg.asset(
    group_name="ml",
    deps=["historical_profiles"],
    description="Compute the 5 training features per (athlete, event, year) using season results filtered to that year.",
)
def training_features() -> pd.DataFrame:
    logger = dg.get_dagster_logger()

    champ = pd.read_csv("data/training/championship_results.csv", dtype=str)
    champ = champ[champ["athlete_id"].notna() & (champ["athlete_id"] != "")]

    # Load all profiles and build a combined season_results for all historical athletes
    from pipeline.assets.dataframes import _load_profiles
    profiles = _load_profiles(Path("data/profiles"))
    logger.info(f"Loaded {len(profiles)} profiles")

    result_rows = []
    for raw in profiles:
        profile = raw.get("data") or {}
        athlete_id = profile.get("athlete_id", "")
        name = profile.get("name", "")
        school = profile.get("school", "")
        for season_entry in profile.get("meets", []):
            meet = season_entry.get("meet", "")
            date = season_entry.get("date", "")
            for result in season_entry.get("results", []):
                result_rows.append({
                    "athlete_id": str(athlete_id),
                    "name": name,
                    "school": school,
                    "meet": meet,
                    "date": date,
                    "event": result.get("event", ""),
                    "mark": result.get("mark", ""),
                    "place": result.get("place", ""),
                })

    all_results = pd.DataFrame(result_rows)
    logger.info(f"Built results table: {len(all_results)} rows across all profiles")

    prs = pd.read_csv("data/flattened_dataframes/athletes_prs.csv", dtype=str)

    feature_rows = []
    for year, year_group in champ.groupby("year"):
        year_results_marks = _filter_results(all_results, year=int(year))
        year_results_place = _filter_results_for_place(all_results, year=int(year))
        logger.info(f"{year}: {len(year_results_marks)} mark rows, {len(year_results_place)} place rows")

        all_year_ids = year_group[~year_group["event"].isin(_RELAY_EVENTS)]["athlete_id"].dropna().unique().tolist()
        year_cross_ap       = _cross_event_avg_place(year_results_place, all_year_ids)
        year_conf_champ_any = _conf_champ_place_any_event(year_results_place, all_year_ids)

        for event, event_group in year_group.groupby("event"):
            if event in _RELAY_EVENTS:
                continue
            athlete_ids = event_group["athlete_id"].tolist()
            sb  = _season_best(year_results_marks, event, athlete_ids)
            avg = _season_avg(year_results_marks, event, athlete_ids)
            ap  = _avg_place(year_results_place, event, athlete_ids)
            cp  = _conf_champ_place(year_results_place, event, athlete_ids)
            pr  = _pr_feature(prs, event, athlete_ids)

            for _, row in event_group.iterrows():
                aid = row["athlete_id"]
                feature_rows.append({
                    "year":              year,
                    "athlete_id":        aid,
                    "athlete_name":      row["athlete_name"],
                    "school":            row["school"],
                    "event":             event,
                    "gender":            row["gender"],
                    "season_best":       sb.get(aid),
                    "season_avg":        avg.get(aid),
                    "avg_place":         ap.get(aid),
                    "conf_champ_place":       cp.get(aid),
                    "pr":                     pr.get(aid),
                    "cross_event_avg_place":      year_cross_ap.get(aid),
                    "conf_champ_place_any_event": year_conf_champ_any.get(aid),
                })

    df = pd.DataFrame(feature_rows)
    out = Path("data/training/training_features.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info(f"Saved {len(df)} feature rows → {out}")
    return df


@dg.asset(
    group_name="ml",
    deps=["championship_results", "training_features"],
    description="Join championship placements with features to produce the final ML training dataset.",
)
def training_dataset() -> pd.DataFrame:
    logger = dg.get_dagster_logger()

    champ = pd.read_csv("data/training/championship_results.csv", dtype=str)
    feats = pd.read_csv("data/training/training_features.csv", dtype=str)

    # Convert numeric columns
    champ["place"] = pd.to_numeric(champ["place"], errors="coerce")
    for col in ["season_best", "season_avg", "avg_place", "conf_champ_place", "pr",
                "cross_event_avg_place", "conf_champ_place_any_event"]:
        feats[col] = pd.to_numeric(feats[col], errors="coerce")

    df = champ.merge(
        feats[["year", "athlete_id", "event", "season_best", "season_avg",
               "avg_place", "conf_champ_place", "pr",
               "cross_event_avg_place", "conf_champ_place_any_event"]],
        on=["year", "athlete_id", "event"],
        how="left",
    )

    out = Path("data/training/training_dataset.csv")
    df.to_csv(out, index=False)

    logger.info(f"Training dataset: {len(df)} rows")
    logger.info(f"Feature fill rates:")
    for col in ["season_best", "season_avg", "avg_place", "conf_champ_place", "pr", "cross_event_avg_place"]:
        n = df[col].notna().sum()
        logger.info(f"  {col}: {n}/{len(df)} ({100*n/len(df):.1f}%)")
    return df


assets = [championship_results, historical_profiles, training_features, training_dataset]
