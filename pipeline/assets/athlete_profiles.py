"""Step 2: Fetch full TFRRS athlete profiles via sports-skills xctf connector.

Reads qualifying_athletes for the current partition, fetches each profile, saves
raw JSON to data/profiles/{athlete_id}.json. Already-saved profiles are skipped
so the asset is safely re-runnable after partial failures.

Rate limit: 1 req/sec enforced inside sports_skills.xctf._connector.
Estimated time per partition (~900 athletes): ~15–20 min.
"""

from __future__ import annotations

import html
import json
import time
from pathlib import Path

import dagster as dg
import pandas as pd

from sports_skills.xctf import get_athlete_profile

from pipeline.partitions import region_gender_partitions


@dg.asset(
    group_name="data_collection",
    description="Bulk-fetch TFRRS athlete profiles and cache raw JSON to disk.",
    partitions_def=region_gender_partitions,
)
def athlete_profiles(context, qualifying_athletes: pd.DataFrame) -> None:
    profiles_dir = Path("data/profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    total = len(qualifying_athletes)
    saved = skipped = errors = 0

    for i, row in qualifying_athletes.iterrows():
        athlete_id = str(row["athlete_id"])
        out_path = profiles_dir / f"{athlete_id}.json"

        if out_path.exists() and json.loads(out_path.read_text()).get("status"):
            skipped += 1
            continue

        result = get_athlete_profile(
            athlete_id=athlete_id,
            school=html.unescape(row["school_slug"]),
            name=html.unescape(row["name_slug"]).removesuffix(".html"),
        )

        if isinstance(result, dict) and result.get("error"):
            context.log.warning(f"[{i}/{total}] Error for {athlete_id}: {result.get('message')}")
            errors += 1
            time.sleep(1)
            continue

        out_path.write_text(json.dumps(result, indent=2))
        saved += 1

        if saved % 50 == 0:
            context.log.info(
                f"Progress: {saved} saved, {skipped} skipped, {errors} errors / {total} total"
            )

    context.log.info(f"Done: {saved} saved, {skipped} skipped, {errors} errors")


@dg.asset(
    group_name="data_collection",
    deps=["final_athletes"],
    description=(
        "Fetch TFRRS profiles for any individual athlete in final_athletes "
        "who wasn't in the qualifying lists and is missing a profile JSON."
    ),
)
def supplemental_profiles(context) -> None:
    final = pd.read_csv("data/final_athletes.csv", dtype=str)
    profiles_dir = Path("data/profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # Relay rows have no athlete_id — skip them
    candidates = final.dropna(subset=["athlete_id", "tfrrs_url"])
    candidates = candidates[candidates["tfrrs_url"].str.startswith("http", na=False)]

    saved = skipped = errors = 0
    for _, row in candidates.iterrows():
        athlete_id = str(row["athlete_id"])
        out_path = profiles_dir / f"{athlete_id}.json"

        if out_path.exists():
            skipped += 1
            continue

        # Parse school_slug and name_slug from the stored TFRRS URL
        # URL format: https://www.tfrrs.org/athletes/{id}/{school_slug}/{name_slug}
        parts = row["tfrrs_url"].rstrip("/").split("/")
        school_slug = parts[-2]
        name_slug = parts[-1]

        result = get_athlete_profile(
            athlete_id=athlete_id,
            school=school_slug,
            name=name_slug,
        )

        if isinstance(result, dict) and result.get("error"):
            context.log.warning(f"Error for {athlete_id} {name_slug}: {result.get('message')}")
            errors += 1
            time.sleep(1)
            continue

        out_path.write_text(json.dumps(result, indent=2))
        context.log.info(f"Saved {athlete_id} ({name_slug})")
        saved += 1
        time.sleep(1)

    context.log.info(f"Done: {saved} saved, {skipped} skipped, {errors} errors")


assets = [athlete_profiles, supplemental_profiles]
