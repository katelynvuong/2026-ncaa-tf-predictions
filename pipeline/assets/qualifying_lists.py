"""Step 1: Scrape athlete slugs from the four TFRRS qualifying lists.

List IDs:
  5622 = East  (men + women)
  5623 = West  (men + women)

Each list page has athlete profile links in the form:
  https://www.tfrrs.org/athletes/{athlete_id}/{School}/{First_Last}
"""

from __future__ import annotations

import re
from pathlib import Path

import dagster as dg
import pandas as pd
import requests

from pipeline.partitions import region_gender_partitions

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_LIST_IDS = {"east": "5622", "west": "5623"}

_ATHLETE_URL_RE = re.compile(
    r'href="(https://www\.tfrrs\.org/athletes/(\d+)/([^/"]+)/([^/"]+))"'
)


def _fetch_list_html(list_id: str, gender: str) -> str:
    url = f"https://www.tfrrs.org/lists/{list_id}?gender={gender}"
    resp = requests.get(url, headers={"User-Agent": _UA}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_athletes(html: str, region: str, gender: str) -> list[dict]:
    seen = set()
    rows = []
    for match in _ATHLETE_URL_RE.finditer(html):
        url, athlete_id, school_slug, name_slug = match.groups()
        if athlete_id in seen:
            continue
        seen.add(athlete_id)
        rows.append(
            {
                "athlete_id": athlete_id,
                "school_slug": school_slug,
                "name_slug": name_slug,
                "gender": gender,
                "region": region,
                "tfrrs_url": url,
            }
        )
    return rows


@dg.asset(
    group_name="data_collection",
    description="Scrape athlete slugs from a single TFRRS D1 Outdoor qualifying list.",
    partitions_def=region_gender_partitions,
)
def qualifying_athletes(context) -> pd.DataFrame:
    region, gender = context.partition_key.split("_")
    list_id = _LIST_IDS[region]

    context.log.info(f"Fetching {region} {gender} list (id={list_id})")
    html = _fetch_list_html(list_id, gender)
    rows = _parse_athletes(html, region, gender)
    context.log.info(f"  → {len(rows)} unique athletes")

    df = pd.DataFrame(rows)
    out = Path(f"data/qualifying_athletes_{context.partition_key}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    context.log.info(f"Saved {len(df)} athletes to {out}")
    return df


assets = [qualifying_athletes]
