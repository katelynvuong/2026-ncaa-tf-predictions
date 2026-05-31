"""Scrape NCAA regional first-round qualifiers from Flash Results.

Discovers all compiled event pages from both East and West index pages,
then extracts every athlete/team marked Q or q (auto-qualifier or
time qualifier). athlete_id is extracted directly from the TFRRS URL
embedded in each Flash Results row (stats-href attribute).

Output columns: athlete_id, athlete_name, school, event, gender, region, qualifier
"""

from __future__ import annotations

import re
from pathlib import Path

import dagster as dg
import pandas as pd
import requests
from bs4 import BeautifulSoup

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_REGIONS = {
    "east": "https://flashresults.ncaa.com/Outdoor/2026/FirstRounds/East/",
    "west": "https://flashresults.ncaa.com/Outdoor/2026/FirstRounds/West/",
}

_CLASS_YEAR_RE = re.compile(r"\s*\[(?:FR|SO|JR|SR|5Y|GR)\]\s*", re.IGNORECASE)
_TFRRS_ID_RE = re.compile(r"/athletes/(\d+)/")

# Map Flash Results <title> → TFRRS event key used in season_results.csv
_EVENT_MAP = {
    "100 m":               "100",
    "200 m":               "200",
    "400 m":               "400",
    "800 m":               "800",
    "1500 m":              "1500",
    "3000 m steeple":      "3000S",
    "3000 m steeplechase": "3000S",
    "5000 m":              "5000",
    "10000 m":             "10k",
    "110 m hurdles":       "110H",
    "100 m hurdles":       "100H",
    "400 m hurdles":       "400H",
    "4x100 m relay":       "4x100",
    "4x400 m relay":       "4x400",
    "high jump":           "HJ",
    "pole vault":          "PV",
    "long jump":           "LJ",
    "triple jump":         "TJ",
    "shot put":            "SP",
    "discus":              "DT",
    "discus throw":        "DT",
    "hammer":              "HT",
    "hammer throw":        "HT",
    "javelin":             "JT",
    "javelin throw":       "JT",
}

_TRAILING_WORDS = re.compile(r"\s+(dash|run|sprint)$", re.IGNORECASE)


def _normalize_event(title: str) -> str:
    lower = re.sub(r"^(men|women)\s+", "", title.strip(), flags=re.IGNORECASE).lower()
    if lower in _EVENT_MAP:
        return _EVENT_MAP[lower]
    stripped = _TRAILING_WORDS.sub("", lower)
    return _EVENT_MAP.get(stripped, title)


def _get(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": _UA}, timeout=30)
    resp.raise_for_status()
    return resp.text


_COMPILED_RE = re.compile(r"^(\d+)-(\d+)_compiled\.htm$")


def _event_links(base_url: str) -> list[str]:
    """Return one URL per event — the highest round number for each base event."""
    soup = BeautifulSoup(_get(base_url + "index.htm"), "html.parser")
    best: dict[str, tuple[int, str]] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = _COMPILED_RE.match(href)
        if not m:
            continue
        base, round_num = m.group(1), int(m.group(2))
        if base not in best or round_num > best[base][0]:
            best[base] = (round_num, href)
    return [base_url + href for _, (_, href) in sorted(best.items())]


def _parse_event(url: str, region: str) -> list[dict]:
    try:
        html = _get(url)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return []
        raise

    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title") or soup.find("h1")
    raw_title = title_tag.get_text(strip=True) if title_tag else url.rsplit("/", 1)[-1]
    gender = "W" if raw_title.lower().startswith("women") else "M"
    is_relay = "relay" in raw_title.lower()
    event_name = _normalize_event(raw_title)

    rows = []
    for tr in soup.find_all("tr"):
        qual_span = tr.find("span", class_="q_qual")
        if not qual_span:
            continue
        qualifier = qual_span.get_text(strip=True)
        if qualifier not in ("Q", "q"):
            continue

        athlete_id = ""
        athlete_name = ""
        school = ""

        tfrrs_url = ""
        a_tag = tr.find("a", attrs={"stats-name": True})
        if a_tag:
            # Extract athlete_id from the embedded TFRRS URL
            stats_href = a_tag.get("stats-href", "")
            id_match = _TFRRS_ID_RE.search(stats_href)
            if id_match:
                athlete_id = id_match.group(1)
                tfrrs_url = stats_href.strip()

            parts = a_tag["stats-name"].split("|", 1)
            if not is_relay:
                athlete_name = parts[0].strip()
            school = parts[1].strip() if len(parts) > 1 else ""
        else:
            b_tag = tr.find("b")
            small_tag = tr.find("small")
            if not is_relay and b_tag:
                athlete_name = b_tag.get_text(strip=True)
            if small_tag:
                school = _CLASS_YEAR_RE.sub("", small_tag.get_text(strip=True)).strip()

        if not school and not athlete_name:
            continue

        rows.append(
            {
                "athlete_id": athlete_id,
                "athlete_name": athlete_name,
                "school": school,
                "event": event_name,
                "gender": gender,
                "region": region,
                "qualifier": qualifier,
                "tfrrs_url": tfrrs_url,
            }
        )

    return rows


@dg.asset(
    group_name="data_collection",
    description=(
        "Scrape NCAA regional Q/q qualifiers from Flash Results (East + West). "
        "athlete_id is extracted directly from the embedded TFRRS URL in each row, "
        "matching the IDs in athlete_profiles and regional_athletes."
    ),
)
def final_athletes() -> pd.DataFrame:
    logger = dg.get_dagster_logger()
    all_rows: list[dict] = []

    for region, base_url in _REGIONS.items():
        try:
            links = _event_links(base_url)
        except Exception as exc:
            logger.warning(f"{region}: index fetch failed — {exc}")
            continue
        logger.info(f"{region}: {len(links)} event pages found")

        for url in links:
            try:
                rows = _parse_event(url, region)
                if rows:
                    logger.info(f"  {url.rsplit('/', 1)[-1]}: {len(rows)} qualifiers")
                all_rows.extend(rows)
            except Exception as exc:
                logger.warning(f"  {url}: failed — {exc}")

    df = pd.DataFrame(
        all_rows,
        columns=["athlete_id", "athlete_name", "school", "event", "gender", "region", "qualifier", "tfrrs_url"],
    )

    out = Path("data/final_athletes.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info(f"Saved {len(df)} total qualifiers → {out}")
    logger.info(f"  Athletes with matched athlete_id: {df['athlete_id'].ne('').sum()}")
    return df


assets = [final_athletes]
