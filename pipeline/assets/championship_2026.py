"""2026 NCAA Championship Results Pipeline.

Fetches 2026 final results from flashresults.com using same logic as training data.

Assets:
  championship_results_2026   — final placements and individual results
  championship_actual_teams   — actual aggregate team scores
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

_CHAMP_BASE = "https://www.flashresults.com/2026_Meets/Outdoor/06-10_NCAA"
_RELAY_EVENTS = {"4x100", "4x400"}
_SCORING_POINTS = {"10", "8", "6", "5", "4", "3", "2", "1"}
_WIND_FIELD_EVENTS = {"LJ", "TJ"}

_ATHLETE_ID_RE = re.compile(r"/athletes/(\d+)/")
_CLASS_YEAR_RE = re.compile(r"\s*\[(?:FR|SO|JR|SR|5Y|GR)\]\s*", re.IGNORECASE)

# URL slug → standardized event key
_SLUG_EVENT_MAP = {
    "100-meters": "100",
    "200-meters": "200",
    "400-meters": "400",
    "800-meters": "800",
    "1500-meters": "1500",
    "5000-meters": "5000",
    "10000-meters": "10k",
    "110-hurdles": "110H",
    "100-hurdles": "100H",
    "400-hurdles": "400H",
    "3000-steeplechase": "3000S",
    "4-x-100-relay": "4x100",
    "4-x-400-relay": "4x400",
    "high-jump": "HJ",
    "pole-vault": "PV",
    "long-jump": "LJ",
    "triple-jump": "TJ",
    "shot-put": "SP",
    "discus": "DT",
    "hammer": "HT",
    "javelin": "JT",
}

_SKIP_SLUGS = {"decathlon", "heptathlon"}


def _get(url: str) -> str:
    """Fetch URL with user agent."""
    resp = requests.get(url, headers={"User-Agent": _UA}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _event_links(index_url: str) -> list[tuple[str, str, str]]:
    """Return (event_url, event_key, gender) for every scoreable event from flashresults FINALS.

    Note: Track/relay events use -2_compiled.htm (finals only).
    Field events use -1_compiled.htm (we filter to top-8 by place).
    """
    soup = BeautifulSoup(_get(index_url), "html.parser")
    seen: set[str] = set()
    links = []

    # Events that only have -1_compiled.htm pages or whose -2 pages are blocked
    # (Field events + distance events whose finals pages are inaccessible)
    FIELD_EVENTS = {"HJ", "PV", "LJ", "TJ", "SP", "DT", "HT", "JT", "5000", "10k"}

    # Find all direct links to event result pages (e.g., "001-1_compiled.htm")
    all_links = soup.find_all("a", href=True)

    for a in all_links:
        href = a.get("href", "")

        # Match compiled result pages like "033-1_compiled.htm"
        if not ("-1_compiled.htm" in href):
            continue

        event_name = a.get_text(strip=True)

        # Skip if no text or already seen
        if not event_name or event_name in seen:
            continue
        seen.add(event_name)

        # Skip special events
        if any(skip in event_name.lower() for skip in _SKIP_SLUGS):
            continue
        if "wheelchair" in event_name.lower() or "para" in event_name.lower():
            continue

        # Determine gender
        if event_name.startswith("Men"):
            gender = "M"
            event_name_clean = event_name[3:].strip()
        elif event_name.startswith("Women"):
            gender = "W"
            event_name_clean = event_name[5:].strip()
        else:
            continue

        # Map event name to key
        event_key = _map_event_name(event_name_clean)
        if not event_key:
            continue

        # For track/relay events, use -2_compiled.htm (finals).
        # For field events, use -1_compiled.htm (filter to top-8).
        if event_key not in FIELD_EVENTS:
            href = href.replace("-1_compiled.htm", "-2_compiled.htm")

        full_url = href if href.startswith("http") else f"{_CHAMP_BASE}/{href}"
        links.append((full_url, event_key, gender))

    return links


def _map_event_name(name: str) -> str | None:
    """Map flashresults event name to standardized event key."""
    name = name.lower().strip()

    # Standard distances
    if "100 m" in name and "hurdle" not in name and "relay" not in name:
        return "100"
    elif "200 m" in name and "relay" not in name:
        return "200"
    elif "400 m" in name and "hurdle" not in name and "relay" not in name:
        return "400"
    elif "800 m" in name:
        return "800"
    elif "1500 m" in name:
        return "1500"
    elif "5000 m" in name:
        return "5000"
    elif "10000 m" in name or "10,000 m" in name:
        return "10k"

    # Hurdles
    elif "110 m hurdle" in name:
        return "110H"
    elif "100 m hurdle" in name:
        return "100H"
    elif "400 m hurdle" in name:
        return "400H"

    # Steeplechase
    elif "3000 m steeple" in name:
        return "3000S"

    # Relays
    elif "4x100" in name or "4 x 100" in name:
        return "4x100"
    elif "4x400" in name or "4 x 400" in name:
        return "4x400"

    # Field events
    elif "high jump" in name:
        return "HJ"
    elif "pole vault" in name:
        return "PV"
    elif "long jump" in name:
        return "LJ"
    elif "triple jump" in name or "triple-jump" in name:
        return "TJ"
    elif "shot put" in name:
        return "SP"
    elif "discus" in name:
        return "DT"
    elif "hammer" in name:
        return "HT"
    elif "javelin" in name:
        return "JT"

    return None


def _parse_results(event_url: str, event_key: str, gender: str, year: int) -> list[dict]:
    """Extract final-round placements from flashresults championship event page.

    Flashresults format:
    - td[0]: place (1, 2, 3, ...)
    - td[1]: empty
    - td[2]: athlete/team name (with school + year embedded)
    - td[3]: time/mark
    - td[4+]: other data (wind, splits, etc.)
    """
    soup = BeautifulSoup(_get(event_url), "html.parser")
    is_relay = event_key in _RELAY_EVENTS
    rows = []

    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        # Try to extract place from first column
        place_text = tds[0].get_text(strip=True)
        try:
            place = int(place_text)
        except ValueError:
            continue  # Skip rows without numeric place

        # Only keep top 8 finishers (scoring places)
        if place > 8:
            continue

        athlete_name = ""
        school = ""
        mark = tds[3].get_text(strip=True) if len(tds) > 3 else ""

        if is_relay:
            # For relays: td[2] contains format like "AUBURN1Auburn" or "LSU2LSU"
            # Extract the proper-case school name (always at the end after digits)
            team_text = tds[2].get_text(strip=True)
            # Strategy: split on the last occurrence of digits and take what comes after
            # This handles both "Auburn1Auburn" and "N. CAROLINA A&T17N. Carolina A&T"
            match = re.search(r"\d+([A-Z][^0-9]*)$", team_text)
            school = match.group(1).strip() if match else team_text
        else:
            # For individuals: td[2] contains "FIRST LAST#{SCHOOL} [YEAR]"
            # Example: "Kanyinsola AJAYI2Auburn [JR]" → school="Auburn", name="Kanyinsola AJAYI"
            full_text = tds[2].get_text(strip=True)

            # Remove year bracket first
            year_match = _CLASS_YEAR_RE.search(full_text)
            before_year = full_text[:year_match.start()].strip() if year_match else full_text

            # Parse: athlete_name + number + school
            name_match = re.match(r"(.+?)(\d+)([A-Z].*)$", before_year)
            if name_match:
                athlete_name = name_match.group(1).strip()
                school = name_match.group(3).strip()
            else:
                athlete_name = before_year
                school = ""

        # Determine score based on place (NCAA championship scoring)
        score_map = {1: 10, 2: 8, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}
        score = score_map.get(place, 0)

        rows.append({
            "year": year,
            "event": event_key,
            "gender": gender,
            "place": place,
            "athlete_id": "",
            "athlete_name": athlete_name,
            "school": school,
            "mark": mark,
            "score": score,
        })

    # For field events with multiple attempts, keep only best place per athlete
    seen_athletes: set[str] = set()
    deduped = []
    for row in sorted(rows, key=lambda r: r["place"]):
        key = row["athlete_name"] or row["school"]
        if key not in seen_athletes:
            seen_athletes.add(key)
            deduped.append(row)

    return deduped


@dg.asset(
    group_name="championship_2026",
    description="Scrape 2026 NCAA championship finals from flashresults.com.",
)
def championship_results_2026() -> pd.DataFrame:
    """Fetch 2026 championship individual results from flashresults."""
    logger = dg.get_dagster_logger()

    try:
        events = _event_links(f"{_CHAMP_BASE}/index.htm")
        logger.info(f"2026: {len(events)} events found")
    except Exception as exc:
        logger.error(f"Failed to fetch event links: {exc}")
        raise

    all_rows: list[dict] = []
    for event_url, event_key, gender in events:
        try:
            rows = _parse_results(event_url, event_key, gender, 2026)
            logger.info(f"  2026 {gender} {event_key}: {len(rows)} finalists")
            all_rows.extend(rows)
        except Exception as exc:
            logger.warning(f"  {event_url}: failed — {exc}")

    df = pd.DataFrame(all_rows)
    out = Path("data/championship/championship_results_2026.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    logger.info(f"2026 Championship Results: {len(df)} finalists across {df['event'].nunique()} events")
    logger.info(f"  Men: {len(df[df['gender'] == 'M'])} entries")
    logger.info(f"  Women: {len(df[df['gender'] == 'W'])} entries")
    logger.info(f"Saved → {out}")

    return df


@dg.asset(
    group_name="championship_2026",
    description="Official 2026 NCAA championship team scores from scores.htm.",
)
def championship_actual_teams() -> pd.DataFrame:
    """Fetch official 2026 team scores from flashresults scores page."""
    logger = dg.get_dagster_logger()

    scores_url = f"{_CHAMP_BASE}/scores.htm"
    soup = BeautifulSoup(_get(scores_url), "html.parser")

    tables = soup.find_all("table")
    if len(tables) < 2:
        raise ValueError("Could not find score tables on scores.htm")

    all_rows: list[dict] = []

    # Parse Men's scores (table 1 - has malformed tbody)
    men_tbody = tables[1].find("tbody")
    if men_tbody:
        tds = men_tbody.find_all("td")
        for i in range(0, len(tds), 4):
            if i + 3 < len(tds):
                try:
                    place_text = tds[i].get_text(strip=True)
                    school = tds[i + 2].get_text(strip=True)
                    points_text = tds[i + 3].get_text(strip=True)

                    place = int(float(place_text))  # Handle ties like "5.5"
                    points = int(points_text)

                    all_rows.append({
                        "year": 2026,
                        "gender": "M",
                        "actual_rank": place,
                        "school": school,
                        "actual_points": points,
                    })
                except (ValueError, IndexError):
                    continue

    # Parse Women's scores (table 0 - has proper tr rows)
    women_tbody = tables[0].find("tbody")
    if women_tbody:
        rows = women_tbody.find_all("tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 4:
                try:
                    place_text = tds[0].get_text(strip=True)
                    school = tds[2].get_text(strip=True)
                    points_text = tds[3].get_text(strip=True)

                    place = int(float(place_text))
                    points = int(points_text)

                    all_rows.append({
                        "year": 2026,
                        "gender": "W",
                        "actual_rank": place,
                        "school": school,
                        "actual_points": points,
                    })
                except (ValueError, IndexError):
                    continue

    result_df = pd.DataFrame(all_rows)
    out = Path("data/championship/championship_actual_teams.csv")
    result_df.to_csv(out, index=False)

    logger.info("2026 Official Final Team Scores:")
    for gender, gender_label in [("M", "Men"), ("W", "Women")]:
        logger.info(f"  ── Top 3 {gender_label} ──")
        gdf = result_df[result_df["gender"] == gender].head(3)
        for _, row in gdf.iterrows():
            logger.info(f"    {int(row['actual_rank'])}. {row['school']} — {int(row['actual_points'])} pts")

    return result_df


assets = [championship_results_2026, championship_actual_teams]
