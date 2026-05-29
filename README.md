# 2026 NCAA D1 Outdoor Track & Field Championship Predictions

A Dagster pipeline that scrapes TFRRS athlete data and builds the dataset for predicting team scores at the 2026 NCAA D1 Outdoor Track & Field Championships.

## Pipeline Overview

```
qualifying_athletes  →  athlete_profiles  →  flattened_dataframes
```

### Assets

| Asset | Description | Output |
|---|---|---|
| `qualifying_athletes` | Scrapes athlete slugs from the TFRRS D1 Outdoor qualifying lists (East + West, men + women) | `data/qualifying_athletes_{partition}.csv` |
| `athlete_profiles` | Fetches each athlete's full TFRRS profile (PRs + career meet results) | `data/profiles/{athlete_id}.json` |
| `flattened_dataframes` | Flattens raw JSON profiles into tabular form for modeling | `data/athletes_prs.csv`, `data/season_results.csv` |

### Partitions

`qualifying_athletes` and `athlete_profiles` are partitioned by region + gender:

| Partition | Qualifying list |
|---|---|
| `east_f` | TFRRS list 5622 — women |
| `east_m` | TFRRS list 5622 — men |
| `west_f` | TFRRS list 5623 — women |
| `west_m` | TFRRS list 5623 — men |

Each partition runs independently (~900 athletes, ~20 min). If a partition fails mid-run, re-running it skips already-fetched profiles and resumes from where it left off.

## Setup

```bash
# Install dependencies
uv sync

# Start the Dagster UI
uv run dg dev
```

Requires Python 3.10+.

## Running the Pipeline

1. Materialize **`qualifying_athletes`** — select all 4 partitions (or run as a backfill)
2. Materialize **`athlete_profiles`** — select all 4 partitions; takes ~60–90 min total
3. Materialize **`flattened_dataframes`** — runs in seconds once all profiles are cached

## Data

| File | Description |
|---|---|
| `data/qualifying_athletes_{partition}.csv` | Athlete IDs, school slugs, name slugs, and TFRRS URLs per partition |
| `data/profiles/{athlete_id}.json` | Raw TFRRS profile: name, school, eligibility, PRs, full career meet results |
| `data/athletes_prs.csv` | One row per athlete with PR columns as features |
| `data/season_results.csv` | One row per athlete × meet × event (training data) |

## Dependencies

- [Dagster](https://dagster.io) — pipeline orchestration
- [sports-skills / xctf](https://github.com/machina-sports/sports-skills) — TFRRS data connector
- pandas, requests
