# 2026 NCAA D1 Outdoor Track & Field Championship Predictions

Predicts the top 3 men's and women's teams at the 2026 NCAA D1 Outdoor Track & Field Championships. For each individual event, an XGBoost model predicts the top 8 finishers — those placements are then scored and summed to rank teams.

**Live app:** [2026-ncaa-tf-predictions.vercel.app](https://2026-ncaa-tf-predictions.vercel.app)

---

## How It Works

1. **Data collection** — Athlete profiles and season results are scraped from TFRRS via the sports-skills library. Regional qualifiers are pulled from Flash Results.
2. **Feature engineering** — 7 features are computed per athlete per event: `season_best`, `season_avg`, `avg_place`, `conf_champ_place`, `pr`, `cross_event_avg_place`, and `conf_champ_place_any_event`.
3. **Model training** — An XGBoost regressor is trained on 4 years of historical NCAA championship results (2022–2025), using 2025 as a holdout validation set.
4. **Predictions** — The model predicts a finishing score for every 2026 qualifier. Athletes are ranked within each event, and team points are summed to produce the final standings.

---

## Setup

Requires Python 3.10+ and Node 18+.

```bash
# Python dependencies
uv sync

# Frontend dependencies
cd app/frontend && npm install
```

---

## Running the Pipeline

```bash
source .venv/bin/activate
dagster dev -m pipeline
```

Open `http://localhost:3000` to access the Dagster UI.

### Asset Groups

**data_collection**
| # | Asset | Description |
|---|---|---|
| 1 | `regional_athletes` | Scrapes TFRRS qualifying lists (partitioned: east_f, east_m, west_f, west_m) |
| 2 | `athlete_profiles` | Bulk-fetches TFRRS profiles (~60–90 min, checkpointed) |
| 3 | `final_athletes` | Scrapes Flash Results regional qualifiers (Q/q) |
| 4 | `supplemental_profiles` | Fetches any profiles missing from final_athletes |

**data_processing**
| # | Asset | Description |
|---|---|---|
| 5 | `flattened_dataframes` | Flattens JSON profiles → season_results.csv + athletes_prs.csv |
| 6 | `features` | Engineers 7 features per (athlete, event) for 2026 qualifiers |

**ml**
| # | Asset | Description |
|---|---|---|
| 7 | `championship_results` | Scrapes 2022–2025 NCAA championship placements from TFRRS |
| 8 | `historical_profiles` | Fetches profiles for historical championship athletes |
| 9 | `training_features` | Computes features for historical athletes per year |
| 10 | `training_dataset` | Joins championship results + features into ML training CSV |
| 11 | `predictions` | Trains XGBoost, evaluates on 2025 holdout, predicts 2026 |

**app**
| # | Asset | Description |
|---|---|---|
| 12 | `app_metrics` | Exports model validation metrics to `public/data/metrics.json` |
| 13 | `app_team_standings` | Builds top-3 team standings with per-athlete scorer breakdowns |
| 14 | `app_event_predictions` | Structures per-event top-8 predictions for the frontend |
| 15 | `frontend_build` | Runs `npm run build` in app/frontend/ |

---

## Running the Web App Locally

Materialize through the `app` group in Dagster first, then:

```bash
cd app/frontend
npm run dev
# Opens http://localhost:5173
```

The app reads JSON files directly from `app/frontend/public/data/` — no backend required.

---

## Deploying to Vercel

The app is deployed as a static site. To redeploy after updating predictions:

```bash
# 1. Re-run Dagster pipeline through the app group
# 2. Commit the updated data files
git add app/frontend/public/data/
git commit -m "update predictions"
git push   # Vercel auto-deploys in ~30 seconds
```

To deploy manually:
```bash
vercel --prod --archive=tgz
```

---

## Data Sources

| Source | Used for |
|---|---|
| [TFRRS](https://tfrrs.org) | Athlete profiles, PRs, season results, historical championship results |
| [Flash Results](https://flashresults.ncaa.com) | 2026 regional qualifying results |
| [ESPN CDN](https://espn.com) | School logos |

## Dependencies

- [Dagster](https://dagster.io) — pipeline orchestration
- [sports-skills / xctf](https://github.com/machina-sports/sports-skills) — TFRRS data connector
- XGBoost, scikit-learn, scipy — ML model
- FastAPI + uvicorn — optional local API backend
- React + Vite + Tailwind + Recharts — frontend
