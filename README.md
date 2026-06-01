# 2026 NCAA D1 Outdoor Track & Field Championship Predictions

A Dagster pipeline that scrapes TFRRS athlete data, engineers features, trains an XGBoost + Ridge ensemble model, and serves predictions through a React web app.

## Project Structure

```
pipeline/assets/         Dagster assets (data collection → ML → app)
data/
  regional_athletes/     TFRRS qualifying list CSVs
  profiles/              Raw TFRRS athlete profile JSONs
  flattened_dataframes/  season_results.csv, athletes_prs.csv
  final_athletes.csv     Regional qualifiers (Flash Results)
  features.csv           Engineered features for 2026 qualifiers
  training/              Historical championship data + training dataset
  predictions/           Model outputs (predictions.csv, team_scores.csv, metrics.json)
  app/                   Preprocessed JSON served by the web app
app/
  backend/               FastAPI server
  frontend/              React + Vite + Tailwind frontend
```

## Setup

Requires Python 3.10+ and Node 18+.

```bash
# Python dependencies
uv sync

# Frontend dependencies
cd app/frontend && npm install
```

## Running the Dagster Pipeline

```bash
source .venv/bin/activate
dagster dev -m pipeline
```

Then open `http://localhost:3000`.

### Asset groups and order

**data_collection**
1. `regional_athletes` — scrapes TFRRS qualifying lists (partitioned: east_f, east_m, west_f, west_m)
2. `athlete_profiles` — bulk-fetches TFRRS profiles (~60–90 min, checkpointed)
3. `final_athletes` — scrapes Flash Results regional qualifiers (Q/q)
4. `supplemental_profiles` — fetches any profiles missing from final_athletes

**data_processing**
5. `flattened_dataframes` — flattens JSON profiles → season_results.csv + athletes_prs.csv
6. `features` — engineers 7 features per (athlete, event) for 2026 qualifiers

**ml**
7. `championship_results` — scrapes 2022–2025 NCAA championship placements from TFRRS
8. `historical_profiles` — fetches profiles for historical championship athletes
9. `training_features` — computes features for historical athletes per year
10. `training_dataset` — joins championship results + features into ML training CSV
11. `predictions` — trains XGBoost, evaluates on 2025 holdout, predicts 2026

**app**
12. `app_metrics` — exports model validation metrics to `app/frontend/public/data/metrics.json`
13. `app_team_standings` — builds top-3 team standings with scorer breakdowns
14. `app_event_predictions` — structures per-event top-8 predictions for the frontend
15. `frontend_build` — runs `npm run build` in app/frontend/

## Running the Web App

After materializing through the `app` group in Dagster:

```bash
cd app/frontend
npm run dev
# Opens http://localhost:5173
```

The FastAPI backend (`app/backend/`) is optional — it's only needed if you prefer to serve data via API locally. The default setup reads JSON files directly from `public/data/`.

The app shows:
- **Team Standings** — gold/silver/bronze podium for predicted top-3 men's and women's teams
- **Event Explorer** — dropdown to view predicted top-8 finishers for any individual event
- **Model Info** — validation metrics and model details

## Deploying to Vercel (Public Link)

1. Install the Vercel CLI: `npm i -g vercel`
2. From the project root, run: `vercel`
3. Follow the prompts — Vercel will detect `vercel.json` and configure automatically
4. On subsequent deploys after updating predictions: `vercel --prod`

**Update workflow:**
1. Re-run Dagster pipeline through the `app` group
2. `git add app/frontend/public/data/ && git commit -m "update predictions"`
3. `git push` — Vercel auto-deploys within ~30 seconds

## Data Sources

| Source | Used for |
|---|---|
| [TFRRS](https://tfrrs.org) | Athlete profiles, PRs, season results, historical championship results |
| [Flash Results](https://flashresults.ncaa.com) | 2026 regional qualifying results (Q/q) |
| [ESPN CDN](https://espn.com) | School logos |

## Dependencies

- [Dagster](https://dagster.io) — pipeline orchestration
- [sports-skills / xctf](https://github.com/machina-sports/sports-skills) — TFRRS data connector
- XGBoost, scikit-learn, scipy — ML model
- FastAPI + uvicorn — REST API backend
- React + Vite + Tailwind + Recharts — frontend
