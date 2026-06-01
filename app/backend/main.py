"""FastAPI backend — serves preprocessed JSON data to the React frontend."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="NCAA T&F Predictions API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_DATA = Path(__file__).parents[1] / "frontend" / "public" / "data"


def _load(filename: str) -> dict | list:
    path = _DATA / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found — run Dagster app assets first.")
    return json.loads(path.read_text())


@app.get("/api/metrics")
def get_metrics():
    return _load("metrics.json")


@app.get("/api/team-standings")
def get_team_standings():
    return _load("team_standings.json")


@app.get("/api/event-predictions")
def get_event_predictions():
    return _load("event_predictions.json")
