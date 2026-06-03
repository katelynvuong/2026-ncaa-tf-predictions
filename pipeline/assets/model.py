"""Train XGBoost regressor on historical championship features → place.

Validation strategy: train on 2022–2024, evaluate on 2025 holdout,
then retrain on all 4 years before predicting 2026 placements.

Outputs:
  data/predictions/predictions.csv  — top-8 per event-gender
  data/predictions/team_scores.csv  — team point totals
  data/predictions/metrics.json     — validation metrics
"""

from __future__ import annotations

import json
from pathlib import Path

import dagster as dg
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

FEATURES = ["season_best", "season_avg", "avg_place", "conf_champ_place", "pr",
            "cross_event_avg_place", "conf_champ_place_any_event",
            "relay_qualifying_time", "relay_season_best", "relay_qualifying_place"]
TARGET = "place"
RELAY_EVENTS = {"4x100", "4x400"}
NORMALIZE_FEATURES = ["season_best", "season_avg", "pr",
                      "relay_qualifying_time", "relay_season_best"]
SCORING = {1: 10, 2: 8, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}


def _load_training(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    for col in FEATURES + [TARGET]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df.dropna(subset=[TARGET])


def _fit_normalizer(df: pd.DataFrame) -> dict:
    stats: dict = {}
    for (event, gender), grp in df.groupby(["event", "gender"]):
        stats[(event, gender)] = {}
        for feat in NORMALIZE_FEATURES:
            vals = grp[feat].dropna()
            stats[(event, gender)][feat] = (vals.mean(), vals.std()) if len(vals) >= 2 else None
    stats["__global__"] = {
        feat: (df[feat].mean(), df[feat].std()) for feat in NORMALIZE_FEATURES
    }
    return stats


def _apply_normalizer(df: pd.DataFrame, stats: dict) -> pd.DataFrame:
    df = df.copy()
    for feat in NORMALIZE_FEATURES:
        normed = pd.Series(index=df.index, dtype=float)
        for (event, gender), grp in df.groupby(["event", "gender"]):
            feat_stats = stats.get((event, gender), {}).get(feat) or stats["__global__"][feat]
            if feat_stats is None:
                continue
            mean, std = feat_stats
            if std and std > 0:
                normed.loc[grp.index] = (grp[feat] - mean) / std
        df[feat] = normed
    return df


def _build_model() -> XGBRegressor:
    return XGBRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        random_state=42,
        tree_method="hist",
    )


def _rank_within_event(df: pd.DataFrame, score_col: str) -> pd.Series:
    return df.groupby(["event", "gender"])[score_col].rank(method="min", ascending=True)


@dg.asset(
    group_name="ml",
    deps=["training_dataset", "features"],
    description=(
        "Train XGBoost on 2022–2024, evaluate on 2025 holdout, "
        "retrain on all years, predict 2026 placements and team scores."
    ),
)
def predictions() -> pd.DataFrame:
    logger = dg.get_dagster_logger()

    train_df = _load_training("data/training/training_dataset.csv")
    pred_df  = pd.read_csv("data/features.csv", dtype=str)
    for col in FEATURES:
        pred_df[col] = pd.to_numeric(pred_df[col], errors="coerce")

    # ── Validation: train 2022–2024, evaluate on 2025 ──────────────────────
    train_val = train_df[train_df["year"] <= 2024].dropna(subset=FEATURES, how="all")
    holdout   = train_df[train_df["year"] == 2025].dropna(subset=FEATURES, how="all")

    val_norm  = _fit_normalizer(train_val)
    train_val = _apply_normalizer(train_val, val_norm)
    holdout   = _apply_normalizer(holdout, val_norm)

    model_val = _build_model()
    model_val.fit(train_val[FEATURES], train_val[TARGET])

    if len(holdout) > 0:
        preds_2025 = model_val.predict(holdout[FEATURES])
        mae  = mean_absolute_error(holdout[TARGET], preds_2025)
        rmse = mean_squared_error(holdout[TARGET], preds_2025) ** 0.5

        holdout = holdout.copy()
        holdout["pred"] = preds_2025

        spearman_scores = []
        top1_correct = top1_total = 0
        top3_hits = top3_total = 0
        for _, grp in holdout.groupby(["event", "gender"]):
            if len(grp) < 2:
                continue
            corr, _ = spearmanr(grp[TARGET], grp["pred"])
            if not pd.isna(corr):
                spearman_scores.append(corr)
            # Top-1 accuracy
            actual_winner = grp.nsmallest(1, TARGET).index[0]
            pred_winner   = grp.nsmallest(1, "pred").index[0]
            top1_correct += int(actual_winner == pred_winner)
            top1_total   += 1
            # Top-3 hit rate
            actual_top3 = set(grp.nsmallest(3, TARGET).index)
            pred_top3   = set(grp.nsmallest(3, "pred").index)
            top3_hits  += len(actual_top3 & pred_top3)
            top3_total += len(actual_top3)

        avg_spearman  = sum(spearman_scores) / len(spearman_scores) if spearman_scores else 0
        top1_accuracy = top1_correct / top1_total if top1_total > 0 else 0
        top3_hit_rate = top3_hits / top3_total if top3_total > 0 else 0

        # Team scoring error — compare actual vs predicted team point totals
        holdout["pred_rank"]  = _rank_within_event(holdout, "pred").astype(int)
        holdout["actual_pts"] = holdout[TARGET].map(SCORING).fillna(0)
        holdout["pred_pts"]   = holdout["pred_rank"].map(SCORING).fillna(0)
        team_actual = holdout.groupby(["gender", "school"])["actual_pts"].sum()
        team_pred   = holdout.groupby(["gender", "school"])["pred_pts"].sum()
        team_df     = pd.DataFrame({"actual": team_actual, "pred": team_pred}).fillna(0)
        team_mae    = mean_absolute_error(team_df["actual"], team_df["pred"])

        logger.info("── 2025 holdout validation ──")
        logger.info(f"  MAE:                {mae:.3f} places")
        logger.info(f"  RMSE:               {rmse:.3f} places")
        logger.info(f"  Spearman (avg):     {avg_spearman:.3f}")
        logger.info(f"  Top-1 accuracy:     {top1_correct}/{top1_total} ({100*top1_accuracy:.1f}%)")
        logger.info(f"  Top-3 hit rate:     {top3_hits}/{top3_total} ({100*top3_hit_rate:.1f}%)")
        logger.info(f"  Team scoring MAE:   {team_mae:.2f} pts")

        Path("data/predictions").mkdir(parents=True, exist_ok=True)
        Path("data/predictions/metrics.json").write_text(json.dumps({
            "mae":              round(float(mae), 3),
            "rmse":             round(float(rmse), 3),
            "spearman":         round(float(avg_spearman), 3),
            "top1_accuracy":    round(float(top1_accuracy), 3),
            "top1_correct":     int(top1_correct),
            "top1_total":       int(top1_total),
            "top3_hit_rate":    round(float(top3_hit_rate), 3),
            "top3_hits":        int(top3_hits),
            "top3_total":       int(top3_total),
            "team_scoring_mae": round(float(team_mae), 2),
        }, indent=2))
    else:
        logger.warning("No 2025 holdout data available for validation.")

    # ── Final model: train on all years ─────────────────────────────────────
    full_train  = train_df.dropna(subset=FEATURES, how="all")
    final_norm  = _fit_normalizer(full_train)
    full_train  = _apply_normalizer(full_train, final_norm)
    model_final = _build_model()
    model_final.fit(full_train[FEATURES], full_train[TARGET])
    logger.info(f"Final model trained on {len(full_train)} rows ({full_train['year'].nunique()} years)")

    importance = dict(zip(FEATURES, model_final.feature_importances_))
    logger.info("Feature importances:")
    for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
        logger.info(f"  {feat:30s}: {imp:.4f}")
    Path("data/predictions/feature_importances.json").write_text(
        json.dumps([
            {"feature": f, "importance": round(float(v), 4)}
            for f, v in sorted(importance.items(), key=lambda x: -x[1])
        ], indent=2)
    )

    # ── Predict 2026 ─────────────────────────────────────────────────────────
    ind = pred_df.copy()
    ind = _apply_normalizer(ind, final_norm)
    ind["predicted_place"] = model_final.predict(ind[FEATURES])
    ind["predicted_rank"]  = _rank_within_event(ind, "predicted_place").astype(int)

    out_dir = Path("data/predictions")
    out_dir.mkdir(parents=True, exist_ok=True)
    ind[ind["predicted_rank"] <= 8].to_csv(out_dir / "predictions.csv", index=False)
    logger.info(f"Saved {len(ind)} predictions → data/predictions/predictions.csv")

    # ── Team scores ──────────────────────────────────────────────────────────
    score_rows = []
    for gender in ["M", "W"]:
        gdf = ind[ind["gender"] == gender].copy()

        gdf["points"] = gdf["predicted_rank"].map(SCORING).fillna(0)
        totals = (
            gdf.groupby("school")["points"]
            .sum()
            .reset_index()
            .rename(columns={"points": "total_points"})
            .sort_values("total_points", ascending=False)
            .reset_index(drop=True)
        )
        totals["gender"] = gender
        totals["predicted_rank"] = range(1, len(totals) + 1)
        score_rows.append(totals)

    team_scores = pd.concat(score_rows, ignore_index=True)
    team_scores.to_csv(out_dir / "team_scores.csv", index=False)

    logger.info("── Top 3 Men ──")
    for _, row in team_scores[team_scores["gender"] == "M"].head(3).iterrows():
        logger.info(f"  {int(row['predicted_rank'])}. {row['school']} — {row['total_points']:.0f} pts")
    logger.info("── Top 3 Women ──")
    for _, row in team_scores[team_scores["gender"] == "W"].head(3).iterrows():
        logger.info(f"  {int(row['predicted_rank'])}. {row['school']} — {row['total_points']:.0f} pts")

    return ind


assets = [predictions]
