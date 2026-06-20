#!/usr/bin/env python3
"""Calculate test metrics comparing 2026 predictions to actual championship results."""

import json
from pathlib import Path
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_error, mean_squared_error

SCORING = {1: 10, 2: 8, 3: 6, 4: 5, 5: 4, 6: 3, 7: 2, 8: 1}

def main():
    # Load predictions and actual results
    pred_file = Path("data/predictions/predictions.csv")
    actual_file = Path("data/championship/championship_results_2026.csv")
    actual_teams_file = Path("data/championship/championship_actual_teams.csv")

    if not pred_file.exists():
        print(f"✗ Missing {pred_file}")
        return
    if not actual_file.exists():
        print(f"✗ Missing {actual_file}")
        return
    if not actual_teams_file.exists():
        print(f"✗ Missing {actual_teams_file}")
        return

    predictions = pd.read_csv(pred_file)
    actual = pd.read_csv(actual_file)
    actual_teams = pd.read_csv(actual_teams_file)

    print("Calculating 2026 test metrics...")
    print(f"  Predictions: {len(predictions)} finalists")
    print(f"  Actual: {len(actual)} finalists")

    # Match predictions to actual results by (event, gender, school, athlete_name)
    # Normalize names: FIRST LAST (championship) vs First Last (predictions)
    predictions = predictions.copy()
    predictions = predictions[predictions["predicted_rank"] <= 8]

    # Normalize championship names to match prediction format (title case)
    actual = actual.copy()
    actual["athlete_name"] = actual["athlete_name"].str.title().str.replace(r'\s+', ' ', regex=True)
    predictions["athlete_name"] = predictions["athlete_name"].str.replace(r'\s+', ' ', regex=True)

    merged = pd.merge(
        actual,
        predictions[["event", "gender", "school", "athlete_name", "predicted_rank", "predicted_place"]],
        on=["event", "gender", "school", "athlete_name"],
        how="inner"
    )

    print(f"  Matched: {len(merged)} athletes with both actual and predicted places")

    # Calculate metrics
    metrics = {}

    if len(merged) >= 2:
        # MAE and RMSE on matched athletes
        mae = mean_absolute_error(merged["place"], merged["predicted_place"])
        rmse = mean_squared_error(merged["place"], merged["predicted_place"]) ** 0.5
        metrics["mae"] = round(float(mae), 3)
        metrics["rmse"] = round(float(rmse), 3)
        print(f"  MAE:  {mae:.3f} places")
        print(f"  RMSE: {rmse:.3f} places")

        # Spearman correlation per event
        spearman_scores = []
        top1_correct = top1_total = 0
        top3_hits = top3_total = 0

        for (event, gender), grp in merged.groupby(["event", "gender"]):
            if len(grp) < 2:
                continue
            corr, _ = spearmanr(grp["place"], grp["predicted_place"])
            if not pd.isna(corr):
                spearman_scores.append(corr)

            # Top-1 accuracy
            actual_winner = grp.nsmallest(1, "place").index[0]
            pred_winner = grp.nsmallest(1, "predicted_place").index[0]
            top1_correct += int(actual_winner == pred_winner)
            top1_total += 1

            # Top-3 hit rate
            actual_top3 = set(grp.nsmallest(3, "place").index)
            pred_top3 = set(grp.nsmallest(3, "predicted_place").index)
            top3_hits += len(actual_top3 & pred_top3)
            top3_total += len(actual_top3)

        avg_spearman = sum(spearman_scores) / len(spearman_scores) if spearman_scores else 0
        top1_accuracy = top1_correct / top1_total if top1_total > 0 else 0
        top3_hit_rate = top3_hits / top3_total if top3_total > 0 else 0

        metrics["spearman"] = round(float(avg_spearman), 3)
        metrics["top1_accuracy"] = round(float(top1_accuracy), 3)
        metrics["top1_correct"] = int(top1_correct)
        metrics["top1_total"] = int(top1_total)
        metrics["top3_hit_rate"] = round(float(top3_hit_rate), 3)
        metrics["top3_hits"] = int(top3_hits)
        metrics["top3_total"] = int(top3_total)

        print(f"  Spearman (avg): {avg_spearman:.3f}")
        print(f"  Top-1 accuracy: {top1_correct}/{top1_total} ({100*top1_accuracy:.1f}%)")
        print(f"  Top-3 hit rate: {top3_hits}/{top3_total} ({100*top3_hit_rate:.1f}%)")

    # Team scoring error
    merged["pred_pts"] = merged["predicted_rank"].map(SCORING).fillna(0)
    merged["actual_pts"] = merged["place"].map(SCORING).fillna(0)

    team_actual = merged.groupby(["gender", "school"])["actual_pts"].sum()
    team_pred = merged.groupby(["gender", "school"])["pred_pts"].sum()
    team_df = pd.DataFrame({"actual": team_actual, "pred": team_pred}).fillna(0)
    team_mae = mean_absolute_error(team_df["actual"], team_df["pred"])
    metrics["team_scoring_mae"] = round(float(team_mae), 2)
    print(f"  Team scoring MAE: {team_mae:.2f} pts")

    # Save metrics
    metrics_file = Path("data/predictions/test_metrics_2026.json")
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    metrics_file.write_text(json.dumps(metrics, indent=2))
    print(f"\n✓ Test metrics saved to {metrics_file}")

    # Print team standings comparison
    print("\n── Top 3 Men ──")
    pred_teams_m = merged[merged["gender"] == "M"].groupby("school")["pred_pts"].sum().sort_values(ascending=False).head(3)
    for i, (school, pts) in enumerate(pred_teams_m.items(), 1):
        actual_pts = actual_teams[(actual_teams["school"] == school) & (actual_teams["gender"] == "M")]["actual_points"].sum()
        print(f"  {i}. {school}: pred={pts:.0f}, actual={actual_pts:.0f}")

    print("\n── Top 3 Women ──")
    pred_teams_w = merged[merged["gender"] == "W"].groupby("school")["pred_pts"].sum().sort_values(ascending=False).head(3)
    for i, (school, pts) in enumerate(pred_teams_w.items(), 1):
        actual_pts = actual_teams[(actual_teams["school"] == school) & (actual_teams["gender"] == "W")]["actual_points"].sum()
        print(f"  {i}. {school}: pred={pts:.0f}, actual={actual_pts:.0f}")

if __name__ == "__main__":
    main()
