import type { Metrics } from "../types"

interface Props { metrics: Metrics }

export default function MetricsPanel({ metrics }: Props) {
  const stats = [
    { label: "MAE", value: `${metrics.mae} places` },
    { label: "RMSE", value: `${metrics.rmse} places` },
    { label: "Spearman", value: metrics.spearman.toFixed(3) },
    { label: "Top-3 Hit Rate", value: `${metrics.top3_hits}/${metrics.top3_total} (${(metrics.top3_hit_rate * 100).toFixed(1)}%)` },
    { label: "Training Rows", value: metrics.training_rows.toLocaleString() },
    { label: "Years Trained", value: metrics.years_trained.join(", ") },
    { label: "Events", value: `${metrics.events_count} (relays excluded)` },
  ]

  return (
    <div className="mt-10 border border-white/10 rounded-xl p-5 bg-white/5">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-4">Model Info</p>
      <div className="flex flex-wrap gap-x-8 gap-y-3">
        {stats.map(({ label, value }) => (
          <div key={label}>
            <p className="text-xs text-white/40">{label}</p>
            <p className="text-sm text-white/80 font-medium">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
