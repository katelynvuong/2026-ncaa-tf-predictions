import { useState } from "react"
import type { Metrics } from "../types"

interface Props { metrics: Metrics }

function Tooltip({ text }: { text: string }) {
  const [visible, setVisible] = useState(false)
  return (
    <span className="relative inline-block ml-1">
      <span
        className="text-white/25 hover:text-white/50 cursor-default text-[10px] select-none"
        onMouseEnter={() => setVisible(true)}
        onMouseLeave={() => setVisible(false)}
      >
        ⓘ
      </span>
      {visible && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 w-52 bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-2 text-xs text-white/60 leading-relaxed z-20 shadow-xl">
          {text}
        </span>
      )}
    </span>
  )
}

export default function MetricsPanel({ metrics }: Props) {
  const years = metrics.years_trained
  const yearsLabel = years.length >= 2
    ? `${years[0]}–${years[years.length - 1]}`
    : years.join(", ")

  const stats = [
    {
      label: "MAE",
      value: `${metrics.mae} places`,
      tip: "On average, the model's predicted finishing position is off by this many places — e.g. 1.3 means a predicted 2nd place might actually finish 1st or 3rd.",
    },
    {
      label: "RMSE",
      value: `${metrics.rmse} places`,
      tip: "Similar to MAE but larger errors are penalized more heavily. A much higher RMSE than MAE would indicate some predictions are badly off.",
    },
    {
      label: "Spearman",
      value: metrics.spearman.toFixed(3),
      tip: "How well the model ranks athletes within each event. 1.0 = perfect order, 0 = random. 0.7+ is considered a strong result.",
    },
    {
      label: "Top-1 Accuracy",
      value: `${metrics.top1_correct}/${metrics.top1_total} (${(metrics.top1_accuracy * 100).toFixed(1)}%)`,
      tip: "Of all individual events at the 2025 NCAA Championships (validation holdout), how many event winners did the model correctly predict.",
    },
    {
      label: "Top-3 Hit Rate",
      value: `${metrics.top3_hits}/${metrics.top3_total} (${(metrics.top3_hit_rate * 100).toFixed(1)}%)`,
      tip: "Of all athletes who actually finished in the top 3 at the 2025 NCAA Championships (used as a validation holdout set), how many did the model also predict in the top 3.",
    },
    {
      label: "Team Scoring MAE",
      value: `${metrics.team_scoring_mae} pts`,
      tip: "On average, how many points off the model's predicted team totals are from actual 2025 team totals. Lower is better — e.g. 3.5 means the model's team point predictions are off by about 3–4 points.",
    },
    {
      label: "Years Trained",
      value: yearsLabel,
      tip: "The championship seasons used to train the model. The most recent year is held out for validation.",
    },
    {
      label: "Events",
      value: `${metrics.events_count}`,
      tip: "Number of events included in model training, including relay events (4x100, 4x400).",
    },
  ]

  return (
    <div className="mt-10 border border-white/10 rounded-xl p-5 bg-white/5">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-4 text-center">Model Info</p>
      <div className="text-center mb-5">
        <p className="text-sm font-semibold text-white/80">{metrics.model_type}</p>
      </div>
      <div className="flex flex-wrap justify-center gap-x-8 gap-y-3">
        {stats.map(({ label, value, tip }) => (
          <div key={label}>
            <p className="text-xs text-white/40 flex items-center">
              {label}
              <Tooltip text={tip} />
            </p>
            <p className="text-sm text-white/80 font-medium">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
