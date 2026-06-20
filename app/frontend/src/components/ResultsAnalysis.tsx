import { useState } from "react"

interface TestMetrics {
  mae: number
  rmse: number
  spearman: number
  top1_accuracy: number
  top1_correct: number
  top1_total: number
  top3_hit_rate: number
  top3_hits: number
  top3_total: number
  team_scoring_mae: number
}

interface Props {
  testMetrics: TestMetrics | null
}

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
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 w-56 bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-2 text-xs text-white/60 leading-relaxed z-20 shadow-xl">
          {text}
        </span>
      )}
    </span>
  )
}

function MetricCard({
  label,
  value,
  unit,
  tip,
  highlight = false,
}: {
  label: string
  value: string | number
  unit?: string
  tip: string
  highlight?: boolean
}) {
  return (
    <div className={`p-4 rounded-lg border transition-colors ${
      highlight
        ? "bg-white/8 border-white/15"
        : "bg-white/5 border-white/10 hover:bg-white/7"
    }`}>
      <p className="text-xs text-white/40 flex items-center mb-2">
        {label}
        <Tooltip text={tip} />
      </p>
      <p className="text-2xl font-bold text-white/90">
        {value}
        {unit && <span className="text-sm text-white/40 ml-1">{unit}</span>}
      </p>
    </div>
  )
}

export default function ResultsAnalysis({ testMetrics }: Props) {
  if (!testMetrics) {
    return (
      <div className="text-center py-12 text-white/40">
        <p className="text-sm">Test metrics not available</p>
      </div>
    )
  }

  const metrics = testMetrics
  const top1Pct = (metrics.top1_accuracy * 100).toFixed(1)
  const top3Pct = (metrics.top3_hit_rate * 100).toFixed(1)

  return (
    <div className="animate-rise">
      <div className="mb-8">
        <p className="text-xs uppercase tracking-widest text-white/40 mb-3">2026 Championship Results</p>
        <h2 className="text-2xl font-semibold text-white/90 mb-6">Model Performance Analysis</h2>

        {/* Key Metrics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
          <MetricCard
            label="Individual Placement Error"
            value={metrics.mae.toFixed(2)}
            unit="places"
            tip="Average error between predicted and actual finishing positions. Lower is better."
            highlight={true}
          />
          <MetricCard
            label="Placement RMSE"
            value={metrics.rmse.toFixed(2)}
            unit="places"
            tip="Root mean square error. Penalizes larger errors more heavily."
            highlight={true}
          />
          <MetricCard
            label="Ranking Correlation"
            value={metrics.spearman.toFixed(3)}
            tip="Spearman correlation of predicted vs actual rankings. Ranges from -1 to 1; 0.3+ indicates moderate correlation."
            highlight={true}
          />
        </div>

        {/* Accuracy Metrics */}
        <div className="bg-white/5 border border-white/10 rounded-xl p-6 mb-8">
          <h3 className="text-sm font-semibold text-white/70 mb-5 uppercase tracking-wide">Event-Level Accuracy</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <p className="text-xs text-white/40 mb-2 flex items-center">
                Event Winner Prediction
                <Tooltip text="Percentage of individual events where the model correctly predicted the actual winner" />
              </p>
              <div className="flex items-end gap-3">
                <span className="text-3xl font-bold text-white/90">{top1Pct}%</span>
                <span className="text-sm text-white/50 mb-1">({metrics.top1_correct}/{metrics.top1_total} events)</span>
              </div>
              <div className="mt-3 h-1.5 bg-white/10 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-white/40 to-white/60 rounded-full"
                  style={{ width: `${metrics.top1_accuracy * 100}%` }}
                />
              </div>
            </div>
            <div>
              <p className="text-xs text-white/40 mb-2 flex items-center">
                Top-3 Finisher Hit Rate
                <Tooltip text="Percentage of athletes who actually finished top-3 that the model also predicted in the top-3" />
              </p>
              <div className="flex items-end gap-3">
                <span className="text-3xl font-bold text-white/90">{top3Pct}%</span>
                <span className="text-sm text-white/50 mb-1">({metrics.top3_hits}/{metrics.top3_total} athletes)</span>
              </div>
              <div className="mt-3 h-1.5 bg-white/10 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-white/40 to-white/60 rounded-full"
                  style={{ width: `${metrics.top3_hit_rate * 100}%` }}
                />
              </div>
            </div>
          </div>
        </div>

        {/* Team Scoring */}
        <div className="bg-white/5 border border-white/10 rounded-xl p-6">
          <h3 className="text-sm font-semibold text-white/70 mb-5 uppercase tracking-wide">Team Scoring Accuracy</h3>
          <div className="flex flex-col md:flex-row md:items-center md:justify-between">
            <div className="mb-4 md:mb-0">
              <p className="text-xs text-white/40 mb-2 flex items-center">
                Average Team Points Error
                <Tooltip text="Average difference between predicted and actual team point totals. Lower is better — predicting team composition accurately is harder than individual placement." />
              </p>
              <p className="text-3xl font-bold text-white/90">
                {metrics.team_scoring_mae.toFixed(2)} <span className="text-sm text-white/40">pts</span>
              </p>
            </div>
            <div className="text-xs text-white/50 space-y-1 max-w-xs">
              <p>• Team scores depend on getting 8 finalists right per event</p>
              <p>• Small differences in individual rankings compound at team level</p>
              <p>• Harder to predict than individual event outcomes</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
