import type { Analytics } from "../types"

interface Props { analytics: Analytics }

const MEDAL_COLOR: Record<number, string> = { 1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32" }
const CAT_COLORS = { sprints: "#967f82", distance: "#6b8fa3", field: "#7fa36b", relays: "#a3896b" }
const CAT_LABELS = { sprints: "Sprints", distance: "Distance", field: "Field", relays: "Relays" }

export default function AnalyticsPanel({ analytics }: Props) {
  const { feature_importances, team_category_breakdown, regional_split } = analytics
  const maxImportance = Math.max(...feature_importances.map(f => f.importance))

  return (
    <div className="mt-6 border border-white/10 rounded-xl p-5 bg-white/5">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-6 text-center">Analytics</p>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">

        {/* Feature Importances */}
        <div>
          <p className="text-xs text-white/40 uppercase tracking-wider mb-3">Feature Importances</p>
          <div className="flex flex-col gap-2">
            {feature_importances.map(f => (
              <div key={f.feature}>
                <div className="flex justify-between text-xs mb-0.5">
                  <span className="text-white/60">{f.label}</span>
                  <span className="text-white/40">{(f.importance * 100).toFixed(1)}%</span>
                </div>
                <div className="h-1.5 bg-white/10 rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full bg-white/50"
                    style={{ width: `${(f.importance / maxImportance) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Team Category Breakdown */}
        <div>
          <p className="text-xs text-white/40 uppercase tracking-wider mb-3">Points by Category</p>
          {["M", "W"].map(gender => {
            const teams = team_category_breakdown.filter(t => t.gender === gender)
            return (
              <div key={gender} className="mb-4">
                <p className="text-xs text-white/30 mb-2">{gender === "M" ? "Men" : "Women"}</p>
                {teams.map(team => (
                  <div key={team.school} className="mb-2">
                    <div className="flex items-center gap-1.5 mb-1">
                      <span style={{ color: MEDAL_COLOR[team.rank] }} className="text-xs font-semibold">
                        {team.rank === 1 ? "🥇" : team.rank === 2 ? "🥈" : "🥉"}
                      </span>
                      <span className="text-xs text-white/60">{team.school}</span>
                      <span className="text-xs text-white/30 ml-auto">{team.total_points}pts</span>
                    </div>
                    <div className="flex h-2 rounded-full overflow-hidden bg-white/10">
                      {(Object.keys(CAT_COLORS) as (keyof typeof CAT_COLORS)[]).map(cat => {
                        const pct = (team[cat] / team.total_points) * 100
                        return pct > 0 ? (
                          <div
                            key={cat}
                            style={{ width: `${pct}%`, background: CAT_COLORS[cat] }}
                            title={`${CAT_LABELS[cat]}: ${team[cat]}pts`}
                          />
                        ) : null
                      })}
                    </div>
                  </div>
                ))}
              </div>
            )
          })}
          <div className="flex gap-3 flex-wrap mt-2">
            {(Object.keys(CAT_COLORS) as (keyof typeof CAT_COLORS)[]).map(cat => (
              <div key={cat} className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-sm" style={{ background: CAT_COLORS[cat] }} />
                <span className="text-[10px] text-white/30">{CAT_LABELS[cat]}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Regional Split */}
        <div>
          <p className="text-xs text-white/40 uppercase tracking-wider mb-3">Regional Split</p>
          <p className="text-xs text-white/30 mb-3">Top-8 predicted finishers by region</p>
          {[
            { label: "East", value: regional_split.east },
            { label: "West", value: regional_split.west },
          ].map(({ label, value }) => (
            <div key={label} className="mb-2">
              <div className="flex justify-between text-xs mb-0.5">
                <span className="text-white/60">{label}</span>
                <span className="text-white/40">{value} <span className="text-white/20">/ {regional_split.total}</span></span>
              </div>
              <div className="h-1.5 bg-white/10 rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full bg-white/50"
                  style={{ width: `${(value / regional_split.total) * 100}%` }}
                />
              </div>
            </div>
          ))}
          <p className="text-xs text-white/20 mt-3">
            {regional_split.east > regional_split.west ? "East" : "West"} producing more predicted top-8 finishers
          </p>
        </div>

      </div>
    </div>
  )
}
