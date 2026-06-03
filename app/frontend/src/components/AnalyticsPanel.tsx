import { useState } from "react"
import { BarChart, Bar, XAxis, YAxis, Cell, ResponsiveContainer, Tooltip as ReTooltip } from "recharts"
import type { Analytics } from "../types"

interface Props { analytics: Analytics }

const MEDAL_COLOR: Record<number, string> = { 1: "#FFD700", 2: "#C0C0C0", 3: "#CD7F32" }
const CAT_COLORS = { sprints: "#967f82", distance: "#6b8fa3", field: "#7fa36b", relays: "#a3896b" }
const CAT_LABELS = { sprints: "Sprints", distance: "Distance", field: "Field", relays: "Relays" }

const FEATURE_TIPS: Record<string, string> = {
  "Avg Place":               "Average finishing position in race finals during the 2026 season.",
  "Conf Champ Place":        "Finishing position at the athlete's outdoor conference championship in this specific event.",
  "Conf Champ (Any Event)":  "Best conference championship finish across any event.",
  "Cross-Event Avg Place":   "Average finishing position across all events the athlete competed in during 2026.",
  "Personal Record":         "All-time career best in this event, normalized against the field.",
  "Season Best":             "Best performance in this event during the 2026 season.",
  "Season Avg":              "Average performance across all 2026 competitions in this event.",
  "Relay Qualifying Time":   "The relay team's time at the 2026 regional qualifying meet.",
  "Relay Season Best":       "The relay team's fastest time run this season.",
  "Relay Qualifying Place":  "The relay team's finishing position at the regional qualifying meet.",
}

function Tooltip({ text }: { text: string }) {
  const [visible, setVisible] = useState(false)
  return (
    <span className="relative inline-block ml-1">
      <span
        className="text-white/25 hover:text-white/50 cursor-default text-[10px] select-none"
        onMouseEnter={() => setVisible(true)}
        onMouseLeave={() => setVisible(false)}
      >ⓘ</span>
      {visible && (
        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 w-52 bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-2 text-xs text-white/60 leading-relaxed z-20 shadow-xl">
          {text}
        </span>
      )}
    </span>
  )
}

export default function AnalyticsPanel({ analytics }: Props) {
  const { feature_importances, team_category_breakdown, regional_split, school_depth } = analytics
  const maxImportance = Math.max(...feature_importances.map(f => f.importance))
  const maxDepth = Math.max(...school_depth.map(s => s.count))

  const regionalSummary = regional_split.east > regional_split.west
    ? "East producing more predicted top-8 finishers"
    : regional_split.west > regional_split.east
    ? "West producing more predicted top-8 finishers"
    : "East and West equally represented"

  return (
    <div className="mt-6 border border-white/10 rounded-xl p-5 bg-white/5">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-6 text-center">Analytics</p>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8 items-stretch">

        {/* Col 1 — Feature Importances */}
        <div>
          <p className="text-xs text-white/60 tracking-wider mb-3 flex items-center font-semibold">
            Feature importances
            <Tooltip text="How much each factor influenced the model's predictions. Higher % = the model relied on it more when ranking athletes." />
          </p>
          <div className="flex flex-col gap-3.5">
            {feature_importances.map(f => (
              <div key={f.feature}>
                <div className="flex justify-between text-xs mb-0.5 items-center">
                  <span className="text-white/60 flex items-center">
                    {f.label}
                    {FEATURE_TIPS[f.label] && <Tooltip text={FEATURE_TIPS[f.label]} />}
                  </span>
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

        {/* Col 2 — Regional Split + School Depth */}
        <div className="flex flex-col gap-6">
          {/* Regional Split */}
          <div>
            <p className="text-xs text-white/60 tracking-wider mb-3 flex items-center font-semibold">
              Regional Split
              <Tooltip text="The model doesn't know about regions — it ranks athletes purely on performance. Any balance here reflects equal qualifying standards between East and West, not a deliberate split. The actual split at nationals may differ." />
            </p>
            <p className="text-xs text-white/30 mb-3">Top-8 predicted finishers by qualifying region</p>
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
            <p className="text-xs text-white/20 mt-3">{regionalSummary}</p>
          </div>

          {/* School Depth */}
          <div>
            <p className="text-xs text-white/60 tracking-wider mb-3 flex items-center font-semibold">
              School Depth
              <Tooltip text="Schools with the most athletes predicted to score at nationals. High depth means a program is competitive across multiple events." />
            </p>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart
                data={school_depth}
                layout="vertical"
                margin={{ top: 0, right: 24, left: 0, bottom: 0 }}
              >
                <XAxis type="number" domain={[0, maxDepth + 1]} hide />
                <YAxis
                  type="category"
                  dataKey="school"
                  tick={{ fill: "rgba(240,238,236,0.5)", fontSize: 10 }}
                  axisLine={false}
                  tickLine={false}
                  width={80}
                />
                <ReTooltip
                  cursor={{ fill: "rgba(255,255,255,0.04)" }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null
                    const d = payload[0].payload
                    return (
                      <div className="bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-1.5 text-xs text-white/60">
                        {d.school}: <span className="text-white/80 font-semibold">{d.count}</span> athletes predicted top-8
                      </div>
                    )
                  }}
                />
                <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                  {school_depth.map((_, i) => (
                    <Cell key={i} fill="#6ba3a0" fillOpacity={1 - i * 0.07} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Col 3 — Team Category Breakdown */}
        <div>
          <p className="text-xs text-white/60 tracking-wider mb-3 font-semibold">Points by Event Category</p>
          {["M", "W"].map(gender => {
            const teams = team_category_breakdown.filter(t => t.gender === gender)
            return (
              <div key={gender} className="mb-12">
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

      </div>
    </div>
  )
}
