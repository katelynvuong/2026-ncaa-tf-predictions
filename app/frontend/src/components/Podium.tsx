import type { TeamStanding } from "../types"

interface Props {
  standings: TeamStanding[]
  gender: "M" | "W"
}

// Podium order: 2nd left, 1st centre, 3rd right
const PODIUM_ORDER = [2, 1, 3]

const MEDAL = {
  1: {
    bar:      ["#B8860B", "#DAA520", "#FFD700"],
    height:   "h-52",
    ring:     "ring-[#FFD700]/50",
    label:    "text-[#FFD700]",
  },
  2: {
    bar:      ["#707070", "#909090", "#C0C0C0"],
    height:   "h-40",
    ring:     "ring-[#C0C0C0]/50",
    label:    "text-[#C0C0C0]",
  },
  3: {
    bar:      ["#7B4A1E", "#A0622A", "#CD7F32"],
    height:   "h-32",
    ring:     "ring-[#CD7F32]/50",
    label:    "text-[#CD7F32]",
  },
} as const

const CATEGORIES = ["sprints", "distance", "field"] as const
const CAT_KEYS = { sprints: "sprints_pts", distance: "distance_pts", field: "field_pts" } as const
const CAT_LABELS = { sprints: "Sprints", distance: "Distance", field: "Field" }

export default function Podium({ standings, gender }: Props) {
  const byRank: Record<number, TeamStanding> = {}
  standings.filter(s => s.gender === gender).forEach(s => { byRank[s.rank] = s })

  return (
    <div className="flex flex-col items-center gap-2">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-2">
        {gender === "M" ? "Men" : "Women"}
      </p>

      {/* Category legend */}
      <div className="flex gap-4 mb-4">
        {CATEGORIES.map((cat, i) => (
          <div key={cat} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-sm" style={{ background: MEDAL[1].bar[i] }} />
            <span className="text-xs text-white/50">{CAT_LABELS[cat]}</span>
          </div>
        ))}
      </div>

      <div className="flex items-end justify-center gap-6">
        {PODIUM_ORDER.map(rank => {
          const team = byRank[rank]
          if (!team) return <div key={rank} className="w-28" />
          const medal = MEDAL[rank as 1 | 2 | 3]

          const total = team.sprints_pts + team.distance_pts + team.field_pts || 1
          const segments = CATEGORIES.map((cat, i) => ({
            key:    cat,
            pts:    team[CAT_KEYS[cat]],
            color:  medal.bar[i],
            pct:    (team[CAT_KEYS[cat]] / total) * 100,
          }))

          return (
            <div key={rank} className="flex flex-col items-center gap-2 w-28">
              {/* Logo */}
              <div className={`w-14 h-14 rounded-full bg-white/10 ring-2 ${medal.ring} flex items-center justify-center overflow-hidden`}>
                {team.logo_url
                  ? <img src={team.logo_url} alt={team.school} className="w-12 h-12 object-contain" />
                  : <span className="text-xs font-bold text-white/60">{team.school.slice(0, 3).toUpperCase()}</span>
                }
              </div>

              {/* Points */}
              <p className={`text-sm font-bold ${medal.label}`}>{team.total_points} pts</p>

              {/* Stacked bar */}
              <div className={`w-24 ${medal.height} flex flex-col-reverse rounded-t-md overflow-hidden`}>
                {segments.map(seg => (
                  <div
                    key={seg.key}
                    style={{ height: `${seg.pct}%`, background: seg.color }}
                    title={`${CAT_LABELS[seg.key as keyof typeof CAT_LABELS]}: ${seg.pts} pts`}
                  />
                ))}
              </div>

              {/* School name */}
              <p className="text-xs text-white/70 text-center leading-tight">{team.school}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
