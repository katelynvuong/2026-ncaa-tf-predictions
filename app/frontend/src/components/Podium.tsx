import { useState } from "react"
import type { TeamStanding } from "../types"

interface Props {
  standings: TeamStanding[]
  gender: "M" | "W"
}

const PODIUM_ORDER = [2, 1, 3]

const MEDAL = {
  1: { color: "#FFD700", height: "h-52", ring: "ring-[#FFD700]/40", label: "text-[#FFD700]" },
  2: { color: "#C0C0C0", height: "h-40", ring: "ring-[#C0C0C0]/40", label: "text-[#C0C0C0]" },
  3: { color: "#CD7F32", height: "h-32", ring: "ring-[#CD7F32]/40", label: "text-[#CD7F32]" },
} as const

export default function Podium({ standings, gender }: Props) {
  const [hovered, setHovered] = useState<number | null>(null)

  const byRank: Record<number, TeamStanding> = {}
  standings.filter(s => s.gender === gender).forEach(s => { byRank[s.rank] = s })

  return (
    <div className="flex flex-col items-center gap-2">
      <p className="text-xs uppercase tracking-widest text-white/40 mb-2">
        {gender === "M" ? "Men" : "Women"}
      </p>

      <div className="flex items-end justify-center gap-6">
        {PODIUM_ORDER.map(rank => {
          const team = byRank[rank]
          if (!team) return <div key={rank} className="w-28" />
          const medal = MEDAL[rank as 1 | 2 | 3]
          const isHovered = hovered === rank

          return (
            <div key={rank} className="flex flex-col items-center gap-2 w-28 relative">
              {/* Logo */}
              <div className={`w-14 h-14 rounded-full bg-white/10 ring-2 ${medal.ring} flex items-center justify-center overflow-hidden`}>
                {team.logo_url
                  ? <img src={team.logo_url} alt={team.school} className="w-12 h-12 object-contain" />
                  : <span className="text-xs font-bold text-white/60">{team.school.slice(0, 3).toUpperCase()}</span>
                }
              </div>

              {/* Points */}
              <p className={`text-sm font-bold ${medal.label}`}>{team.total_points} pts</p>

              {/* Solid bar */}
              <div
                className={`w-24 ${medal.height} rounded-t-md cursor-pointer transition-opacity duration-150 ${isHovered ? "opacity-80" : "opacity-100"}`}
                style={{ background: medal.color }}
                onMouseEnter={() => setHovered(rank)}
                onMouseLeave={() => setHovered(null)}
              />

              {/* Tooltip */}
              {isHovered && (
                <div className="absolute bottom-full mb-2 left-1/2 -translate-x-1/2 z-10 w-52 bg-[#1c1c24] border border-white/10 rounded-xl shadow-xl p-3">
                  <p className="text-xs font-semibold text-white/80 mb-2">{team.school} Scorers</p>
                  <div className="flex flex-col gap-1">
                    {team.scorers.map((s, i) => (
                      <div key={i} className="flex justify-between items-center text-xs">
                        <span className="text-white/50 truncate mr-2">{s.athlete_name} <span className="text-white/30">· {s.event}</span></span>
                        <span className={`font-semibold flex-shrink-0 ${medal.label}`}>{s.points}pt</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* School name */}
              <p className="text-xs text-white/70 text-center leading-tight">{team.school}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
