import { BarChart, Bar, XAxis, YAxis, Cell, Tooltip, ResponsiveContainer } from "recharts"
import type { Athlete } from "../types"

interface Props {
  athletes: Athlete[]
  gender: "M" | "W"
}

const COLOR = { M: "#967f82", W: "#d9996f" }
const ORDINAL = ["", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th"]

const CustomTick = ({ x, y, payload, data }: any) => {
  const entry = data[payload.index]
  if (!entry) return null
  const isRelay = !entry.name
  const label = isRelay
    ? (entry.school.length > 12 ? entry.school.slice(0, 12) + "…" : entry.school)
    : (entry.name.split(" ").pop() ?? entry.name)
  return (
    <g transform={`translate(${x},${y})`}>
      <text x={0} y={0} dy={14} textAnchor="middle" fontSize={11} fill="#9ca3af">
        {payload.value}
      </text>
      <text x={0} y={0} dy={28} textAnchor="middle" fontSize={10} fill="rgba(240,238,236,0.65)">
        {label}
      </text>
      {!isRelay && (
        <text x={0} y={0} dy={40} textAnchor="middle" fontSize={9} fill="rgba(240,238,236,0.3)">
          {entry.school.length > 10 ? entry.school.slice(0, 10) + "…" : entry.school}
        </text>
      )}
    </g>
  )
}


export default function EventChart({ athletes, gender }: Props) {
  if (!athletes.length) return (
    <p className="text-sm text-white/30 text-center py-8">No data</p>
  )

  const data = athletes.map(a => ({
    label:   ORDINAL[a.rank] ?? `${a.rank}th`,
    height:  a.bar_height,
    name:    a.athlete_name ?? null,
    school:  a.school,
    place:   a.predicted_place,
  }))

  return (
    <div className="flex flex-col gap-1">
      <p className="text-xs uppercase tracking-widest text-white/40 text-center mb-1">
        {gender === "M" ? "Men" : "Women"}
      </p>
      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={data} margin={{ top: 32, right: 12, left: 12, bottom: 48 }} barCategoryGap="28%">
          <XAxis
            dataKey="label"
            tick={<CustomTick data={data} />}
            tickLine={false}
            axisLine={false}
            interval={0}
          />
          <YAxis hide domain={[0, 9]} />
          <Tooltip
            cursor={{ fill: "rgba(255,255,255,0.04)" }}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null
              const d = payload[0].payload
              return (
                <div className="bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-2 text-xs">
                  <p className="font-semibold text-white">{d.name ?? d.school}</p>
                  {d.name && <p className="text-white/50">{d.school}</p>}
                  <p className="text-white/40 mt-1">
                    Model score: <span className="text-white/70">{d.place.toFixed(2)}</span>
                    <span className="text-white/30"> (lower = stronger predicted finish)</span>
                  </p>
                </div>
              )
            }}
          />
          <Bar dataKey="height" radius={[4, 4, 0, 0]} isAnimationActive>
            {data.map((_, i) => (
              <Cell key={i} fill={COLOR[gender]} fillOpacity={1 - i * 0.08} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
