import { BarChart, Bar, XAxis, YAxis, Cell, Tooltip, ResponsiveContainer } from "recharts"
import type { Athlete } from "../types"

interface Props {
  athletes: Athlete[]
  gender: "M" | "W"
}

const COLOR = { M: "#967f82", W: "#d9996f" }

const ORDINAL = ["", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th"]

const CustomLabel = ({ x, y, width, value }: any) => (
  <text
    x={x + width / 2}
    y={y - 8}
    textAnchor="middle"
    fontSize={11}
    fill="#f0eeec"
    opacity={0.85}
  >
    {value}
  </text>
)

export default function EventChart({ athletes, gender }: Props) {
  if (!athletes.length) return (
    <p className="text-sm text-white/30 text-center py-8">No data</p>
  )

  const data = athletes.map(a => ({
    label:    ORDINAL[a.rank] ?? `${a.rank}th`,
    height:   a.bar_height,
    name:     a.athlete_name,
    school:   a.school,
    place:    a.predicted_place,
  }))

  return (
    <div className="flex flex-col gap-1">
      <p className="text-xs uppercase tracking-widest text-white/40 text-center mb-1">
        {gender === "M" ? "Men" : "Women"}
      </p>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 28, right: 8, left: -24, bottom: 0 }} barCategoryGap="25%">
          <XAxis
            dataKey="label"
            tick={{ fill: "#9ca3af", fontSize: 11 }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis hide domain={[0, 9]} />
          <Tooltip
            cursor={{ fill: "rgba(255,255,255,0.04)" }}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null
              const d = payload[0].payload
              return (
                <div className="bg-[#1c1c24] border border-white/10 rounded-lg px-3 py-2 text-xs">
                  <p className="font-semibold text-white">{d.name}</p>
                  <p className="text-white/50">{d.school}</p>
                  <p className="text-white/40 mt-1">Predicted score: {d.place.toFixed(2)}</p>
                </div>
              )
            }}
          />
          <Bar dataKey="height" radius={[4, 4, 0, 0]} label={<CustomLabel />}>
            {data.map((_, i) => (
              <Cell key={i} fill={COLOR[gender]} fillOpacity={1 - i * 0.08} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      {/* Athlete names below bars */}
      <div className="flex justify-around px-1 mt-1">
        {data.map((d, i) => (
          <div key={i} className="flex flex-col items-center w-0 flex-1">
            <p className="text-[10px] text-white/60 text-center leading-tight truncate w-full px-0.5">{d.name.split(" ").pop()}</p>
            <p className="text-[9px] text-white/30 text-center truncate w-full px-0.5">{d.school}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
