import { useEffect, useState } from "react"
import type { TeamStanding, EventData, Metrics } from "./types"
import Podium from "./components/Podium"
import EventChart from "./components/EventChart"
import MetricsPanel from "./components/MetricsPanel"

const BASE = import.meta.env.VITE_DATA_URL ?? "/data"

const EVENT_LABELS: Record<string, string> = {
  "100": "100m", "200": "200m", "400": "400m", "800": "800m",
  "1500": "1500m", "3000S": "3000m SC", "5000": "5000m", "10k": "10,000m",
  "110H": "110m Hurdles", "100H": "100m Hurdles", "400H": "400m Hurdles",
  "HJ": "High Jump", "PV": "Pole Vault", "LJ": "Long Jump", "TJ": "Triple Jump",
  "SP": "Shot Put", "DT": "Discus", "HT": "Hammer", "JT": "Javelin",
  "4x100": "4x100m Relay", "4x400": "4x400m Relay",
}

const CAT_ORDER = ["sprints", "distance", "field", "relays"]

export default function App() {
  const [standings, setStandings] = useState<TeamStanding[]>([])
  const [events, setEvents]       = useState<EventData[]>([])
  const [metrics, setMetrics]     = useState<Metrics | null>(null)
  const [selected, setSelected]   = useState("__podium__")

  useEffect(() => {
    fetch(`${BASE}/team_standings.json`).then(r => r.json()).then(setStandings)
    fetch(`${BASE}/event_predictions.json`).then(r => r.json()).then(setEvents)
    fetch(`${BASE}/metrics.json`).then(r => r.json()).then(setMetrics)
  }, [])

  const byCategory = CAT_ORDER.map(cat => ({
    cat,
    events: events.filter(e => e.category === cat),
  }))

  const selectedEvent = events.find(e => e.event === selected)

  return (
    <div className="min-h-screen bg-[#0f0f13] text-[#f0eeec] px-6 py-10 max-w-5xl mx-auto">

      <div className="mb-8 text-center">
        <p className="text-xs uppercase tracking-widest text-white/30 mb-1">2026 NCAA D1 Outdoor</p>
        <h1 className="text-3xl font-bold tracking-tight">Track & Field Predictions</h1>
      </div>

      <div className="mb-8 flex justify-center">
        <select
          value={selected}
          onChange={e => setSelected(e.target.value)}
          className="bg-[#1c1c24] border border-white/10 text-white/80 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:ring-1 focus:ring-white/20 min-w-[220px]"
        >
          <option value="__podium__">🏆 Team Standings</option>
          {byCategory.map(({ cat, events: evts }) => (
            <optgroup key={cat} label={cat.charAt(0).toUpperCase() + cat.slice(1)}>
              {evts.map(e => (
                <option key={e.event} value={e.event}>
                  {EVENT_LABELS[e.event] ?? e.event}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
      </div>

      {selected === "__podium__" ? (
        <div key="podium" className="animate-rise flex flex-col md:flex-row gap-12 justify-center">
          <Podium standings={standings} gender="M" />
          <div className="hidden md:block w-px bg-white/10" />
          <Podium standings={standings} gender="W" />
        </div>
      ) : selectedEvent ? (
        <div key={selectedEvent.event} className="animate-rise">
          <h2 className="text-lg font-semibold text-center mb-6 text-white/80">
            {EVENT_LABELS[selectedEvent.event] ?? selectedEvent.event}
          </h2>
          {(() => {
            const hasMen   = selectedEvent.M.length > 0
            const hasWomen = selectedEvent.W.length > 0
            if (hasMen && hasWomen) return (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div className="bg-white/5 border border-white/10 rounded-xl p-5">
                  <EventChart athletes={selectedEvent.M} gender="M" />
                </div>
                <div className="bg-white/5 border border-white/10 rounded-xl p-5">
                  <EventChart athletes={selectedEvent.W} gender="W" />
                </div>
              </div>
            )
            const gender   = hasMen ? "M" : "W"
            const athletes = hasMen ? selectedEvent.M : selectedEvent.W
            return (
              <div className="flex justify-center">
                <div className="bg-white/5 border border-white/10 rounded-xl p-5 w-full max-w-lg">
                  <EventChart athletes={athletes} gender={gender} />
                </div>
              </div>
            )
          })()}
        </div>
      ) : null}

      {metrics && <MetricsPanel metrics={metrics} />}
    </div>
  )
}
