export interface TeamStanding {
  gender: "M" | "W"
  rank: number
  school: string
  logo_url: string | null
  total_points: number
  sprints_pts: number
  distance_pts: number
  field_pts: number
}

export interface Athlete {
  rank: number
  bar_height: number
  athlete_name: string
  school: string
  predicted_place: number
}

export interface EventData {
  event: string
  category: "sprints" | "distance" | "field" | "other"
  M: Athlete[]
  W: Athlete[]
}

export interface Metrics {
  mae: number
  rmse: number
  spearman: number
  top3_hit_rate: number
  top3_hits: number
  top3_total: number
  training_rows: number
  years_trained: number[]
  relay_excluded: boolean
  events_count: number
}
