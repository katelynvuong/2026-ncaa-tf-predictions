export interface Scorer {
  event: string
  athlete_name: string
  points: number
}

export interface TeamStanding {
  gender: "M" | "W"
  rank: number
  school: string
  logo_url: string | null
  total_points: number
  scorers: Scorer[]
}

export interface Athlete {
  rank: number
  bar_height: number
  athlete_name: string | null
  school: string
  predicted_place: number
}

export interface EventData {
  event: string
  category: "sprints" | "distance" | "field" | "other"
  M: Athlete[]
  W: Athlete[]
}

export interface FeatureImportance {
  feature: string
  importance: number
  label: string
}

export interface TeamCategoryBreakdown {
  gender: "M" | "W"
  rank: number
  school: string
  logo_url: string | null
  total_points: number
  sprints: number
  distance: number
  field: number
  relays: number
}

export interface Analytics {
  feature_importances: FeatureImportance[]
  team_category_breakdown: TeamCategoryBreakdown[]
  regional_split: { east: number; west: number; total: number }
}

export interface Metrics {
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
  training_rows: number
  years_trained: number[]
  relay_excluded: boolean
  events_count: number
  model_type: string
  model_note: string
}
