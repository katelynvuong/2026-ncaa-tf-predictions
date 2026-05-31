import dagster as dg

from pipeline.assets import regional_lists, athlete_profiles, dataframes, final_athletes, features

defs = dg.Definitions(
    assets=[
        *regional_lists.assets,
        *athlete_profiles.assets,  # includes supplemental_profiles
        *dataframes.assets,
        *final_athletes.assets,
        *features.assets,
    ],
)
