import dagster as dg

from pipeline.assets import qualifying_lists, athlete_profiles, dataframes, final_athletes, features

defs = dg.Definitions(
    assets=[
        *qualifying_lists.assets,
        *athlete_profiles.assets,  # includes supplemental_profiles
        *dataframes.assets,
        *final_athletes.assets,
        *features.assets,
    ],
)
