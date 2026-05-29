import dagster as dg

from pipeline.assets import qualifying_lists, athlete_profiles, dataframes

defs = dg.Definitions(
    assets=[
        *qualifying_lists.assets,
        *athlete_profiles.assets,
        *dataframes.assets,
    ],
)
