import dagster as dg

region_gender_partitions = dg.StaticPartitionsDefinition(
    ["east_f", "east_m", "west_f", "west_m"]
)
