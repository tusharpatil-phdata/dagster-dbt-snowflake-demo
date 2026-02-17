from dagster import Definitions, DailyPartitionsDefinition, asset
from .resources import dbt_cli

daily_partitions = DailyPartitionsDefinition(start_date="2026-02-15")

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")
    max_date = time_window.end.strftime("%Y-%m-%d")

    context.log.info(f"Running dbt for window {min_date} -> {max_date}")

    # dbt expects vars as a YAML/JSON-like string
    vars_arg = f'{{min_date: "{min_date}", max_date: "{max_date}"}}'

    cli_args = [
        "build",
        "--select",
        "fct_daily_sales",
        "--target",
        "dev",
        "--vars",
        vars_arg,
    ]

    # No context= here (plain CLI run)
    yield from dbt_cli.cli(cli_args).stream()

defs = Definitions(
    assets=[fct_daily_sales_partitioned],
)
