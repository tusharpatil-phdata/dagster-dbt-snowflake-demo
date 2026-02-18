from dagster import Definitions, DailyPartitionsDefinition, asset, Output
from .resources import dbt_cli

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-02-15",
    timezone="Asia/Kolkata",
    end_offset=1,  # include today’s partition
)

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")
    max_date = time_window.end.strftime("%Y-%m-%d")

    context.log.info(f"Running dbt for window {min_date} -> {max_date}")

    # vars string in the same format that worked in dbt Cloud
    vars_arg = f'{{min_date: "{min_date}", max_date: "{max_date}"}}'

    cli_args = [
        "build",
        "--select", "fct_daily_sales",
        "--vars", vars_arg,
    ]

    context.log.info(f"dbt CLI args: {cli_args}")

    # IMPORTANT: no context= here, since this is NOT @dbt_assets
    yield from dbt_cli.cli(cli_args).stream()

    yield Output(value=None)

defs = Definitions(assets=[fct_daily_sales_partitioned])
