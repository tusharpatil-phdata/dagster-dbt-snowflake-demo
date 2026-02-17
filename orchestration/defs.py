from dagster import Definitions, DailyPartitionsDefinition, asset
from .resources import dbt_cli

# Daily partitions starting from first SALE_DATE
daily_partitions = DailyPartitionsDefinition(start_date="2026-02-15")

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    """
    For each partition (one day), run dbt only for that date window.
    """
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")
    max_date = time_window.end.strftime("%Y-%m-%d")

    context.log.info(f"Running dbt for window {min_date} -> {max_date}")

    cli_args = [
        "build",
        "--select", "fct_daily_sales",
        "--vars", f"min_date: {min_date}, max_date: {max_date}",
    ]

    # IMPORTANT: no context= here, so dagster-dbt does NOT expect @dbt_assets metadata
    yield from dbt_cli.cli(cli_args).stream()

defs = Definitions(
    assets=[fct_daily_sales_partitioned],
)
