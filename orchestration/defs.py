from dagster import Definitions, DailyPartitionsDefinition, asset
from .resources import dbt_cli

# Daily partitions starting from first SALE_DATE in your sample data
daily_partitions = DailyPartitionsDefinition(start_date="2026-02-15")

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    """
    For each partition (one day), run dbt only for that date window.
    """
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")  # e.g. 2026-02-15
    max_date = time_window.end.strftime("%Y-%m-%d")    # e.g. 2026-02-16

    cli_args = [
        "build",
        "--select", "fct_daily_sales",
        "--vars", f"min_date: {min_date}, max_date: {max_date}",
    ]

    # Run dbt for this one-day window, stream logs to Dagster
    yield from dbt_cli.cli(cli_args, context=context).stream()

defs = Definitions(
    assets=[fct_daily_sales_partitioned],
)
