from dagster import Definitions, DailyPartitionsDefinition, asset, Output
from .resources import dbt_cli
import json

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-02-15",
    timezone="Asia/Kolkata",
    end_offset=1,
)

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")
    max_date = time_window.end.strftime("%Y-%m-%d")

    context.log.info(f"Running dbt for window {min_date} -> {max_date}")

    # build vars exactly like the manual CLI that worked
    vars_arg = json.dumps({"min_date": min_date, "max_date": max_date})

    cli_args = [
        "build",
        "--select",
        "fct_daily_sales",
        "--vars",
        vars_arg,
    ]

    context.log.info(f"dbt CLI args: {cli_args}")

    yield from dbt_cli.cli(cli_args).stream()
    yield Output(value=None)

defs = Definitions(assets=[fct_daily_sales_partitioned])
