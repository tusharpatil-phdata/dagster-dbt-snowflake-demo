from dagster import Definitions, DailyPartitionsDefinition, asset, Output, Failure
from pathlib import Path
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-02-15",
    timezone="Asia/Kolkata",
    end_offset=1,  # include today's partition
)

@asset(partitions_def=daily_partitions)
def fct_daily_sales_partitioned(context):
    # 1) Determine the date window for this partition
    time_window = context.partition_time_window
    min_date = time_window.start.strftime("%Y-%m-%d")
    max_date = time_window.end.strftime("%Y-%m-%d")

    context.log.info(f"Running dbt for window {min_date} -> {max_date}")

    # 2) Build the vars string EXACTLY like the manual dbt command that worked
    vars_arg = f'{{min_date: "{min_date}", max_date: "{max_date}"}}'

    # 3) Build the dbt CLI command (same as you ran in dbt Cloud)
    cmd = [
        "dbt",
        "build",
        "--project-dir", str(PROJECT_ROOT),
        "--profiles-dir", str(PROJECT_ROOT),
        "--target", "dev",
        "--select", "fct_daily_sales",
        "--vars", vars_arg,
    ]

    context.log.info("Running dbt CLI: " + " ".join(cmd))

    # 4) Run dbt as a subprocess
    result = subprocess.run(cmd, capture_output=True, text=True)

    context.log.info("dbt stdout:\n" + result.stdout)
    if result.returncode != 0:
        context.log.error("dbt stderr:\n" + result.stderr)
        raise Failure(f"dbt failed with return code {result.returncode}")

    # 5) Emit a dummy output so Dagster marks the asset as materialized
    yield Output(value=None)

defs = Definitions(
    assets=[fct_daily_sales_partitioned],
)
