from pathlib import Path
from dagster_dbt import DbtCliResource

# Repo root (dbt project root: has dbt_project.yml)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# dbt CLI resource that uses profiles.yml in the repo root
dbt_cli = DbtCliResource(
    project_dir=PROJECT_ROOT,
    profiles_dir=PROJECT_ROOT,
    target="dev",
)
