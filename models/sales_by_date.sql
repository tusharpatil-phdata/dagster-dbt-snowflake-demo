select *
from demo_dagster.raw.raw_sales
where sale_date = '{{ var("run_date", "2026-02-15") }}'

