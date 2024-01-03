"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection
from  dagster import ScheduleDefinition,define_asset_job,AssetSelection

from test_dag.assets import dag_eng_dbt_assets,CUSTOMERS,Pyspark_notebook,plot_customers_hist,LOAD_BIG_DATA




schedules = [
    ScheduleDefinition(
        job=define_asset_job(
        name="MATERIALIZE_DBT_DAGSTER_ASSETS",
        selection=AssetSelection.all(),
        ),
        cron_schedule="1/5 * * * *",
    )
]