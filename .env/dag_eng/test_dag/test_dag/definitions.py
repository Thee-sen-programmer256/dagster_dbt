import os

from dagster import Definitions
from dagster_dbt import DbtCliResource
from dagstermill import ConfigurableLocalOutputNotebookIOManager

from test_dag.assets import dag_eng_dbt_assets,CUSTOMERS,Pyspark_notebook,plot_customers_hist,LOAD_BIG_DATA
from test_dag.constants import dbt_project_dir
from test_dag.schedules import schedules

defs = Definitions(
    assets=[dag_eng_dbt_assets,CUSTOMERS,Pyspark_notebook,plot_customers_hist,LOAD_BIG_DATA],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "output_notebook_io_manager":ConfigurableLocalOutputNotebookIOManager()
    },
)