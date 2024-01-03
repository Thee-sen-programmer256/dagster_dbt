from dagster import AssetExecutionContext,asset,file_relative_path,FreshnessPolicy,MetadataValue
from dagstermill import define_dagstermill_asset
from dagster_dbt import DbtCliResource, dbt_assets
import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path
from test_dag.constants import dbt_manifest_path

#ducdb path
duckdb_path=Path(__file__).parent.joinpath("..","..","engineering.duckdb")



@asset(compute_kind='duckdb',group_name="STAGING")
def CUSTOMERS(context):
    """LOAD CUSTOMER DATA TO THE IN MEMORY DUCK"""
    data=pd.read_csv('/Users/mac/Desktop/Analytics_Enginerring/dagster-dbt/.env/datasets/CUSTOMERS.csv')

    #connect duck
    connection=duckdb.connect(database=str(duckdb_path))
    try:
        connection.execute("create or replace table CUSTOMERS as select * from data ")
        print('exported data successfully')
    except:
        print('Failed to export data')
    print('done')

    context.add_output_metadata({"num_rows":data.shape[0]})


#create an assetfor the notebok 
Pyspark_notebook = define_dagstermill_asset(
    name="Pyspark_Sqlserver",
    notebook_path=file_relative_path(__file__, "../../test.ipynb"),
    deps=['STG_TRANSACTIONS'],group_name="PRODUCTION"
)


@asset(compute_kind='plotly',deps=['STG_TRANSACTIONS'],group_name="PRODUCTION")
def plot_customers_hist(context):
    """PLOT CUSTOMER AMOUNT HIST"""
    #connect duck
    connection=duckdb.connect(database=str(duckdb_path))
    try:
        # df=connection.execute("SELECT * FROM STG_TRANSACTIONS LIMIT 10")
        df=connection.sql("SELECT * FROM STG_TRANSACTIONS").to_df()
        # df=df.fetch_df()
        context.add_output_metadata({"df_len":df.shape[0]})
    except:
        print('Failed to export data')
    print('done')

    fig=px.histogram(df,x="TRANSACTION_AMOUNT")
    customers_path=Path(__file__).parent.joinpath("customers_plot.html")
    fig.write_html(file=customers_path)

    context.add_output_metadata(
        {
            "plot_url":MetadataValue.url("file://"+str(customers_path))
        }
    )


@asset(compute_kind='pyspark',deps=['plot_customers_hist'],group_name="BIG_DATA")
def LOAD_BIG_DATA(context):
    """LOAD 2M RECORDS TO WAREHOUSE"""
    import os
    print('|',"="*10,"Setting the Java Home Env")
    os.environ['JAVA_HOME']='/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home'
    os.environ['SPARK_HOME']='/Users/mac/spark'
    os.environ['PYSPARK_DRIVER_PYTHON_OPTS']= "notebook"

    #importing libraries
    from pyspark import SparkConf,SparkContext
    from pyspark.sql import SparkSession
    import os,duckdb,sys
    import warnings
    from pathlib import Path

    #setting up session
    spark = SparkSession.builder \
                    .appName('Duck to MSSQL') \
                    .config('spark.jars', '/Users/mac/Downloads/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.2.jre11.jar') \
                    .getOrCreate()
    
    people=spark.read.option('delimeter',',').option('header',True).csv('/Users/mac/Downloads/people-2000000.csv')

    properties = {
    "user": "sa",
    "password": "K@roukazeno1",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "true",
    "trustServerCertificate": "true"
    }

    # Correct the JDBC URL
    jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=ENGINEERING"

    try:
        people.write \
        .mode('overwrite') \
        .jdbc(url=jdbc_url, table="PEOPLE", properties=properties)
        context.log.info('! Table loaded successfully')
    except Exception as e:
        print("Error:", e)
        raise e
    finally:
        spark.stop()

    



   


@dbt_assets(manifest=dbt_manifest_path)
def dag_eng_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()