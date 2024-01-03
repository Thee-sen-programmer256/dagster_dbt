from setuptools import find_packages, setup

setup(
    name="test_dag",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-core>=1.4.0",
        "dbt-duckdb",
        "dbt-sqlserver","pandas","duckdb","dagstermill","plotly-express"
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)