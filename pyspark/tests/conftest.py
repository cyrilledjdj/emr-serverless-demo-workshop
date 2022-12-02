import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def mock_views_df():
    # Create a local Spark session, then read and return our mock data
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("tests")
        .config("spark.ui.enabled", False)
        .getOrCreate()
    )
    return spark.read.json("./tests/sample-data.json")
