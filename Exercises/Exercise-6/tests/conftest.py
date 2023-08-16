from pyspark.sql import SparkSession

import pytest


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Exercise6-test").enableHiveSupport().getOrCreate()
    yield spark
    spark.stop()