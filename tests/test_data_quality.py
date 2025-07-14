from pyspark.sql import SparkSession

def test_sales_not_negative():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("delta").load("abfss://retailsales@hexastorage145.blob.core.windows.net/silver/train/")
    assert df.filter("Sales < 0").count() == 0
