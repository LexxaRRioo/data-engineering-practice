from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType, ShortType
from pyspark.sql import DataFrame

def data_read(spark) -> (DataFrame, DataFrame):
    schema_19 = StructType([StructField('trip_id', IntegerType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('bikeid', IntegerType(), True),
        StructField('tripduration', FloatType(), True),
        StructField('from_station_id', IntegerType(), True),
        StructField('from_station_name', StringType(), True),
        StructField('to_station_id', IntegerType(), True),
        StructField('to_station_name', StringType(), True),
        StructField('usertype', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('birthyear', ShortType(), True)]
    )
    
    schema_20 = StructType([StructField('ride_id', StringType(), True),
        StructField('rideable_type', StringType(), True),
        StructField('started_at', TimestampType(), True),
        StructField('ended_at', TimestampType(), True),
        StructField('start_station_name', StringType(), True),
        StructField('start_station_id', IntegerType(), True),
        StructField('end_station_name', StringType(), True),
        StructField('end_station_id', IntegerType(), True),
        StructField('start_lat', FloatType(), True),
        StructField('start_lng', FloatType(), True),
        StructField('end_lat', FloatType(), True),
        StructField('end_lng', FloatType(), True),
        StructField('member_casual', StringType(), True)]
    )
    
    df_19y = spark.read.schema(schema_19).options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/data/Divvy_Trips_2019_Q4.csv")
    
    df_20y = spark.read.schema(schema_20).options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/data/Divvy_Trips_2020_Q1.csv")
        
    return df_19y, df_20y