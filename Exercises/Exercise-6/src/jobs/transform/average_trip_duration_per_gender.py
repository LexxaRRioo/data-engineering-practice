from pyspark.sql.functions import col, avg, round
from pyspark.sql import DataFrame

def average_trip_duration_per_gender(df:DataFrame) -> DataFrame:
    df_res = df.filter(col('gender').isNotNull()) \
        .groupBy(col('gender')) \
        .agg(round(avg(col('tripduration')), 2).alias('avg_trips_duration_by_gender_in_sec')) \
        .orderBy(col('avg_trips_duration_by_gender_in_sec').desc()) \
        .coalesce(1)
    
    return df_res