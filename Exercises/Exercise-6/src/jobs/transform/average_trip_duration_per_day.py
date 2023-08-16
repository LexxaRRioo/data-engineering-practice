from pyspark.sql.functions import col, avg, round
from pyspark.sql import DataFrame

def average_trip_duration_per_day(df:DataFrame) -> DataFrame:
    df_res = df.groupBy(col('trip_date')) \
        .agg(round(avg('tripduration'), 2).alias('avg_tripduration')) \
        .orderBy(col('trip_date').asc()) \
        .coalesce(1)
    
    return df_res