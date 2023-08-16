from pyspark.sql.functions import col, count
from pyspark.sql import DataFrame

def count_trips_per_day(df:DataFrame) -> DataFrame:
    df_res = df.groupBy(col('trip_date')) \
        .agg(count('tripduration').alias('count_trips')) \
        .orderBy(col('count_trips').desc(), col('trip_date').asc()) \
        .coalesce(1)
    
    return df_res