from pyspark.sql.functions import col, year, avg, round
from pyspark.sql import DataFrame

def ages_with_longest_and_ages_with_shortest_trips_10_each(df:DataFrame) -> DataFrame:
    df_prep = df.withColumn('age', year(col('trip_date')) - year(col('birthyear'))) \
        .filter(col('age').isNotNull()) \
        .groupBy(col('age')) \
        .agg(round(avg(col('tripduration')), 2).alias('avg_trips_duration_by_age_in_sec')) \
           
    df_top = df_prep.orderBy(col('avg_trips_duration_by_age_in_sec').desc()) \
        .limit(10) \
        .coalesce(1)
           
    df_bottom = df_prep.orderBy(col('avg_trips_duration_by_age_in_sec').asc()) \
        .limit(10) \
        .coalesce(1)
        
    df_res = df_top.unionAll(df_bottom).distinct().coalesce(1)
    
    return df_res