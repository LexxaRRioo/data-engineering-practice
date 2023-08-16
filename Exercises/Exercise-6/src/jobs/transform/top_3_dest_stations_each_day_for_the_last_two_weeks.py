from pyspark.sql.functions import col, row_number, count, lit, date_sub
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime

def top_3_dest_stations_each_day_for_the_last_two_weeks(df:DataFrame) -> DataFrame:
    test_current_date = lit(datetime(2020,1,6)) # current_date()
    wnd_trips = Window.partitionBy('trip_date') \
        .orderBy(col('trips_cnt_by_station_nm').desc(), col('to_station_name').asc())
    
    df_res = df.filter(col('trip_date').cast('date') > date_sub(test_current_date, 14)) \
        .groupBy(col('trip_date'), col('to_station_name')) \
        .agg(count(col('trip_date')).alias('trips_cnt_by_station_nm')) \
        .withColumn('row_num',row_number().over(wnd_trips)) \
        .filter(col('row_num') <= 3) \
        .drop(col('row_num')) \
        .orderBy(col('trip_date'),col('trips_cnt_by_station_nm').desc()) \
        .coalesce(1)
    
    return df_res