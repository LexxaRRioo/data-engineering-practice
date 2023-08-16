from pyspark.sql.functions import col, date_format, row_number, count
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

def the_most_popular_starting_trip_station_per_month(df:DataFrame) -> DataFrame:
    wnd_trips = Window.partitionBy('trip_month') \
        .orderBy(col('trips_cnt_by_station_nm').desc(), col('from_station_name').asc())
    
    df_res = df.withColumn('trip_month', date_format(col('trip_date'), 'yyyy-MM')) \
        .groupBy(col('trip_month'), col('from_station_name')) \
        .agg(count(col('trip_month')).alias('trips_cnt_by_station_nm')) \
        .withColumn('row_num',row_number().over(wnd_trips)) \
        .filter(col('row_num') == 1) \
        .drop(col('row_num')) \
        .coalesce(1)
    
    return df_res