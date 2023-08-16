from pyspark.sql.functions import to_date, col, lit, when
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

def data_prep(df_19y:DataFrame, df_20y:DataFrame) -> DataFrame:
    rename_cols_20_to_19 = {'started_at': 'start_time',
                            'ended_at': 'end_time',
                            'start_station_name': 'from_station_name',
                            'start_station_id': 'from_station_id',
                            'end_station_name': 'to_station_name',
                            'end_station_id': 'to_station_id'
                            }
        
    for old_name, new_name in rename_cols_20_to_19.items():
        df_20y = df_20y.withColumnRenamed(old_name, new_name)
        
    not_in_df19y = set(df_20y.columns) - set(df_19y.columns)
    for new_column in not_in_df19y:
        df_19y = df_19y.withColumn(new_column, lit(None).cast(StringType()))
        
    not_in_df20y = set(df_19y.columns) - set(df_20y.columns)
    for new_column in not_in_df20y:
        df_20y = df_20y.withColumn(new_column, lit(None).cast(StringType()))
     
    df = df_19y.unionByName(df_20y) 
              
    df = df.withColumn('trip_date',to_date(col('start_time'))) \
        .withColumn('tripduration', 
                    when(col('tripduration').isNull(), 
                        (col('end_time').cast('long') - col('start_time').cast('long')).cast('float')) \
                    .otherwise(col('tripduration')))
        
    return df