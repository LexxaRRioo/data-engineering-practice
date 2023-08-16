from pyspark.sql import SparkSession
from datetime import datetime

from jobs.extract.data_read import data_read
from jobs.transform.data_prep import data_prep
from jobs.load.save_df_into_csv import save_df_into_csv
from jobs.transform.average_trip_duration_per_day import average_trip_duration_per_day
from jobs.transform.count_trips_per_day import count_trips_per_day
from jobs.transform.the_most_popular_starting_trip_station_per_month import the_most_popular_starting_trip_station_per_month
from jobs.transform.top_3_dest_stations_each_day_for_the_last_two_weeks import top_3_dest_stations_each_day_for_the_last_two_weeks
from jobs.transform.average_trip_duration_per_gender import average_trip_duration_per_gender
from jobs.transform.ages_with_longest_and_ages_with_shortest_trips_10_each import ages_with_longest_and_ages_with_shortest_trips_10_each
    

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    
    df_19y, df_20y = data_read(spark)
    
    df = data_prep(df_19y, df_20y)

    # 1. What is the `average` trip duration per day?        
    df_1 = average_trip_duration_per_day(df)
    save_df_into_csv(df_1, '/app/reports/task1')
    
    # 2. How many trips were taken each day?
    df_2 = count_trips_per_day(df)
    save_df_into_csv(df_2, '/app/reports/task2')
        
    # 3. What was the most popular starting trip station for each month?
    df_3 = the_most_popular_starting_trip_station_per_month(df)
    save_df_into_csv(df_3, '/app/reports/task3')
        
    # 4. What were the top 3 trip stations each day for the last two weeks?
    # Let's say we want to query destination stations
    df_4 = top_3_dest_stations_each_day_for_the_last_two_weeks(df) 
    save_df_into_csv(df_4, '/app/reports/task4')   
    
    # 5. Do `Male`s or `Female`s take longer trips on average?
    df_5 = average_trip_duration_per_gender(df)
    save_df_into_csv(df_5, '/app/reports/task5')
    
    # 6. What is the top 10 ages of those that take the longest trips, and shortest?
    # The task vaguely describes what should be done,
    # so I interpert it as the following:
    # . count average trip duration by every age are presented in the dataset
    # . show distinct (top 10 union bottom 10)
    df_6 = ages_with_longest_and_ages_with_shortest_trips_10_each(df)
    save_df_into_csv(df_6, '/app/reports/task6')

if __name__ == "__main__":
    main()
