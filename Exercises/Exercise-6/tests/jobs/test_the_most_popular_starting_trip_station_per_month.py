from src.jobs.transform.the_most_popular_starting_trip_station_per_month import the_most_popular_starting_trip_station_per_month

def test_the_most_popular_starting_trip_station_per_month(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = the_most_popular_starting_trip_station_per_month(df)
    output.show(10)

    expected_output = [{'2020-01': 'Station 1'},
                       {'2020-02': 'Station 1'},
                       {'2020-03': 'Station 3'},
                       {'2020-04': None}]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
