from src.jobs.transform.top_3_dest_stations_each_day_for_the_last_two_weeks import top_3_dest_stations_each_day_for_the_last_two_weeks

def test_top_3_dest_stations_each_day_for_the_last_two_weeks(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = top_3_dest_stations_each_day_for_the_last_two_weeks(df)
    output.show(10)

    expected_output = [{'2020-01-01': 'Station 1'},
                       {'2020-01-01': 'Station 2'},
                       {'2020-01-02': 'Station 1'},
                       {'2020-01-03': 'Station 2'},
                       {'2020-01-03': 'Station 1'},
                       {'2020-01-04': 'Station 4'}
                       ]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
