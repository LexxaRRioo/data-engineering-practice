from src.jobs.transform.average_trip_duration_per_day import average_trip_duration_per_day

def test_average_trip_duration_per_day(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = average_trip_duration_per_day(df)
    output.show(10)

    expected_output = [{'2020-01-01': 150.0},
                       {'2020-01-02': 200.0},
                       {'2020-01-03': 200.27},
                       {'2020-01-04': None},
                       {'2020-02-01': None},
                       {'2020-03-01': None},
                       {'2020-04-01': None}
                       ]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
