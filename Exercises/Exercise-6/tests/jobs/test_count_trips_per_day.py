from src.jobs.transform.count_trips_per_day import count_trips_per_day

def test_count_trips_per_day(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = count_trips_per_day(df)
    output.show(10)

    expected_output = [{'2020-01-03': 3},
                       {'2020-01-01': 2},
                       {'2020-01-02': 1},
                       {'2020-01-04': 0},
                       {'2020-02-01': 0},
                       {'2020-03-01': 0},
                       {'2020-04-01': 0}
                       ]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
