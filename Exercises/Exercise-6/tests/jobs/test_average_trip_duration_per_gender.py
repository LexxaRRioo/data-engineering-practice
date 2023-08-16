from src.jobs.transform.average_trip_duration_per_gender import average_trip_duration_per_gender

def test_average_trip_duration_per_gender(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = average_trip_duration_per_gender(df)
    output.show(10)

    expected_output = [{'W': 200.27},
                       {'M': 166.67}]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
