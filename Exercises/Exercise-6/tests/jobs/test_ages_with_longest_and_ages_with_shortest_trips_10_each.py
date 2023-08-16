from src.jobs.transform.ages_with_longest_and_ages_with_shortest_trips_10_each import ages_with_longest_and_ages_with_shortest_trips_10_each

def test_ages_with_longest_and_ages_with_shortest_trips_10_each(spark):
    df = spark.read.options(header='true', delimiter=',', encoding='utf-8', quotes='"') \
        .csv("/app/tests/resources/data.csv")

    output = ages_with_longest_and_ages_with_shortest_trips_10_each(df)
    output.show(10)

    expected_output = [{40: 250.35},
                       {50: None},
                       {30: 200.0},
                       {20: 133.37},
                       ]

    assert output.count() == len(expected_output)
    assert [{row[0]: row[1]} for row in output.collect()] == expected_output
