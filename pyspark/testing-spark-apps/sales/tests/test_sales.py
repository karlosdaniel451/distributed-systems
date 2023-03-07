import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from src.main import get_total_of_sales_by_date, get_total_of_sales_by_location


@pytest.fixture(scope='session')
def sales_data(spark):
    data = [
        {'date': '2023-01-01', 'store_location': 'New York',
         'number_of_sales': 10},
        {'date': '2023-01-01', 'store_location': 'Shanghai',
         'number_of_sales': 9},
        {'date': '2023-01-01', 'store_location': 'London',
         'number_of_sales': 11},
        {'date': '2023-01-01', 'store_location': 'Paris',
         'number_of_sales': 8},
        {'date': '2023-01-02', 'store_location': 'New York',
         'number_of_sales': 13},
        {'date': '2023-01-02', 'store_location': 'Shanghai',
         'number_of_sales': 9},
        {'date': '2023-01-02', 'store_location': 'London',
         'number_of_sales': 12},
        {'date': '2023-01-02', 'store_location': 'Paris',
         'number_of_sales': 14},
        {'date': '2023-01-03', 'store_location': 'New York',
         'number_of_sales': 15},
        {'date': '2023-01-03', 'store_location': 'Shanghai',
         'number_of_sales': 9},
        {'date': '2023-01-03', 'store_location': 'London',
         'number_of_sales': 8},
        {'date': '2023-01-03', 'store_location': 'Paris',
         'number_of_sales': 12},
        {'date': '2023-01-04', 'store_location': 'New York',
         'number_of_sales': 8},
        {'date': '2023-01-04', 'store_location': 'Shanghai',
         'number_of_sales': 12},
        {'date': '2023-01-04', 'store_location': 'London',
         'number_of_sales': 11},
        {'date': '2023-01-04', 'store_location': 'Paris',
         'number_of_sales': 9},

    ]

    return spark.createDataFrame(Row(**row) for row in data)


def test_get_total_of_sales_by_date(spark: SparkSession, sales_data: DataFrame):
    expected_data = [
        {'date': '2023-01-01', 'total_of_sales': 38},
        {'date': '2023-01-02', 'total_of_sales': 48},
        {'date': '2023-01-03', 'total_of_sales': 44},
        {'date': '2023-01-04', 'total_of_sales': 40},
    ]

    expected_result = spark.createDataFrame([Row(**row) for row in expected_data],
        schema=['date', 'total_of_sales'])

    result = get_total_of_sales_by_date(sales_data)

    assert result.count() == 4
    assert expected_result.collect() == result.collect()


def test_get_total_of_sales_by_location(spark: SparkSession, sales_data: DataFrame):
    expected_data = [
        {'store_location': 'London', 'total_of_sales': 42},
        {'store_location': 'New York', 'total_of_sales': 46},
        {'store_location': 'Paris', 'total_of_sales': 43},
        {'store_location': 'Shanghai', 'total_of_sales': 39},
    ]

    expected_result = spark.createDataFrame([Row(**row) for row in expected_data],
        schema=['store_location', 'total_of_sales'])

    result = get_total_of_sales_by_location(sales_data)

    assert result.count() == 4
    assert expected_result.collect() == result.collect()
