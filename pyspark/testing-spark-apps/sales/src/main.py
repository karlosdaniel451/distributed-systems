from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder \
        .appName('Analysis of sells in stores') \
        .getOrCreate()


    sales_schema = T.StructType(
        [
            T.StructField('date', T.DateType()),
            T.StructField('store_location', T.StringType()),
            T.StructField('number_of_sales', T.IntegerType())
        ]
    )

    sales = spark.read.csv(
        './data/sales.csv',
        sep=',',
        header=True,
        schema=sales_schema
    )

    sales.show()

    total_of_sales_by_location = get_total_of_sales_by_location(sales)
    total_of_sales_by_date = get_total_of_sales_by_date(sales)


    total_of_sales_by_location.coalesce(1).write.csv(
        './data/total_sales_by_location.csv',
        mode='overwrite',
        header=True
    )

    total_of_sales_by_date.coalesce(1).write.csv(
        './data/total_sales_by_date.csv',
        mode='overwrite',
        header=True
    )


def get_total_of_sales_by_location(sales: DataFrame) -> DataFrame:
    """Get the sum of sales by location."""
    return sales.groupBy('store_location') \
        .agg(F.sum('number_of_sales').alias('total_of_sales')) \
        .orderBy('store_location', ascending=True)


def get_total_of_sales_by_date(sales: DataFrame) -> DataFrame:
    """Get the sum of sales by date."""
    return sales.groupBy('date') \
        .agg(F.sum('number_of_sales').alias('total_of_sales')) \
        .orderBy('date', ascending=True)


if __name__ == '__main__':
    main()
