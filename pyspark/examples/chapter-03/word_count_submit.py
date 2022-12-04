from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName('Counting word ocurrencies of some public domain books') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

word_counting = spark.read.text('./data/gutenberg_books/*.txt') \
    .select(F.split(F.col('value'), ' ').alias('line')) \
    .select(F.explode(F.col('line')).alias('word')) \
    .select(F.lower(F.col('word')).alias('word')) \
    .select(F.regexp_extract(F.col('word'), '[a-z]+', 0).alias('word')) \
    .where(F.col('word') != '') \
    .groupBy('word') \
    .count() \
    .orderBy('count', ascending=False)

word_counting.show()

word_counting.coalesce(1).write.csv('./data/word_counting_results.csv')
