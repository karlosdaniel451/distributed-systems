from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName('Chapter 05 Examples') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


logs = spark.read.csv(
   '../../data/broadcast_logs/BroadcastLogs_2018_Q3_M8.CSV',
   sep='|',
   header=True,
   inferSchema=True,
   timestampFormat='yyyy-MM-dd'
)

# Create column of DurationInSeconds..
logs = logs.withColumn(
    'DurationInSeconds',
    F.col('Duration').substr(1, 2).cast('int') * 60 * 60  # For hours
    + F.col('Duration').substr(4, 2).cast('int') * 60  # For minutes
    + F.col('Duration').substr(7, 2).cast('int')  # For seconds
)


logs_identifier = spark.read.csv(
    '../../data/broadcast_logs/ReferenceTables/LogIdentifier.csv',
    sep='|',
    header=True,
    inferSchema=True
)

## Filter for primary channels.
logs_identifier = logs_identifier.where(F.col('PrimaryFG') == 1)


cd_category = spark.read.csv(
    '../../data/broadcast_logs/ReferenceTables/CD_Category.csv',
    sep='|',
    header=True,
    inferSchema=True
).select(
    'CategoryID',
    'CategoryCD',
    F.col('EnglishDescription').alias('CategoryDescription')
)

cd_program_class = spark.read.csv(
    '../../data/broadcast_logs/ReferenceTables/CD_ProgramClass.csv',
    sep='|',
    header=True,
    inferSchema=True
).select(
    'ProgramClassID',
    'ProgramClassCD',
    F.col('EnglishDescription').alias('ProgramClassDescription')
)

call_signs = spark.read.csv(
    '../../data/broadcast_logs/Call_Signs.csv',
    sep=',',
    header=True,
    inferSchema=True
).drop('UndertakingNO')


# Join DataFrames.

logs_and_channels = logs.join(
    logs_identifier,
    on='LogServiceID',
    how='inner'
)

full_logs = logs_and_channels.join(
    cd_category,
    on='CategoryID',
    how='left'
).join(
    cd_program_class,
    on='ProgramClassID',
    how='left'
)


# Compute the commercial ratio by channel and drop the records with NA
# value for `CommercialRatio``.

commercial_codes = ['COM', 'PRC', 'PGI', 'PRO', 'PSA', 'MAG', 'LOC',
    'SPO', 'MER', 'SOL']

commercial_ratio_by_channel = full_logs.groupBy('LogIdentifierID') \
    .agg(
        F.sum(
            F.when(
                F.trim(F.col('ProgramClassCD')).isin(commercial_codes),
                F.col('DurationInSeconds')
            ).otherwise(0)
        ).alias('DurationCommercial'),
        F.sum('DurationInSeconds').alias('SumOfDurationInSeconds')
    ).withColumn(
        'CommercialRatio',
        F.col('DurationCommercial') / F.col('SumOfDurationInSeconds')
    ).dropna(subset=['CommercialRatio']) \
    .orderBy('CommercialRatio', ascending=False) \


# Join with `call_signs` to include the name of the channel and make
# the results more human readable.

commercial_ratio_by_channel = commercial_ratio_by_channel.join(
    call_signs,
    on='LogIdentifierID',
    how='left'
)


# Write commercial ratios to STDOUT and to a CSV file.

commercial_ratio_by_channel.show(
    commercial_ratio_by_channel.count(),
    truncate=False
)

commercial_ratio_by_channel.coalesce(1).write.csv(
    '../../data/commercial_ratio_more_human_readable_results.csv',
    mode='overwrite',
    sep='|',
    header=True
)
