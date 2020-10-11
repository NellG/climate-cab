import csv
import boto3
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


def make_schema(schema_file):
    """Define schema based on .csv input."""
    with open(schema_file, newline='') as f:
        reader = csv.reader(f)
        schema_cols = list(reader)
    schema = StructType()
    for i, name in enumerate(schema_cols[0]):
        schema.add(name, schema_cols[1][i], True) 
    return schema


def read_bucket(spark, bucket, folder):
    """Read a csv files in bucket/folder into dataFrame."""
    s3 = boto3.resource('s3')
    s3_path = 's3a://'+bucket+'/'
    s3_bucket = s3.Bucket(bucket)
    schema = folder + '_schema.csv'
    
    files = []
    for obj in s3_bucket.objects.filter(Prefix=folder):
        if obj.key[-4:] == '.csv':
            files.append(s3_path + obj.key)

    df = spark.read \
        .option('header', True) \
        .schema(make_schema(schema)) \
        .csv(files)
    return df


def persist_cabs(cabs):
    """Filter, calculate columns, partition on start time and cache cab df."""
    cabs = cabs \
        .filter(cabs.trip_tot < 500) \
        .select(['taxi', 'start_str', 'comm_pick', 'dur', 
                 'dist', 'fare', 'tip', 'extra']) \
        .fillna(0, subset=['fare', 'tip', 'extra', 'dur', 'dist'])
    cabs = cabs \
        .withColumn('startrnd', sf.date_trunc("Hour", 
            sf.to_timestamp(cabs.start_str, 'MM/dd/yyyy hh:mm:ss aa'))) \
        .withColumn('total', cabs.fare + cabs.tip + cabs.extra) \
        .drop('start_str', 'fare', 'tip', 'extra')
    cabs = cabs \
        .withColumn('permile', 
            sf.when(cabs.dist > 0.2, sf.least(cabs.total / cabs.dist, sf.lit(20))) \
              .otherwise(sf.lit(4))) \
        .withColumn('permin', 
            sf.when(cabs.dur > 1, sf.least(cabs.total / (cabs.dur/60), sf.lit(5))) \
              .otherwise(sf.lit(1))) \
        .drop('dur', 'dist')

    cabs = cabs.repartition(200, 'startrnd') \
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    return cabs
    

def aggregate_cabs(cabs, cols):
    """Calculate aggregate cab metrics."""
    cab_agg = cabs \
        .groupBy(cols) \
        .agg(sf.countDistinct('taxi').alias('taxis'),
             sf.sum('total').alias('sum_fares'),
             sf.mean('permile').alias('avg_permile'),
             sf.mean('permin').alias('avg_permin'),
             sf.count(sf.lit(1)).alias('rides'))
    cab_agg = cab_agg \
        .withColumn('d_hr_cab', cab_agg.sum_fares/cab_agg.taxis) \
        .withColumn('avg_perride', cab_agg.sum_fares/cab_agg.rides)
    return cab_agg


def persist_weather(wthr):
    """ Create and cache dataframe of weather data.

    Round hourly temp to nearest 10 def F
    Divide rainfall into none, light (<=0.2 in), and heavy bins
    Round timestamp to nearest hour
    """
    wthr = wthr \
        .select('date', 'tdry', 'precip') \
        .filter(wthr.station == '72534014819') \
        .filter(wthr.report == 'FM-15') \
        .fillna({'precip':0})
    wthr = wthr \
        .withColumn('trnd', sf.round(wthr.tdry/10)*10) \
        .withColumn('prnd', sf.when(wthr.precip == 0, 0) \
                              .when(wthr.precip.between(0,0.2), 0.2) \
                              .otherwise(1)) \
        .withColumn('timernd', sf.date_trunc("Hour", wthr.date)) \
        .withColumn('day', (sf.date_format(wthr.date, 'u')).cast('int')) \
        .withColumn('hour', sf.hour(wthr.date)) \
        .drop('tdry', 'precip', 'date')
    wthr = wthr.cache()
    return wthr


def agg_cabs_and_wthr(cabs, wthr):
    """ Return joined, aggregated dataframe with cab and weather data."""
    combo = cabs \
        .join(sf.broadcast(wthr), cabs.startrnd == wthr.timernd) \
        .drop('startrnd', 'timernd')

    # check for comm_pick column and group accordingly
    try:
        combo['comm_pick']
        group = ['trnd', 'prnd', 'day', 'hour', 'comm_pick']
    except AnalysisException:
        group = ['trnd', 'prnd', 'day', 'hour']

    hist = combo \
        .groupBy(group) \
        .agg(sf.mean('taxis').alias('taxis'),
            sf.mean('d_hr_cab').alias('d_hr_cab'),
            sf.mean('avg_permile').alias('d_mile'),
            sf.mean('avg_permin').alias('d_min'),
            sf.mean('rides').alias('rides'),
            sf.mean('avg_perride').alias('d_ride'),
            sf.count(sf.lit(1)).alias('avged_over'))
    return hist


def write_table(df, table):
    """Write history table to postgresql database."""
    with open('/home/ubuntu/code/.spark-config.csv') as infile:
        reader = csv.reader(infile)
        config = {row[0]: row[1] for row in reader}
    dburl = config['dburl']
    user = config['user']
    password = config['password']
    driver = "org.postgresql.Driver"

    df.write.option('truncate', 'true') \
        .jdbc(dburl, table, mode = 'overwrite', 
            properties={"user":user,
                        "password":password,
                        "driver":driver})


if __name__ == '__main__':
    # set spark configuration and suppress info messages
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.executor.memory', '2g') \
        .set('spark.executor.cores', 2) \
        .set('spark.sql.files.maxPartitionBytes', 128*1024*1024) \
        .set('spark.sql.shuffle.partitions', 64)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession \
        .builder \
        .appName('cabhistory') \
        .getOrCreate()

    # name of S3 bucket and folders containing data
    bucket = 'chi-cab-bucket'
    cab_folder = 'taxi'
    wthr_folder = 'weather'

    # define years of interest and read in data
    cabs = persist_cabs(read_bucket(spark, bucket, cab_folder))
    wthr = persist_weather(read_bucket(spark, bucket, wthr_folder))

    # save summary table for community areas
    cabs_area = aggregate_cabs(cabs, ['startrnd', 'comm_pick'])
    hist_area = agg_cabs_and_wthr(cabs_area, wthr)
    write_table(hist_area, 'areahistory')

    # save summary table for whole city
    cabs_city = aggregate_cabs(cabs, ['startrnd'])
    hist_city = agg_cabs_and_wthr(cabs_city, wthr)
    write_table(hist_city, 'cityhistory')