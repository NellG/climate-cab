# launch pyspark interpreter with:
# usr/local/spark/bin/pyspark --master spark://privateip:7077 --packages/
# org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7

# launch script with:
# spark-submit 
# --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7 
# --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true
# --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true
# --master spark://10.0.0.14:7077 spark.py
import ast
import csv
import time
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime
from pyspark.sql.types import StructType


def make_cab_table(years, verb = False):
    """ Create dataframe of cab data.

    Round timestamp to nearest hour
    Calculate total cost without tolls
    Calculate $/mile
    Calculate $/min
    Aggregate into hourly averages
    """

    # load cab table schema
    with open('cab_schema.csv', newline='') as f:
        reader = csv.reader(f)
        cab_cols = list(reader)
    schema = StructType()
    for i, name in enumerate(cab_cols[0]):
        schema.add(name, cab_cols[1][i], True) 

    # load cab data
    cabfiles = ['chi_20'+n+'.csv' for n in years]
    cabbucket = 's3a://chi-cab-bucket/taxi/'
    cabpaths = [cabbucket + f for f in cabfiles]
    cabs = spark.read.option('header', True).schema(schema).csv(cabpaths)
    if verb: print('Cab data table has', cabs.count(), 'rows.')

    cabs = cabs \
        .select(['taxi', 'start_str', 'dur', 'dist', 'fare', 'tip', 'extra']) \
        .fillna(0, subset=['fare', 'tip', 'extra']) \
        .fillna(1, subset=['dur', 'dist'])
    cabs = cabs \
        .withColumn('startrnd', sf.date_trunc("Hour", 
            sf.to_timestamp(cabs.start_str, 'MM/dd/yyyy hh:mm:ss aa'))) \
        .withColumn('total', cabs.fare + cabs.tip + cabs.extra) \
        .drop('fare', 'tip', 'extra', 'start_str')
    cabs = cabs \
        .withColumn('permile', cabs.total / cabs.dist) \
        .withColumn('permin', cabs.total / cabs.dur * 60) \
        .drop('dist', 'dur')
    if verb: cabs.printSchema()
    if verb: print(cabs.sort('start_str', ascending = False).head(5))

    cab_agg = cabs \
        .groupBy('startrnd') \
        .agg(sf.countDistinct('taxi').alias('taxis'),
             sf.sum('total').alias('sum_fares'),
             sf.mean('permile').alias('avg_permile'),
             sf.mean('permin').alias('avg_permin'),
             sf.count(sf.lit(1)).alias('rides'))
    cab_agg = cab_agg \
        .withColumn('d_hr_cab', cab_agg.sum_fares/cab_agg.taxis)
    if verb: cab_agg.sort('taxis', ascending = False).show()

    return cab_agg


def make_weather_table(years, verb = False):
    """ Create dataframe of weather data.

    Round hourly temp to nearest 10 def F
    Ceil hourly precipitation to nearest 0.2 in
    Round timestamp to nearest hour
    """

    # load weather table schema
    with open('weather_schema.csv', newline='') as f:
        reader = csv.reader(f)
        wthr_cols = list(reader)
    schema = StructType()
    for i, name in enumerate(wthr_cols[0]):
        schema.add(name, wthr_cols[1][i], True)

    # load weather data
    wthrfiles = ['chi-weather_20'+n+'.csv' for n in years]
    wthrbucket = 's3a://chi-cab-bucket/weather/'
    wthrpaths = [wthrbucket + f for f in wthrfiles]
    wthr = spark.read.option('header', True).schema(schema).csv(wthrpaths)
    if verb: print('Weather data table has', wthr.count(), 'rows.')

    wthr = wthr \
        .select('date', 'tdry', 'precip') \
        .filter(wthr.station == '72534014819') \
        .filter(wthr.report == 'FM-15') \
        .fillna({'precip':0})
    wthr = wthr \
        .withColumn('trnd', sf.round(wthr.tdry/10)*10) \
        .withColumn('prnd', sf.ceil(wthr.precip*5)/5) \
        .withColumn('timernd', sf.date_trunc("Hour", wthr.date)) \
        .withColumn('day', (sf.date_format(wthr.date, 'u')).cast('int')) \
        .withColumn('hour', sf.hour(wthr.date)) \
        .drop('tdry', 'precip', 'date')
    if verb: wthr.printSchema()
    if verb: print(wthr.sort('tdry', ascending = False).head(5))
    return wthr


def join_cabs_and_wthr(cabs, wthr, verb = False):
    """ Return joined dataframe with cab and weather data."""
    combo = cabs.join(sf.broadcast(wthr), cabs.startrnd == wthr.timernd) \
        .drop('startrnd', 'timernd')
    if verb: print(combo.sort('trnd', ascending = False).head(5))
    return combo


def aggregate_combo(combo, verb = False):
    """Return aggregated history table."""
    hist = combo \
        .groupBy('trnd', 'prnd', 'day', 'hour') \
        .agg(sf.mean('taxis').alias('taxis'),
            sf.mean('d_hr_cab').alias('d_hr_cab'),
            sf.mean('avg_permile').alias('d_mile'),
            sf.mean('avg_permin').alias('d_min'),
            sf.count(sf.lit(1)).alias('avged_over'))
    if verb: print('Historical data table has', hist.count(), 'rows.')
    return hist


def write_table(hist, verb = False):
    """Write history table ot postgresql database."""

    configfile = '/home/ubuntu/code/.spark-config'
    with open(configfile, 'r') as f:
        config = ast.literal_eval(f.read())
    dburl = config['dburl']
    table = "cabhistory" # could also be "schema.table" if using schema
    user = config['user']
    password = config['password']
    driver = "org.postgresql.Driver"

    hist.write.option('truncate', 'true') \
        .jdbc(dburl, table, mode = 'overwrite', 
            properties={"user":user,
                        "password":password,
                        "driver":driver})
    print('Table written:', showtime())


def data_processing():
    # set configuration and suppress info messages
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.executor.memory', '2048m') \
        .set('spark.executor.cores', 2) \
        .set('spark.sql.files.maxPartitionBytes', 512*1024*1024) \
        .set('spark.sql.shuffle.partitions', 64)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # start a spark session
    spark = SparkSession.builder.appName('cabhistory').getOrCreate()

    verb = False
    years = ['13', '14', '15', '16', '17', '18', '19']
    cabs = make_cab_table(years, verb)
    wthr = make_weather_table(years, verb)
    combo = join_cabs_and_wthr(cabs, wthr, verb)
    hist = aggregate_combo(combo, verb)
    write_table(hist)

if __name__ == '__main__':
    data_processing()