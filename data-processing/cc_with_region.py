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
from spark_functions import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime
from pyspark.sql.types import StructType


def persist_cabs(cabs, verb=False):
    
    cabs = cabs \
        .filter(cabs.trip_tot < 500) \
        .select(['taxi', 'start_str', 'comm_pick', 'dur', 
                 'dist', 'fare', 'tip', 'extra']) \
        .fillna(0, subset=['fare', 'tip', 'extra', 'dur', 'dist']) 
        #.fillna(1, subset=['dur', 'dist'])
    cabs = cabs \
        .withColumn('startrnd', sf.date_trunc("Hour", 
            sf.to_timestamp(cabs.start_str, 'MM/dd/yyyy hh:mm:ss aa'))) \
        .withColumn('total', cabs.fare + cabs.tip + cabs.extra) \
        .drop('start_str', 'fare', 'tip', 'extra')
    cabs = cabs \
        .withColumn('permile', 
            sf.when(cabs.dist > 0.2, sf.least(cabs.total / cabs.dist, sf.lit(50))) \
                .otherwise(sf.lit(2.5))) \
        .withColumn('permin', 
            sf.when(cabs.dur.isNull(), cabs.dur) \
                .otherwise(sf.least(cabs.total / (cabs.dur/60), sf.lit(200))))
        #.drop('dur', 'dist')


    cabs = cabs.repartition(64, 'startrnd').cache()
    if verb: cabs.sort('permile', ascending=False).show()
    return cabs

    cab_agg(cabs, ['startrnd', 'comm_pick'], True)
    cab_agg(cabs, ['startrnd'], True)
    

def cab_agg(cabs, cols, verb=False):
    cab_agg = cabs \
        .groupBy(cols) \
        .agg(sf.countDistinct('taxi').alias('taxis'),
             sf.sum('total').alias('sum_fares'),
             sf.mean('permile').alias('avg_permile'),
             sf.mean('permin').alias('avg_permin'),
             sf.mean('dur').alias('avg_dur'),
             sf.mean('dist').alias('avg_dist'),
             sf.max('permin').alias('max_permin'),
             sf.count(sf.lit(1)).alias('rides'))
    cab_agg = cab_agg \
        .withColumn('d_hr_cab', cab_agg.sum_fares/cab_agg.taxis)

    if verb: cab_agg.show()
    if verb: print('City aggregation:', showtime())


def make_weather_table(wthr, verb = False):
    """ Create dataframe of weather data.

    Round hourly temp to nearest 10 def F
    Divide rainfall into none, light (<=0.2 in), and heavy bins
    Round timestamp to nearest hour
    """

    if verb: print('Weather data table has', wthr.count(), 'rows.')

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


if __name__ == '__main__':
    # save start time
    start = showtime()
    start_time = datetime.now()
    # set configuration and suppress info messages
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.executor.memory', '2g') \
        .set('spark.executor.cores', 2) \
        .set('spark.sql.files.maxPartitionBytes', 512*1024*1024) \
        .set('spark.sql.shuffle.partitions', 64)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # start a spark session
    spark = SparkSession \
        .builder \
        .appName('cabhistory') \
        .enableHiveSupport() \
        .getOrCreate()

    print('Script started at:', start)
    verb = True
    #years = ['13', '14', '15', '16', '17', '18', '19']
    years = ['13']
    cabs = persist_cabs(read_cabs(spark, years), verb)
    #cabs = make_cab_table(read_cabs(spark, years), verb)
    #wthr = make_weather_table(read_wthr(years), verb)
    #combo = join_cabs_and_wthr(cabs, wthr, verb)
    #hist = aggregate_combo(combo, verb)
    #write_table(hist, 'cabhistory')
    #hist.explain(True)
    finish_time = datetime.now()
    print('Script finished:', showtime())
    delta_str = str(finish_time - start_time)
    print('Total run time:', delta_str)
    #inp = input('Press enter to continue.')