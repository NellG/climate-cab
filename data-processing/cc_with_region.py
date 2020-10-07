# launch pyspark interpreter with:
# usr/local/spark/bin/pyspark --master spark://privateip:7077 --packages/
# org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7

# launch script with:
# spark-submit 
# --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7 
# --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true
# --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true
# --master spark://10.0.0.14:7077 spark.py
import csv
import time
from spark_functions import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


def persist_cabs(cabs, verb=False):
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
    #cabs = cabs.persist(StorageLevel.MEMORY_AND_DISK_SER)
    if verb: cabs.show(50)
    return cabs
    

def aggregate_cabs(cabs, cols, verb=False):
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

    if verb: cab_agg.show(50)
    if verb: print('City aggregation:', showtime())
    return cab_agg


def persist_weather(wthr, verb = False):
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

    if verb: print(wthr.sort('tdry', ascending = False).show(5))
    return wthr


def agg_cabs_and_wthr(cabs, wthr, verb = False):
    """ Return joined, aggregated dataframe with cab and weather data."""
    
    combo = cabs \
        .join(sf.broadcast(wthr), cabs.startrnd == wthr.timernd) \
        .drop('startrnd', 'timernd')
    if verb: print(combo.sort('trnd', ascending = False).show(5))

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
        .set('spark.sql.files.maxPartitionBytes', 128*1024*1024) \
        .set('spark.sql.shuffle.partitions', 200)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # start a spark session
    spark = SparkSession \
        .builder \
        .appName('cabhistory') \
        .getOrCreate()

    print('Script started at:', start)
    verb = False
    years = ['13', '14', '15', '16', '17', '18', '19']
    #years = ['13']
    cabs = persist_cabs(read_cabs(spark, years), verb)
    #cabs = cabs.persist()
    #cabs.explain()
    wthr = persist_weather(read_wthr(spark, years), verb)
    #wthr.explain()

    # save summary table for community areas
    cabs_area = aggregate_cabs(cabs, ['startrnd', 'comm_pick'], verb)
    hist_area = agg_cabs_and_wthr(cabs_area, wthr, verb)
    #hist_area.printSchema()
    #print(hist_area.collect()[:5])
    write_table(hist_area, 'areahistory')
    print('Area table written:', showtime())
    #hist_area.explain()

    # save summary table for whole city
    cabs_city = aggregate_cabs(cabs, ['startrnd'], verb)
    hist_city = agg_cabs_and_wthr(cabs_city, wthr, verb)
    #hist_city.printSchema()
    #print(hist_city.collect()[:5])
    write_table(hist_city, 'cityhistory')
    print('City table written:', showtime())
    #hist_city.explain()

    finish_time = datetime.now()
    print('Script finished:', showtime())
    delta_str = str(finish_time - start_time)
    print('Total run time:', delta_str)
    print('Completed successfully.')
    time.sleep(30)