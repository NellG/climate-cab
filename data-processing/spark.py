# launch pyspark interpreter with:
# usr/local/spark/bin/pyspark --master spark://privateip:7077 --packages/
# org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7
import ast
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf


def make_cab_table(verb = False):
    """ Create dataframe of cab data.

    Round timestamp to nearest hour
    Calculate total cost without tolls
    Calculate $/mile
    Calculate $/min
    """
    cabfiles = ['taxi-test-data.csv']
    cabbucket = 's3a://chi-cab-bucket/test/'
    cabpaths = [cabbucket + f for f in cabfiles]
    cabs = spark.read.option('header', True).csv(cabpaths)
    if verb: print('Cab data table has', cabs.count(), 'rows.')

    # grab desired columns
    cab_cols = [#('Trip ID', 'trip', 'string'),
                ('Taxi ID', 'taxi', 'string'),
                ('Trip Start Timestamp', 'start_str', 'string'),
                ('Trip Seconds', 'dur', 'float'),
                ('Trip Miles', 'dist', 'float'),
                ('Fare', 'fare', 'float'),
                ('Tips', 'tip', 'float'),
                ('Extras', 'extra', 'float')]
    cabs = cabs \
        .select([sf.col(c).alias(n).cast(t) for (c, n, t) in cab_cols]) \
        .fillna(0, subset=['fare', 'tip', 'extra']) \
        .fillna(1, subset=['dur', 'dist'])
    cabs = cabs \
        .withColumn('startrnd', sf.date_trunc("Hour", 
            sf.to_timestamp(cabs.start_str, 'mm/dd/yyyy hh:mm:ss aa'))) \
        .withColumn('total', cabs.fare + cabs.tip + cabs.extra) \
        .drop('start_str', 'fare', 'tip', 'extra')
    cabs = cabs \
        .withColumn('permile', cabs.total / cabs.dist) \
        .withColumn('permin', cabs.total / cabs.dur * 60) \
        .drop('dist', 'dur')
    if verb: print(cabs.head(5))

    cab_agg = cabs \
        .groupBy('startrnd') \
        .agg(sf.countDistinct('taxi').alias('taxis'),
             sf.sum('total').alias('sum_fares'),
             sf.mean('permile').alias('avg_permile'),
             sf.mean('permin').alias('avg_permin'),
             sf.count(sf.lit(1)).alias('rides'))
    cab_agg = cab_agg \
        .withColumn('$/hr/cab', cab_agg.sum_fares/cab_agg.taxis)
    if verb: cab_agg.sort('taxis', ascending = False).show()

    return cab_agg


def make_weather_table(verb = False):
    """ Create dataframe of weather data.

    Round hourly temp to nearest 5 def F
    Round hourly precipitation to nearest 0.1 in
    Round timestamp to nearest hour
    """
    wthrfiles = ['chi-weather_2019.csv']
    wthrbucket = 's3a://chi-cab-bucket/weather/'
    wthrpaths = [wthrbucket + f for f in wthrfiles]
    wthr = spark.read.option('header', True).csv(wthrpaths)
    if verb: print('Weather data table has', wthr.count(), 'rows.')

    # grab desired columns and types from weather
    wthr_cols = [#('STATION', 'station', 'string'), 
                 ('DATE', 'date', 'timestamp'),
                 ('HourlyDryBulbTemperature', 'Tdry','float'),
                 ('HourlyPrecipitation', 'precip', 'float')]
    wthr = wthr \
        .select([sf.col(c).alias(n).cast(t) for (c, n, t) in wthr_cols]) \
        .filter(wthr.STATION == '72534014819') \
        .filter(wthr.REPORT_TYPE2 == 'FM-15') \
        .fillna({'precip':0})
    
    # round temperature, precipitation, time
    wthr = wthr \
        .withColumn('Trnd', sf.round(wthr.Tdry/5)*5) \
        .withColumn('prnd', sf.round(wthr.precip, 1)) \
        .withColumn('timernd', sf.date_trunc("Hour", wthr.date)) \
        .withColumn('day', sf.date_format(wthr.date, 'u')) \
        .withColumn('hour', sf.hour(wthr.date)) \
        .drop('Tdry', 'precip', 'date')
    if verb: print(wthr.head(5))
    return wthr


# suppress info messages
sc = SparkContext()
sc.setLogLevel("ERROR")
# start a spark session
spark = SparkSession.builder.appName('testpipe').getOrCreate()

verb = False
cabs = make_cab_table(verb)
wthr = make_weather_table(verb)
combo = cabs.join(sf.broadcast(wthr), cabs.startrnd == wthr.timernd) \
    .drop('startrnd', 'timernd')
if verb: print(combo.head(5))

hist = combo \
    .groupBy('Trnd', 'prnd', 'day', 'hour') \
    .agg(sf.mean('taxis').alias('taxis'),
         sf.mean('$/hr/cab').alias('d_hr_cab'),
         sf.mean('avg_permile').alias('d_mile'),
         sf.mean('avg_permin').alias('d_min'),
         sf.count(sf.lit(1)).alias('avged_over'))

print('Historical data table has', hist.count(), 'rows.')
#hist.sort('d_hr_cab', ascending = False).show()
#hist.sort('avged_over', ascending = False).show()
#hist.sort('prnd', ascending = False).show()

# write csv to postgresql database
configfile = '/home/ubuntu/code/.spark-config'
with open(configfile, 'r') as f:
    config = ast.literal_eval(f.read())
dburl = config['dburl']
table = "cabhistory" # could also be "schema.table" if using schema
user = config['user']
password = config['password']
driver = "org.postgresql.Driver"

hist.write.jdbc(dburl, table, mode = 'overwrite',
              properties={"user":user,
                          "password":password,
                          "driver":driver})

print('Script finished')