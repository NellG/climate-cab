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
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime


def make_cab_table(years, verb = False):
    """ Create dataframe of cab data.

    Round timestamp to nearest hour
    Calculate total cost without tolls
    Calculate $/mile
    Calculate $/min
    """
    cabfiles = ['chi_201'+n+'.csv' for n in years]
    cabbucket = 's3a://chi-cab-bucket/taxi/'
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
    wthrfiles = ['chi-weather_201'+n+'.csv' for n in years]
    wthrbucket = 's3a://chi-cab-bucket/weather/'
    wthrpaths = [wthrbucket + f for f in wthrfiles]
    wthr = spark.read.option('header', True).csv(wthrpaths)
    if verb: print('Weather data table has', wthr.count(), 'rows.')

    # grab desired columns and types from weather
    wthr_cols = [#('STATION', 'station', 'string'), 
                 ('DATE', 'date', 'timestamp'),
                 ('HourlyDryBulbTemperature', 'tdry','float'),
                 ('HourlyPrecipitation', 'precip', 'float')]
    wthr = wthr \
        .select([sf.col(c).alias(n).cast(t) for (c, n, t) in wthr_cols]) \
        .filter(wthr.STATION == '72534014819') \
        .filter(wthr.REPORT_TYPE2 == 'FM-15') \
        .fillna({'precip':0})
    
    # round temperature, precipitation, time
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


def showtime():
    """Return current time as string."""
    now = datetime.now()
    now_str = now.strftime("%H:%M:%S")
    return now_str


# save start time
start = showtime()
# suppress info messages
sc = SparkContext()
sc.setLogLevel("ERROR")
# start a spark session
spark = SparkSession.builder.appName('testpipe').getOrCreate()

print('Script started at:', start)
verb = False
years = '89'
cabs = make_cab_table(years, verb)
print('Cab ingestion done:', showtime())
wthr = make_weather_table(years, verb)
print('Weather ingestion done:', showtime())
combo = join_cabs_and_wthr(cabs, wthr, verb)
print('Cab-weather join done:', showtime())
hist = aggregate_combo(combo, verb)
print('Historical table done:', showtime())
write_table(hist)
print('Script finished:', showtime())