import ast
import csv
import time
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from datetime import datetime
from pyspark.sql.types import StructType

def showtime():
    """Return current time as string."""
    now = datetime.now()
    now_str = now.strftime("%H:%M:%S")
    return now_str

def make_schema(schema_file):
    """Define schema based on .csv input."""

    with open(schema_file, newline='') as f:
        reader = csv.reader(f)
        schema_cols = list(reader)
    schema = StructType()
    for i, name in enumerate(schema_cols[0]):
        schema.add(name, schema_cols[1][i], True) 
    return schema

def write_table(df, table, verb = False):
    """Write history table to postgresql database."""

    configfile = '/home/ubuntu/code/.spark-config'
    with open(configfile, 'r') as f:
        config = ast.literal_eval(f.read())
    dburl = config['dburl']
    user = config['user']
    password = config['password']
    driver = "org.postgresql.Driver"

    df.write.option('truncate', 'true') \
        .jdbc(dburl, table, mode = 'overwrite', 
            properties={"user":user,
                        "password":password,
                        "driver":driver})

def read_cabs(spark, years):
    """Read cab data into dataFrame."""

    cabfiles = ['chi_20'+n+'.csv' for n in years]
    cabbucket = 's3a://chi-cab-bucket/taxi/'
    cabpaths = [cabbucket + f for f in cabfiles]

    cabs = spark.read \
        .option('header', True) \
        .schema(make_schema('cab_schema.csv')) \
        .csv(cabpaths)

    return cabs


def read_wthr(spark, years):
    """Read weather data into dataFrame."""

    wthrfiles = ['chi-weather_20'+n+'.csv' for n in years]
    wthrbucket = 's3a://chi-cab-bucket/weather/'
    wthrpaths = [wthrbucket + f for f in wthrfiles]
    wthr = spark.read \
        .option('header', True) \
        .schema(make_schema('weather_schema.csv')) \
        .csv(wthrpaths)
    
    return wthr

def cab_sums(cabs, show_rows=True, show_agg=True):
    """Total rides, fares and unique taxis."""

    # count rows
    if show_rows: print('Cab data table has', cabs.count(), 'rows.')
    
    # aggregate sums
    cab_agg = cabs.agg(
        sf.countDistinct('taxi').alias('count'),
        sf.sum('Trip Total').alias('total_fare'))
    if show_agg: cab_agg.show()


def cab_regions(cabs, show_df=False, write_out=True):
    """Count each community and tract pair."""

    # aggregate counts
    cab_agg = cabs \
        .groupBy('tract_pick', 'comm_pick') \
        .agg(sf.count(sf.lit(1)).alias('rides'))
    
    if show_df: cab_agg.show()
    if write_out: write_table(cab_agg, 'pickup_pairs')


def change_community(cabs):
    """Find fraction of cab trips that start and end in different community areas."""
    
    cabs = cabs.withColumn('stay', cabs.comm_pick == cabs.comm_drop)
    cabs.groupBy('stay').agg(sf.count(sf.lit(1)).alias('count')).show()


if __name__ == '__main__':
    # set configuratin and suppress info messages
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.sql.shuffle.partitions', 64) \
        .set('spark.executor.memory', '2400m') \
        .set('spark.executor.cores', 2)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # start a spark session
    spark = SparkSession.builder.appName('cabhistory').getOrCreate()

    # show start time
    print('Started at:', showtime())

    #years = ['13', '14', '15', '16', '17', '18', '19']
    years = ['19']
    cabs = read_cabs(years)
    change_community(cabs)

    # show end time and pause
    print('Finished at:', showtime())
    time.sleep(30)