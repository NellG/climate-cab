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

# Read speed test for opening files in S# with Spark
# load cab table schema

# save start time
start = showtime()
start_time = datetime.now()
# set configuratin and suppress info messages
conf = SparkConf() \
    .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .set('spark.sql.shuffle.partitions', 300) \
    .set('spark.executor.memory', '2400m') \
    .set('spark.executor.cores', 2)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# start a spark session
spark = SparkSession.builder.appName('cabhistory').getOrCreate()

print('Script started at:', start)
with open('cab_schema.csv', newline='') as f:
    reader = csv.reader(f)
    cab_cols = list(reader)
schema = StructType()
for i, name in enumerate(cab_cols[0]):
    schema.add(name, cab_cols[1][i], True) 

years = '3456789'

# load cab data
cabfiles = ['chi_201'+n+'.csv' for n in years]
cabbucket = 's3a://chi-cab-bucket/taxi/'
cabpaths = [cabbucket + f for f in cabfiles]
cabs = spark.read.option('header', True).schema(schema).csv(cabpaths)
print('Cab data table has', cabs.count(), 'rows.')
cab_agg = cabs.agg(sf.countDistinct('taxi').alias('count').collect()
print(cab_agg)

print('Finished at:', showtime())
time.sleep(60)