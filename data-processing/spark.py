# launch pyspark interpreter with:
# bin/pyspark --master spark://privateip:7077 --packages/
# org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7

# open csv test files, check length and schema
small = "s3a://chi-cab-bucket/test/taxi-small-data.csv"
large = "s3a://chi-cab-bucket/test/taxi-test-data.csv"

df = spark.read.option('header', True).csv(small)
df.printSchema()
df.count() # should give 28225

df2 = spark.read.option('header', True).csv(large)
df2.printSchema()
df2.count() # should give 531521

df3 = spark.read.option('header', True).csv([small, large])
df3.printSchema()
df3.count() # should give 559746

# write csv to postgresql database
dburl = "jdbc:postgresql://10.0.0.4:18188/testdb"
table = "testtbl" # could also be "schema.table" if using schema
user = "db_select"
password = <secret>
driver = "org.postgresql.Driver"

df.write.jdbc(dburl, table,
              properties={"user":user,
                          "password":password,
                          "driver":driver})