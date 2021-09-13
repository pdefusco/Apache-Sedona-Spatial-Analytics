from pyspark.sql import SparkSession
from pyspark.sql.functions import substring_index
from sedona.register import SedonaRegistrator

spark = SparkSession\
    .builder\
    .appName("Python")\
    .master("local[*]")\
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-2.4_2.11:1.0.1-incubating,'
           'org.datasyslab:geotools-wrapper:geotools-24.1') \
    .getOrCreate()

SedonaRegistrator.registerAll(spark)

df = spark.read.option("delimiter", "|").csv("s3a://akramer-dl/akramer-data/county_small.tsv", header=False)
df.show(5)

df.createOrReplaceTempView("geo_table")

geo_query = spark.sql('''SELECT ST_GeomFromWKT(_c0) AS shape
                         FROM geo_table''')

geo_query.show()

geo_query.printSchema()