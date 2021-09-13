from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator

#Initialize Spark Session
#Include Maven Coordinates
spark = SparkSession\
    .builder\
    .appName("Python")\
    .master("local[*]")\
    .config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-2.4_2.11:1.0.1-incubating') \
    .getOrCreate()

#Register SedonaSQL functions in your Spark Session
SedonaRegistrator.registerAll(spark)

#Read data into Spark DataFrame
df = spark.read.option("delimiter", "|").csv("county_small.tsv", header=False)
df.show(5)

#Create a temporary table to use with Spark SQL
df.createOrReplaceTempView("geo_table")

#Convert c0 to Geometric type
geo_query = spark.sql('''SELECT ST_GeomFromWKT(_c0) AS shape
                         FROM geo_table''')

#Ensure co is of Geometric type
geo_query.printSchema()

#View the data
geo_query.show()