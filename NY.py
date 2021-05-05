# Import & Spark Context configuration
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import socket, requests, json
sc = SparkContext('local[*]', "NY")
spark = SparkSession(sc)


# Building Schema for row data 
schema=StructType([StructField("address",StringType(),True),
StructField("attributes",StructType(),True),
StructField("business_id",StringType(),True),
StructField("city",StringType(),True),
StructField("hours",StructType(),True),
StructField("is_open",DoubleType(),True),
StructField("latitude",DoubleType(),True),
StructField("longitude",DoubleType(),True),
StructField("postal_code",StringType(),True),
StructField("review_count",LongType(),True),
StructField("stars",DoubleType(),True),
StructField("state",StringType(),True)])


# Reading data sources and dataframes building
biz = spark.read.json('file:///home/hadoopsa/data/yelp_biz.json', schema)
tip = spark.read.json('file:///home/hadoopsa/data/yelp_tip.json')
rvw = spark.read.json('file:///home/hadoopsa/data/yelp_review.json')
chk = spark.read.json('file:///home/hadoopsa/data/yelp_checkin.json')
usr = spark.read.json('file:///home/hadoopsa/data/yelp_user.json')

hours = biz.select(col("hours.*"))
attr = biz.select(col("attributes.*"))
biz = biz.drop('attributes').drop('hours')


# Aggregation using stars feature and business id count
rvw = rvw.withColumn("DateTime_r", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
chk = chk.withColumn("DateTime_c", to_timestamp("date", "yyyy-MM-dd HH:mm:ss")).withColumnRenamed('business_id', 'business_id_chk')
rvwchk = rvw.join(chk, rvw.business_id == chk.business_id_chk, how ='left')

agg = rvwchk.groupBy("business_id", window(col("DateTime_r"), "168 hours").alias("Weekly")).agg(sum("stars").alias("stars"), count("business_id_chk").alias("checkins"))


# Save to Hive warehouse
tbl= {biz:'business', tip: 'tip', rvw: 'review',chk: 'checkins',usr: 'user', agg: 'Weekly_agg'}
for k,v in tbl.items():
    k.write.mode('append').option('compression','snappy').saveAsTable("default."+v)
