# Databricks notebook source
import urllib


delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"


aws_keys_df = spark.read.format("delta").load(delta_table_path)

ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

kinesis_df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_pin = kinesis_df.filter(kinesis_df.partitionKey == '121704c19363.pin')
df_geo = kinesis_df.filter(kinesis_df.partitionKey == '121704c19363.geo')
df_user = kinesis_df.filter(kinesis_df.partitionKey == '121704c19363.user')

# COMMAND ----------

df_pin = df_pin.selectExpr("CAST(data AS STRING) jsonData")
df_geo = df_geo.selectExpr("CAST(data AS STRING) jsonData")
df_user = df_user.selectExpr("CAST(data AS STRING) jsonData")

# COMMAND ----------

df_pin = df_pin.filter(df_pin.jsonData != '""')
df_geo = df_geo.filter(df_geo.jsonData != '""')
df_user = df_user.filter(df_user.jsonData != '""')

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField
pin_struct = StructType([
    StructField('index', IntegerType(), True),
    StructField('unique_id', StringType(), True),
    StructField('title', StringType(), True),
    StructField('description', StringType(), True),
    StructField('poster_name', StringType(), True),
    StructField('follower_count', StringType(), True),
    StructField('tag_list', StringType(), True),
    StructField('is_image_or_video', StringType(), True),
    StructField('image_src', StringType(), True),
    StructField('downloaded', IntegerType(), True),
    StructField('save_location', StringType(), True),
    StructField('category', StringType(), True)
])

geo_struct = StructType([
    StructField('ind', IntegerType(), True),
    StructField('timestamp', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('country', StringType(), True)
])

user_struct = StructType([
    StructField('ind', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('date_joined', StringType(), True)
])


# COMMAND ----------

from pyspark.sql.functions import from_json

df_pin = df_pin.select(from_json('jsonData', pin_struct).alias('data')).select('data.*')
df_geo = df_geo.select(from_json('jsonData', geo_struct).alias('data')).select('data.*')
df_user = df_user.select(from_json('jsonData', user_struct).alias('data')).select('data.*')

# COMMAND ----------

def convert_follower_count(follower_count):
    if follower_count is None:
        return 0
    follower_count.strip()
    if follower_count.endswith('K') or follower_count.endswith('k'):
        if follower_count[:-1].isdigit():
            return int(float(follower_count[:-1]) * 1000)
        else:
            return 0
    elif follower_count.endswith('M') or follower_count.endswith('m'):
        if follower_count[:-1].isdigit():
            return int(float(follower_count[:-1]) * 1000000)
        else:
            return 0
    else:
        if follower_count.isdigit():
            return int(follower_count)
        else:
            return 0

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

convert_follower_count_udf = udf(convert_follower_count, IntegerType())

def clean_pin_df(df):
    df = df.dropDuplicates()
    df = df.replace({
        'Image src error': None,
        "No description available Story format": None,
        "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e": None,
        "No Title Data Available": None,
        'User Info Error': None
    })
    df = df.withColumn('follower_count', convert_follower_count_udf(col('follower_count')))
    df = df.withColumn('follower_count', df['follower_count'].cast('int'))
    save_strip = lambda save_loc: save_loc[14:]
    df = df.withColumn('save_location', udf(save_strip)(df['save_location']))
    df = df.withColumn('is_image_or_video', df['is_image_or_video'].cast('boolean'))
    df = df.withColumnRenamed('index', 'ind')
    df = df.select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')

    return df

# COMMAND ----------

from pyspark.sql.functions import lit, array, to_timestamp

def clean_geo_df(df):
    df = df.dropDuplicates()
    df = df.withColumn('coordinates', array('latitude','longitude'))
    df = df.withColumn('timestamp', to_timestamp('timestamp'))
    df = df.select('ind','country','coordinates','timestamp')
    return df

# COMMAND ----------

from pyspark.sql.functions import concat
def clean_user_df(df):
    df = df.dropDuplicates()
    df = df.withColumn('date_joined', to_timestamp('date_joined'))
    df = df.withColumn('user_name', concat(df.first_name,lit(' '),df.last_name))
    df = df.select('ind','user_name','age','date_joined')
    return df

# COMMAND ----------

cleaned_pin = clean_pin_df(df_pin)
cleaned_geo = clean_geo_df(df_geo)
cleaned_user = clean_user_df(df_user)

# COMMAND ----------

df_pin.createOrReplaceTempView('pin')

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
cleaned_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("121704c19363_pin_table")

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
cleaned_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("121704c19363_geo_table")

# COMMAND ----------

dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
cleaned_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("121704c19363_user_table")

# COMMAND ----------


