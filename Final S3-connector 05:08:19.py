# Databricks notebook source
import urllib


delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"


aws_keys_df = spark.read.format("delta").load(delta_table_path)

ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

aws_bucket_name = "user-121704c19363-bucket"
df_pin = spark.read.format('json').load(f"s3a://{aws_bucket_name}/topics/121704c19363.pin/partition=0/")


# COMMAND ----------

aws_bucket_name = "user-121704c19363-bucket"
df_geo = spark.read.format('json').load(f"s3a://{aws_bucket_name}/topics/121704c19363.geo/partition=0/")

# COMMAND ----------

aws_bucket_name = "user-121704c19363-bucket"
df_user = spark.read.format('json').load(f"s3a://{aws_bucket_name}/topics/121704c19363.user/partition=0/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

def convert_follower_count(follower_count):
    if follower_count is None:
        return 0
    follower_count.strip()
    if follower_count.endswith('K') or follower_count.endswith('k'):
        return int(float(follower_count[:-1]) * 1000)
    elif follower_count.endswith('M') or follower_count.endswith('m'):
        return int(float(follower_count[:-1]) * 1000000)
    else:
        return int(follower_count)

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

df_pin_cleaned = clean_pin_df(df_pin)
df_pin_cleaned.createOrReplaceTempView('pin')


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

df_geo_cleaned = clean_geo_df(df_geo)
df_user_cleaned = clean_user_df(df_user)
df_geo_cleaned.createOrReplaceTempView('geo')
df_user_cleaned.createOrReplaceTempView('user')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     pin.category,
# MAGIC     geo.country,
# MAGIC     COUNT(pin.ind) AS category_count
# MAGIC     FROM pin
# MAGIC       JOIN geo ON pin.ind = geo.ind
# MAGIC     GROUP BY geo.country, pin.category
# MAGIC     ORDER BY geo.country, category_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     pin.category,
# MAGIC     YEAR(geo.timestamp) AS post_year,
# MAGIC     COUNT(pin.ind) AS category_count,
# MAGIC     RANK() OVER (PARTITION BY pin.category ORDER BY COUNT(pin.ind) DESC) AS category_rank
# MAGIC     FROM pin
# MAGIC     JOIN geo ON pin.ind = geo.ind
# MAGIC     GROUP BY category, post_year

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH follower_ranks AS 
# MAGIC (SELECT 
# MAGIC     user.user_name AS name,
# MAGIC     geo.country AS country,
# MAGIC     pin.follower_count AS follower_count,
# MAGIC     RANK() OVER (ORDER BY follower_count DESC) AS follower_rank
# MAGIC     FROM user JOIN
# MAGIC     pin ON user.ind = pin.ind JOIN
# MAGIC     geo ON pin.ind = geo.ind)
# MAGIC
# MAGIC SELECT  
# MAGIC   name,
# MAGIC   country,
# MAGIC   AVG(follower_count)
# MAGIC   FROM follower_ranks
# MAGIC WHERE follower_rank = 1
# MAGIC GROUP BY name, country

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN user.age < 18 THEN 'under 18'
# MAGIC          WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC          WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
# MAGIC          WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
# MAGIC          ELSE '50+' END AS age_group,
# MAGIC     pin.category,
# MAGIC     COUNT(pin.category) AS category_count
# MAGIC     FROM pin JOIN user ON user.ind = pin.ind
# MAGIC     GROUP BY age_group, pin.category
# MAGIC     ORDER BY pin.category, age_group

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN user.age < 18 THEN 'under 18'
# MAGIC          WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC          WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
# MAGIC          WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
# MAGIC          ELSE '50+' END AS age_group,
# MAGIC     MEDIAN(pin.follower_count)
# MAGIC     FROM pin JOIN user ON user.ind = pin.ind
# MAGIC     GROUP BY age_group

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW distinct_users AS
# MAGIC SELECT 
# MAGIC   YEAR(date_joined) AS join_year,
# MAGIC   user_name, 
# MAGIC   date_joined,
# MAGIC   age,
# MAGIC   CASE WHEN user.age < 18 THEN 'under 18'
# MAGIC          WHEN user.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC          WHEN user.age BETWEEN 25 AND 34 THEN '25-34'
# MAGIC          WHEN user.age BETWEEN 35 AND 49 THEN '35-49'
# MAGIC          ELSE '50+' END AS age_group,
# MAGIC   AVG(pin.follower_count) AS follower_count
# MAGIC   FROM user JOIN
# MAGIC   pin ON pin.ind = user.ind
# MAGIC   GROUP BY user_name, date_joined, age 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(user_name) AS number_users_joined,
# MAGIC   join_year
# MAGIC   FROM distinct_users
# MAGIC   GROUP BY join_year
# MAGIC   ORDER BY join_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     join_year,
# MAGIC     MEDIAN(follower_count)
# MAGIC     FROM distinct_users
# MAGIC     GROUP BY join_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     join_year,
# MAGIC     age_group,
# MAGIC     MEDIAN(follower_count)
# MAGIC     FROM distinct_users
# MAGIC     GROUP BY join_year, age_group
# MAGIC     ORDER BY age_group, join_year

# COMMAND ----------


