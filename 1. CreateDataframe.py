# Databricks notebook source
# MAGIC %md
# MAGIC ##1. Creating Data Frames

# COMMAND ----------

# MAGIC %md
# MAGIC ##create dataframe with Range

# COMMAND ----------

df = spark.range(1,10) 

# COMMAND ----------

df.display()

# COMMAND ----------

df =spark.range(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC ##CreateDatafrme():

# COMMAND ----------

lst1=[
    [10,"REDDY",20000,'BANGALORE'],
    [20,"GANI",30000,'CHENNAI'],
    [30,"SIVA",20000,'HYDERABAD']
  ]

scema1=['deptno','ename','salary','city']
  
df=spark.createDataFrame(lst1,scema1)
display(df)

# COMMAND ----------

lst1=[
    [10,"REDDY",20000,'BANGALORE'],
    [20,"GANI",30000,'CHENNAI'],
    [30,"SIVA",20000,'HYDERABAD']
  ]

scema1=['deptno','ename','salary','city']
  
df=spark.createDataFrame(lst1)
display(df)

# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;

# COMMAND ----------

#This creates a DataFrame from a collection(list, dict), RDD or Python Pandas.
lst = (('Robert',35),('James',25)) 
#spark.createDataFrame(data=lst)##With Out Schema 
df = spark.createDataFrame(data=lst,schema=('Name','Age'))  ##With Schema


# COMMAND ----------

df.display()

# COMMAND ----------

#using dictinary
dict = ({"name":"robert","age":25}, {"name" : "james","age" : 31}) 
df = spark.createDataFrame(dict)
display(df)
#df.show()


# COMMAND ----------

#using rdd
lst3 = (('Robert',35),('James',25)) 
rdd = sc.parallelize(lst3)
schema10=['name','age']
#df =  spark.createDataFrame(data=rdd,schema=('name string, age long'))
df =  spark.createDataFrame(rdd,schema10)
df.show()
#DF=RDD+schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to create spark dataframe from pandas dataframe

# COMMAND ----------

#Using Python Pandas DamaFrame*
#Pandas dataframe is a two dimensional structure with named rows and columns. So data is aligned in a tabular fashion in rows and columns

import pandas as pd

data = (('tom', 10), ('nick', 15), ('juli', 14)) 
df_pandas = pd.DataFrame(data,columns=('Name','Age')) 
#df = spark.createDataFrame(data=df_pandas)
display(df_pandas)

# COMMAND ----------

df = spark.createDataFrame(df_pandas)

# COMMAND ----------

# DBTITLE 1,Create dataframe from list of lists
#create data frame from list of lists
users_list = [[1, 'Scott'], [2, 'Donald'], [3, 'Mickey'], [4, 'Elvis']]
type(users_list)
type(users_list[1])


# COMMAND ----------

spark.createDataFrame(users_list, 'user_id int, user_first_name string')


# COMMAND ----------

from pyspark.sql import Row
users_rows = [Row(*user) for user in users_list]
spark.createDataFrame(users_rows, 'user_id int, user_first_name string')


# COMMAND ----------

# DBTITLE 1,Create dataframe from list of tuples
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
spark.createDataFrame(users_list, 'user_id int, user_first_name string')

# COMMAND ----------

# DBTITLE 1,Create Dataframe from List of Dict
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]

spark.createDataFrame(users_list)

# COMMAND ----------

#using rows
users_rows = [Row(*user.values()) for user in users_list]
spark.createDataFrame(users_rows, 'user_id bigint, user_first_name string')

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Data Types
import datetime
users = [
    {
        "id": 1,
        "first_name": "Venkat",
        "last_name": "rama",
        "email": "venkat@gmail.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Hanu",
        "last_name": "Reddy",
        "email": "abc@gmail.com",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Ram",
        "last_name": "Charan",
        "email": "power@gmail.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "scott",
        "last_name": "tiger",
        "email": "tiger@gmail.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    }
]

# COMMAND ----------

from pyspark.sql import Row
users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##create dataframe using schema

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Venkat",
        "last_name": "rama",
        "email": "venkat@gmail.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Hanu",
        "last_name": "Reddy",
        "email": "abc@gmail.com",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Ram",
        "last_name": "Charan",
        "email": "power@gmail.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "scott",
        "last_name": "tiger",
        "email": "tiger@gmail.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    }
]

# COMMAND ----------

data_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts TIMESTAMP
'''

# COMMAND ----------

spark.createDataFrame(users, data_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##create dataframe with spark schema

# COMMAND ----------

data_schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])

spark.createDataFrame(users, schema=users_schema)
spark.createDataFrame(users, schema=users_schema).rdd.collect()


# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Ram",
        "last_name": "Laxman",
        "email": "sder@gmail.com",
        "phone_numbers": ["9345678901", "2345678911"],
        "is_customer": True,
        "amount_paid": 1000.00,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Basha",
        "last_name": "Shaik",
        "email": "sdert@gmail.com",
        "phone_numbers": ["13456708923", "2345678934"],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "ABC",
        "last_name": "XYZ",
        "email": "abc@gmail.com",
        "phone_numbers": ["7145129752", "7145126601"],
        "is_customer": True,
        "amount_paid": 850.50,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@gmail.com",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": ["8179347142"],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()
#users_df.select('id', 'phone_numbers').show(truncate=False)
#users_df.dtypes
#users_df.columns



# COMMAND ----------

from pyspark.sql.functions import *
#df2=users_df.select(col("id"),col("first_name"))
df2=users_df.select("id","first_name")
df3=df2.withColumn("city",lit("Bangalore"))


# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import col
ggr=users_df. \
    withColumn('phone_number', explode('phone_numbers')).drop("phone_numbers")

df_rename=ggr.withColumnRenamed("phone_number","phone")

# COMMAND ----------

users_df. \
    select('id', col('phone_numbers')[0].alias('mobile'), col('phone_numbers')[1].alias('home')). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Map Type Columns in Spark Dataframes

# COMMAND ----------

import datetime
users_data = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": {"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"},
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": {"mobile": "+1 234 567 8923", "home": "+1 234 567 8934"},
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": {"mobile": "+1 714 512 9752", "home": "+1 714 512 6601"},
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": {"mobile": "+1 817 934 7142"},
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

from pyspark.sql import Row
users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()

# COMMAND ----------

users_df.select('id', 'phone_numbers').show(truncate=False)


# COMMAND ----------

users_df.dtypes
users_df.columns

# COMMAND ----------

from pyspark.sql.functions import col
users_df.select('id', col('phone_numbers')['mobile']).show()
users_df.select('id', col('phone_numbers')['mobile'].alias('mobile')).show()

# COMMAND ----------

users_df. \
    select('id', col('phone_numbers')['mobile'].alias('mobile'), col('phone_numbers')['home'].alias('home')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import explode
users_df.select('id', explode('phone_numbers')).show()

users_df.select('*', explode('phone_numbers')). \
    withColumnRenamed('key', 'phone_type'). \
    withColumnRenamed('value', 'phone_number'). \
    drop('phone_numbers'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##struct type in dataframe

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.select('id', 'phone_numbers').show(truncate=False)
users_df. \
    select('id', 'phone_numbers.mobile', 'phone_numbers.home'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'phone_numbers.mobile', 'phone_numbers.home'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC #SELECT

# COMMAND ----------

users_df.select('*').show()
users_df.select('id', 'first_name', 'last_name').show()
users_df.select(['id', 'first_name', 'last_name']).show()
users_df.alias('u').select('u.*').show()


# COMMAND ----------

users_df.select('id', 'first_name', 'last_name').show()


# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
users_df.select(
    col('id'), 
    'first_name', 
    'last_name',
    concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #SelectExpr

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

users_df.alias('u').selectExpr('u.*').show()
users_df.selectExpr('id', 'first_name', 'last_name').show()
users_df.selectExpr('id', 'first_name', 'last_name', "concat(first_name, ', ', last_name) AS full_name").show()


# COMMAND ----------

users_df.createOrReplaceTempView('users')
spark.sql("""
    SELECT id, first_name, last_name,
        concat(first_name, ', ', last_name) AS full_name
    FROM users
"""). \
    show()

# COMMAND ----------

users_df.select(
        col('id'), 
        date_format('customer_from', 'yyyyMMdd')
    ).show()

# COMMAND ----------

users_df.select(
    col('id'), 
    date_format('customer_from', 'yyyyMMdd')
).printSchema()

# COMMAND ----------

users_df.select(
    col('id'), 
    date_format('customer_from', 'yyyyMMdd').alias('customer_from')
).show()

# COMMAND ----------

users_df.select(
    col('id'), 
    date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
).show()

# COMMAND ----------

users_df.select(
    col('id'), 
    date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
).printSchema()



cols = [col('id'), date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')]
users_df.select(*cols).show()

# COMMAND ----------

from pyspark.sql.functions import date_format
date_format('customer_from', 'yyyyMMdd').cast('int')
date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
customer_from_alias = date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')


# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming

# COMMAND ----------

users_df. \
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ). \
    show()

# COMMAND ----------

users_df. \
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ). \
    show()

# COMMAND ----------

# Using withColumn and alias (first select and then withColumn)
users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')
    ). \
    withColumn('user_full_name', concat(col('user_first_name'), lit(', '), col('user_last_name'))). \
    show()

# COMMAND ----------

users_df. \
    withColumn('user_full_name', concat(users_df['first_name'], lit(', '), users_df['last_name'])). \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ). \
    show()

# COMMAND ----------

from pyspark.sql import Row


# COMMAND ----------

help(Row)

# COMMAND ----------

Row("Azure",10)

# COMMAND ----------

r2=Row(name="azure",age=57)

# COMMAND ----------

r2.name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT concat('Spark', 'SQL');
