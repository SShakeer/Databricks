# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

dept_df.write.json(f'/FileStore/tables/courses100', mode='overwrite')
#overwrite
#append
#dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

dbutils.fs.rm(f'/FileStore/tables/courses100')

# COMMAND ----------

emp_df.write.format('json').save(f'/user/{username}/courses', mode='overwrite')

# COMMAND ----------

# Default behavior
# It will delimit the data using comma as separator
dept_df.write.csv(f'/user/{username}/courses')

# COMMAND ----------

dept_df.write.format('csv').save(f'/FileStore/tables/dept1000.csv')

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/merge_target2/")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/dept1000.csv")

# COMMAND ----------

username="azure"

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    csv(f'/user/{username}/courses', mode='overwrite', header=True)

# COMMAND ----------

spark.read.text(f'/user/{username}/courses').show(truncate=False)

# COMMAND ----------

dept_df.write.format("csv").partitionBy("deptno").mode("overwrite").option("mergeSchema","true").saveAsTable("default.dept2023")

# COMMAND ----------

path='/FileStore/tables/emp12.csv'

# COMMAND ----------

spark.read.csv(f'{path}', header=True).show()

# COMMAND ----------

import getpass
username = getpass.getuser()
print(username)

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    csv(
        f'/user/{username}/courses', 
        mode='overwrite', 
        compression='gzip',
        header=True
    )

# COMMAND ----------

dbutils.fs.ls('FileStore/tables/c*')

# COMMAND ----------

dbutils.fs.rm('FileStore/tables/dataset')

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

spark.read.csv(f'/user/{username}/courses', header=True).show()

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/orders', sep='|')

#orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|')

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    csv(f'{output_dir}/employee', sep='|', header=True, compression='gzip')

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    option('compression', 'gzip'). \
    option('header', True). \
    option('sep', '|'). \
    csv(f'{output_dir}/dept')

# COMMAND ----------

spark.read. \
    csv(f'/user/{username}/dept', sep='|', header=True, inferSchema=True). \
    show()

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    options(sep='|', header=True, compression='gzip'). \
    csv(f'{output_dir}/dept')

# COMMAND ----------

spark.read.csv(f'/user/{username}/dept', sep='|', header=True, inferSchema=True).show()

# COMMAND ----------

options = {
    'sep': '|',
    'header': True,
    'compression': 'snappy'
}

# COMMAND ----------

emp. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    options(**options). \
    csv(f'{output_dir}/output')

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    json(
        f'/FileStore/data/abc', 
        mode='overwrite'
    )

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    format('json'). \
    save(
        f'/FileStore/data/abc', 
        mode='overwrite'
    )

# COMMAND ----------

# DBTITLE 1,COMPRESS-JSON
emp_df. \
    coalesce(1). \
    write. \
    json(
        f'FileStore/data/abc', 
        mode='overwrite',
        compression='gzip'
    )

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    format('json'). \
    save(
        f'/FileStore/data/abcd', 
        mode='overwrite',
        compression='snappy'
    )

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    parquet(
        f'/FileStore/{username}/data', 
        mode='overwrite'
    )

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    format('parquet'). \
    save(
        f'/FileStore/{path}/', 
        mode='overwrite'
    )

# COMMAND ----------

spark.conf.get('spark.sql.parquet.compression.codec')

# COMMAND ----------

# Write parquet files without compression
# compression can be set to none or uncompressed
dept_df. \
    coalesce(1). \
    write. \
    parquet(
        f'/output/{path}/gggg', 
        mode='overwrite',
        compression='none'
    )

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    parquet(
        f'/user/{username}/courses', 
        mode='overwrite',
        compression='gzip'
    )

# COMMAND ----------

spark.conf.set('spark.sql.parquet.compression.codec', 'none')

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    parquet(
        f'/{path}/pouput', 
        mode='overwrite'
    )

# COMMAND ----------

# DBTITLE 1,MODES
emp_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    parquet(f'')

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    mode('append'). \
    parquet(f'/')

# COMMAND ----------

dbutils.fs.ls('databricks-datasets/')

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/COVID', header=True, inferSchema=True)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

#emp_df.rdd.getNumPartitions()
empdf2=emp_df.repartition(10)

empdf2.rdd.getNumPartitions()

# COMMAND ----------



# COMMAND ----------

# coalescing the Dataframe to 16
df.coalesce(16).rdd.getNumPartitions()

# COMMAND ----------

# not effective as coalesce can be used to reduce the number of partitioning.
# Faster as no shuffling is involved
df.coalesce(186).rdd.getNumPartitions()

# COMMAND ----------

# repartitioned to higher number of partitions
df.repartition(186, 'Year', 'Month').rdd.getNumPartitions()

# COMMAND ----------

#coalesce

df.write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')
#10mins

df.repartition(16).write.mode('overwrite').csv(f'/user/{username}/airlines', header=True, compression='gzip')
#20 mins

# COMMAND ----------

from pyspark.sql.functions import *

emp=[
    (7839,"KING", "PRESIDENT",1234,"1981-11-10",5000,100,10),
    (7698,"BLAKE", "MANAGER",7839,"1981-05-01",2850,500,30),
    (7782,"CLARK", "MANAGER",7839, "1981-06-09", 2450,7890 ,10),
    (7566, "JONES", "MANAGER",7839, "1981-04-01", 2975,987,20),
    (7788, "SCOTT",  "ANALYST",7566, "1990-04-07",3000,786,20),
    (7902, "FORD", "ANALYST",7566, "1981-03-12", 3000,765,20),
    (7369,"SMITH",  "CLERK",7902, "1980-12-01", 800,40,20),
    (7499, "ALLEN",  "SALESMAN", 7698, "1981-02-20",1600,300, 30),
    (7521, "WARD" ,  "SALESMAN", 7698, "1981-02-03",    1250,     500,    30),
    (7654, "MARTIN",    "SALESMAN",    7698, "1981-09-28", 1250 ,   1400,    30),
    (7844, "TURNER",    "SALESMAN",    7698, "1981-09-08",    1500,       0,    30),
    (7876, "ADAMS",    "CLERK",       7788, "1987-09-09",    1100,90,            20),
    (7900,"JAMES",     "CLERK",       7698, "1981-12-03",     950,90,            30),
    (7934, "MILLER",    "CLERK",       7782, "1982-01-01" ,   1300 ,80,           10)
]


emp_df = spark.createDataFrame(emp, schema="empno INTEGER, ename STRING,job STRING,mgr INTEGER,hiredate STRING,sal INTEGER,comm INTEGER,deptno INTEGER")


df_filter=emp_df.filter(col("deptno")==10)

df_add=df_filter.withColumn("city",lit("HYD"))


df_add.write.mode("overwrite").saveAsTable("spark.emp")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema spark;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC use spark;
# MAGIC show tables;
