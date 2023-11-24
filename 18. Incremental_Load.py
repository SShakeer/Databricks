# Databricks notebook source
data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.show()

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Naresh", "company20",10000],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Shakeer","IBM",4000]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_target3
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_target3;

# COMMAND ----------

df.createOrReplaceTempView("emp_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO emp_target3
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target3.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target3.empno=emp_source.empno,
# MAGIC emp_target3.ename=emp_source.ename,
# MAGIC emp_target3.org=emp_source.org,
# MAGIC emp_target3.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO emp_target3
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target3.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO emp_target1
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target1.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target1.empno=emp_source.empno,
# MAGIC emp_target1.ename=emp_source.ename,
# MAGIC emp_target1.org=emp_source.org,
# MAGIC emp_target1.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC Insert into emp_target1 values(emp_source.empno,emp_source.ename,emp_source.org,emp_source.sal);
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,dataframe approache
# MAGIC %sql
# MAGIC create table emp3_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta
# MAGIC location "/FileStore/tables/merge_target4"

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Naresh", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Maheer","Oracle",40200]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.createOrReplaceTempView("emp_source1")

# COMMAND ----------

from delta.tables import *
df_delta=DeltaTable.forPath(spark,"/FileStore/tables/merge_target4")

df_delta.alias("target").merge(
source =df.alias("source"),
  condition="target.empno=source.empno"
).whenMatchedUpdate(set=
                   {
                     "empno":"source.empno",
                     "ename":"source.ename",
                     "org":"source.org",
                     "sal":"source.sal"
                   }).whenNotMatchedInsert(values=
                                          {
                                            "empno":"source.empno",
                                            "ename":"source.ename",
                                            "org":"source.org",
                                            "sal":"source.sal"
                                          }
                                          ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp3_target;

# COMMAND ----------

# DBTITLE 1,extra stuff
spark.sql("""
CREATE TABLE orders (
  order_id INT,
  order_date STRING,
  order_customer_id INT,
  order_status STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC --USE default;
# MAGIC show tables

# COMMAND ----------

spark.sql("SHOW tables").show()
spark.sql("USE database")
spark.sql("DROP TABLE emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students (
# MAGIC     student_id INT,
# MAGIC     student_first_name STRING,
# MAGIC     student_last_name STRING,
# MAGIC     student_phone_number STRING,
# MAGIC     student_address STRING
# MAGIC ) STORED AS TEXTFILE
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students (
# MAGIC     student_id INT,
# MAGIC     student_first_name STRING,
# MAGIC     student_last_name STRING,
# MAGIC     student_phone_numbers ARRAY<STRING>,
# MAGIC     student_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING>
# MAGIC ) STORED AS TEXTFILE
# MAGIC ROW FORMAT
# MAGIC     DELIMITED FIELDS TERMINATED BY '\t'
# MAGIC     COLLECTION ITEMS TERMINATED BY ','

# COMMAND ----------

INSERT INTO students VALUES 
    (3, 'Mickey', 'Mouse', ARRAY('1234567890', '2345678901'), STRUCT('A Street', 'One City', 'Some State', '12345')),
    (4, 'Bubble', 'Guppy', ARRAY('5678901234', '6789012345'), STRUCT('Bubbly Street', 'Guppy', 'La la land', '45678'))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

dbutils.fs.rm(f'/FileStore/tables/{folder}/asa/airlines', recurse=True)

# COMMAND ----------

spark.read.csv('dbfs:/databricks-datasets/asa/airlines', header=True). \
    write. \
    partitionBy('Year'). \
    csv(f'/FileStore/tables/{folder}/asa/airlines', header=True, mode='overwrite')

# COMMAND ----------

spark.read.csv(f'/user/{username}/asa/airlines/', header=True).count()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/asa/airlines/Year=2004')
