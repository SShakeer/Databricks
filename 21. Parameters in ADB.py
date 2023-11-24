# Databricks notebook source
dbutils.widgets.dropdown("Deptno","10",["10","20"])

# COMMAND ----------

dbutils.widgets.text("deptno","10")

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format("csv").option("header","true").option("inferSchema","true").\
    load("/mnt/raw/Emp.csv")

param=dbutils.widgets.get("deptno")
print(param)
df.filter(col("deptno")==dbutils.widgets.get("deptno")).show()

# COMMAND ----------

dbutils.widgets.removeAll()
