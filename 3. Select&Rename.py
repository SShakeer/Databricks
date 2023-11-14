# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

help(emp_df.select)

# COMMAND ----------

emp_df.select('*').show()

# COMMAND ----------

emp_df.select('empno', 'ename', 'hiredate','sal').display()

# COMMAND ----------

emp_df.select(['empno', 'ename', 'sal']).show()

# COMMAND ----------

emp_df.alias('e').select('e.*').show()

# COMMAND ----------

dept_df.alias('d').select('d.deptno', 'd.dname', 'd.loc').show()

# COMMAND ----------

from pyspark.sql.functions import col

dept_df.select(col('deptno'), col('dname'), col('loc').alias("location")).show()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
emp_df.select(
    col('empno'), 
    'ename', 
    'job',
    lit('Banaglore').alias('city'),
    concat('ename', lit(' and his job is '), 'job').alias('new_name')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## selectExpr

# COMMAND ----------

help(emp_df.selectExpr)

# COMMAND ----------

emp_df.selectExpr('*').show()

# COMMAND ----------

# Defining alias to the dataframe
emp_df.alias('e').selectExpr('e.*').show()

# COMMAND ----------

emp_df.selectExpr('empno', 'job', 'hiredate').show()

# COMMAND ----------

emp_df.selectExpr('empno', 'job', 'hiredate',"sal*10 as total").show()

# COMMAND ----------

emp_df.select(emp_df['sal'], emp_df['deptno']).show()

# COMMAND ----------

emp_df.selectExpr('sal', 'deptno', 'comm').show()

# COMMAND ----------

colums1 = ['deptno', 'sal', 'comm','hiredate']
emp_df.select(*colums1).show()

# COMMAND ----------

from pyspark.sql.functions import date_format
df2=emp_df.select('sal', col('deptno'), 'comm',date_format('hiredate','yyyyMMdd').alias("hdate"))

# COMMAND ----------

emp_df.select('sal', col('deptno'), 'comm',date_format('hiredate','yyyyMMdd').cast('int').alias("hdate")).show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

# COMMAND ----------

df2=spark.sql("""select 
ename,
sal,
d.deptno,
d.dname
 from emp e inner join dept d on e.deptno=d.deptno""")


# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import *

df=spark.read.format("csv").option("header",True).option("inferSchema",True).load("dbfs:/FileStore/tables/emp12.csv")

df_select=df.select("name","salary","comm","deptno")

df_filter=df_select.filter(col("deptno")==10)

df_filter.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/azureoutput.csv")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------


