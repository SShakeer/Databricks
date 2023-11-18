# Databricks notebook source
# DBTITLE 1,Window Functions

Window Functions operates on a group of rows and return a single value for each input row. 
Main Package: pyspark.sql.window. It has two classes Window and WindowSpec 
• Window class has APIs such as partitionBy, orderBy, rangeBetween, rowsBetween. 
• WindowSpec class defines the partitioning, ordering and frame boundaries

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select deptno,sal,ename,dense_rank() over (partition by deptno order by sal desc) as rank from emp;
# MAGIC select deptno,sum(sal) as total from emp group by deptno;

# COMMAND ----------

from pyspark.sql.functions import *
#df=emp_df.groupBy("deptno").agg(sum("sal").alias("Total"))
#df=emp_df.groupBy("deptno").agg(avg("sal").alias("Total"))
#df=emp_df.groupBy("deptno").agg(max("sal").alias("Total"))
df=emp_df.groupBy("deptno").agg(count("sal").alias("Total_employees"))
df2=df.filter(col("Total_employees")>5)
df2.show()

# COMMAND ----------

# DBTITLE 1,Dept wise highest salary
from pyspark.sql.functions import *
from pyspark.sql.window import * 

spec = Window.partitionBy("deptno").orderBy(desc("sal"))
df_rank=emp_df.withColumn("rank1",dense_rank().over(spec))

df_filter=df_rank.filter(col("rank1")==1).show()

# COMMAND ----------



from pyspark.sql.functions import *
from pyspark.sql.window import * 

df1=df.where(col("dept")=='Sales')
df2=emp_df.withColumn("city",lit("Bangalore")).withColumn("pincode",lit(12343454)).
df_3434=df2.filter(col("deptno")==10)
spec = Window.partitionBy("deptno").orderBy(desc("sal"))
df3=df2.withColumn("rank1",dense_rank().over(spec))
df4=df3.where(col("rank1")==1).show()

# COMMAND ----------

from pyspark.sql.functions import *
data = [["James","Sales","NY",9000,34], 
        ["Alicia","Sales","NY",8600,56], 
        ["Robert","Sales","CA",8100,30], 
        ["Lisa","Finance","CA",9000,24], 
        ["Deja","Finance","CA",9900,40], 
        ["Sugie","Finance","NY",8300,36], 
        ["Ram","Finance","NY",7900,53], 
        ["Kyle","Marketing","CA",8000,25], 
        ["Reid","Marketing","NY",9100,50]]

schema=("empname","dept","state","salary","age") 
df = spark.createDataFrame(data=data,schema=schema)

df2=df.groupBy(df.dept).max("salary").alias("avvg")
df2.show()

# COMMAND ----------

spec = Window.partitionBy("deptno")
emp_df.select(col("deptno"),col("sal")) \
.withColumn("sum_sal",sum("sal").over(spec)) \
.withColumn("max_sal",max("sal").over(spec)) \
.withColumn("min_sal",min("sal").over(spec)) \
.withColumn("avg_sal",avg("sal").over(spec)) \
.withColumn("count_sal",count("sal").over(spec)).show()


# COMMAND ----------

from pyspark.sql import functions as F
agg_df = emp_df.groupBy("deptno").agg(F.min("sal"), F.count("empno"),F.max("sal")).show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Build an example DataFrame dataset to work with.
dbutils.fs.rm("/tmp/dataframe_sample.csv", True)
dbutils.fs.put("/tmp/dataframe_sample.csv", """id|end_date|start_date|location
1|2015-10-14 00:00:00|2015-09-14 00:00:00|CA-SF
2|2015-10-15 01:00:20|2015-08-14 00:00:00|CA-SD
3|2015-10-16 02:30:00|2015-01-14 00:00:00|NY-NY
4|2015-10-17 03:00:20|2015-02-14 00:00:00|NY-NY
5|2015-10-18 04:30:00|2014-04-14 00:00:00|CA-SD
""", True)

df = spark.read.format("csv").options(header='true', delimiter = '|').load("/tmp/dataframe_sample.csv")
df.show()

# Instead of registering a UDF, call the builtin functions to perform operations on the columns.
# This will provide a performance improvement as the builtins compile and run in the platform's JVM.

# Convert to a Date type
df2 = df.withColumn('date', F.to_date(df.end_date))

# Parse out the date only
df3 = df.withColumn('date_only', F.regexp_replace(df.end_date,' (\d+)[:](\d+)[:](\d+).*$', ''))

# Split a string and index a field
df = df.withColumn('city', F.split(df.location, '-')[0])

# Perform a date diff function
#df = df.withColumn('date_diff', F.datediff(F.to_date(df.end_date), F.to_date(df.start_date)))

# COMMAND ----------

#Provide the min, count, and avg and groupBy the location column. Diplay the results
agg_df = df.groupBy("location").agg(F.min("id"), F.count("id"))
display(agg_df)

# COMMAND ----------

df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date')).show()
#df.write.partitionBy("end_year", "end_month").parquet("/tmp/sample_table")
#display(dbutils.fs.ls("/tmp/sample_table"))

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT empno, deptno, sal,hiredate,
# MAGIC     count(1) OVER (PARTITION BY deptno) AS employee_count,
# MAGIC     rank() OVER (ORDER BY sal DESC) AS rnk,
# MAGIC     lead(sal) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_sal
# MAGIC FROM emp where deptno=10

# COMMAND ----------

df20=spark.sql("SELECT empno, deptno, sal,hiredate,count(1) OVER (PARTITION BY deptno) AS employee_count,rank() OVER (ORDER BY sal DESC) AS rnk,lead(empno) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_id,lead(sal) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_sal FROM emp ORDER BY empno")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC df_sum=emp_df.groupBy("deptno").sum("sal").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC e.empno, 
# MAGIC e.deptno, 
# MAGIC e.sal,
# MAGIC sum(e.sal) OVER (PARTITION BY e.deptno) AS department_salary
# MAGIC FROM emp e
# MAGIC ORDER BY e.deptno

# COMMAND ----------

# DBTITLE 1,SQL Approach
# MAGIC %sql
# MAGIC SELECT e.empno, 
# MAGIC e.deptno, 
# MAGIC e.sal,
# MAGIC     sum(e.sal) OVER (PARTITION BY e.deptno) AS sum_sal_expense,
# MAGIC     avg(e.sal) OVER (PARTITION BY e.deptno) AS avg_sal_expense,
# MAGIC     min(e.sal) OVER (PARTITION BY e.deptno) AS min_sal_expense,
# MAGIC     max(e.sal) OVER (PARTITION BY e.deptno) AS max_sal_expense,
# MAGIC     count(e.sal) OVER (PARTITION BY e.deptno) AS cnt_sal_expense
# MAGIC FROM emp e
# MAGIC ORDER BY e.deptno

# COMMAND ----------

# DBTITLE 1,dataframe approache




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC LAG and LEAD are window functions in SQL that allow you to access data from rows that are either before (LAG) or after (LEAD) the current row within the result set. These functions are often used for time-series analysis and comparisons between rows.

# COMMAND ----------

lead(col1,seq,default)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.*,
# MAGIC   --lead(hiredate,) OVER (ORDER BY hiredate DESC) AS prior_date,
# MAGIC   --lead(sal) OVER (ORDER BY sal DESC) AS prior_revenue,
# MAGIC   --lag(hiredate) OVER (ORDER BY hiredate) AS lag_prior_date,
# MAGIC   lag(sal,1,0) OVER (ORDER BY sal) AS lag_prior_revenue,
# MAGIC     lag(sal,2,0) OVER (ORDER BY sal) AS lag_prior_revenue2
# MAGIC FROM emp AS t

# COMMAND ----------

# DBTITLE 1,df approache
from pyspark.sql.functions import *
from pyspark.sql.window import * 


spec = Window.partitionBy("deptno").orderBy(desc("sal"))
df3=emp_df.withColumn("prior_revenue",lead('sal',1,0).over(spec)).withColumn("post_revenue",lag('sal',1,0).over(spec)).show()


# COMMAND ----------

# DBTITLE 1,Rank(),Dense_rank(),Row_number()
# MAGIC %sql
# MAGIC SELECT
# MAGIC   empno,
# MAGIC   deptno,
# MAGIC   sal,
# MAGIC   rank() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) rnk,
# MAGIC   dense_rank() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) drnk,
# MAGIC   row_number() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) rn
# MAGIC FROM emp
# MAGIC ORDER BY deptno, sal DESC

# COMMAND ----------

spec = Window.partitionBy("deptno").orderBy(desc("sal"))

df2=emp_df.select("empno","deptno","sal")
df3=df2.withColumn("rnk",rank().over(spec)).\
  withColumn("drnk",dense_rank().over(spec))

# COMMAND ----------

# DBTITLE 1,rollup,cube
# MAGIC %sql
# MAGIC SELECT 
# MAGIC         deptno, SUM(sal) AS TotalSales
# MAGIC     FROM 
# MAGIC         emp
# MAGIC     GROUP BY 
# MAGIC         ROLLUP (deptno)

# COMMAND ----------

emp_df.rollup("deptno","job").agg(sum("sal")).show()

# COMMAND ----------

emp_df.cube("deptno","job").agg(sum("sal")).show()

# COMMAND ----------

emp_df.cube("deptno","job").count().sort(asc("deptno")).show()
