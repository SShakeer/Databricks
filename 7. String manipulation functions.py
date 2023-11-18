# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### There are approximately 300 functions under pyspark.sql.functions. At a higher level they can be grouped into a few categories.
# MAGIC
# MAGIC ####1. String Manipulation Functions
# MAGIC
# MAGIC Case Conversion - lower, upper
# MAGIC
# MAGIC Getting Length - length
# MAGIC
# MAGIC Extracting substrings - substring, split
# MAGIC
# MAGIC Trimming - trim, ltrim, rtrim
# MAGIC
# MAGIC Padding - lpad, rpad
# MAGIC
# MAGIC Concatenating string - concat, concat_ws
# MAGIC
# MAGIC ####2. Date Manipulation Functions
# MAGIC
# MAGIC Getting current date and time - current_date, current_timestamp
# MAGIC
# MAGIC Date Arithmetic - date_add, date_sub, datediff, months_between, add_months, next_day
# MAGIC
# MAGIC Beginning and Ending Date or Time - last_day, trunc, date_trunc
# MAGIC
# MAGIC Formatting Date - date_format
# MAGIC
# MAGIC Extracting Information - dayofyear, dayofmonth, dayofweek, year, month
# MAGIC
# MAGIC ####3. Aggregate Functions
# MAGIC
# MAGIC count, countDistinct
# MAGIC
# MAGIC sum, avg
# MAGIC
# MAGIC min, max
# MAGIC
# MAGIC ####4. Other Functions - We will explore depending on the use cases.
# MAGIC
# MAGIC CASE and WHEN
# MAGIC
# MAGIC CAST for type casting
# MAGIC
# MAGIC Functions to manage special types such as ARRAY, MAP, STRUCT type columns
# MAGIC
# MAGIC Many others

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

from pyspark.sql.functions import col
dept_df. \
    select(col("dname"), col("loc"),col("deptno")). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOWER,UPPER,COUNT,ALIAS,ORDER BY,ASC,DESC

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import upper,lower
emp_df. \
    select(upper(col("ename")).alias("uename"), initcap(col("job")).alias("newjob")). \
    show()

# COMMAND ----------

emp_df. \
    groupBy(upper(col("job")).alias("job")). \
    count(). \
    show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp order by empno asc

# COMMAND ----------

# We can invoke desc on columns which are of type column
emp_df. \
    orderBy(col("empno").asc()). \
    show()

# COMMAND ----------

# Alternative - we can also refer column names using Data Frame like this
emp_df. \
    orderBy(lower(emp_df['ename']).alias('emp_name')). \
    show()


# COMMAND ----------

#Alternative - we can also refer column names using Data Frame like this
emp_df. \
    orderBy(upper(emp_df.ename).alias('emp_name')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import concat,lit
df2=emp_df. \
    withColumn("full_name", concat("ename",lit(' and his job is '), "job")).\
withColumn("city",lit("Bangalore"))
display(df2)

# COMMAND ----------

# DBTITLE 1,col, lower, upper, initcap, length
from pyspark.sql.functions import col, lower, upper, initcap, length
df3=emp_df. \
  select("empno", "ename","job"). \
  withColumn("ename_upper", upper(col("ename"))). \
  withColumn("ename_lower", lower(col("ename"))). \
  withColumn("ename_initcap", initcap(col("ename"))). \
  withColumn("ename_length", length(col("ename")))

# COMMAND ----------

df4=df3.where(col("ename_length")>=5).show()

# COMMAND ----------

# DBTITLE 1,substr,col
s = "Hello World"
# Extracts first 5 characters from the string
s[:5]
# Extracts characters from 2nd to 4th (3 characters). 
# Second argument is length of the string that need to be considered.
s[1:4]
l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")
from pyspark.sql.functions import substring, lit
# Function takes 3 arguments
# First argument is a column from which we want to extract substring.
# Second argument is the character from which string is supposed to be extracted.
# Third argument is number of characters from the first argument.
df.select(substring(lit("Hello World"), 7, 5)). \
  show()

df.select(substring(lit("Hello World"), -5, 5)).show()

# COMMAND ----------

from pyspark.sql.functions import substring, col

emp_df. \
    select("empno", "ename", "sal"). \
    withColumn("sal2", substring(col("sal"), -2, 1).cast("int")). \
    withColumn("sal3", substring(col("sal"), 2, 2).cast("int")). \
    show()

# COMMAND ----------

# DBTITLE 1,split, explode
employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_numbers STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF. \
    select('employee_id', 'phone_numbers'). \
    show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, explode
employeesDF = employeesDF. \
    select('employee_id', 'phone_numbers', 'ssn'). \
    withColumn('phone_number', explode(split('phone_numbers', ',')))
employeesDF.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,lpad,rpad
from pyspark.sql.functions import lpad, rpad, concat
empFixedDF = emp_df.select(
    concat(
        lpad("empno", 5, "0"), 
        rpad("ename", 10, "-"), 
        rpad("job", 10, "-"),
        lpad("sal", 10, "0"), 
        rpad("hiredate", 15, "-"), 
        rpad("deptno", 17, "-")
    ).alias("employee")
).display()

# COMMAND ----------

from pyspark.sql.functions import lpad, rpad, concat
empFixedDF = emp_df.select(
  "empno",
        lpad("empno", 10, "0"), 
        rpad("ename", 10, "-"), 
        rpad("job", 10, "-"),
        lpad("sal", 10, "0"), 
        rpad("hiredate", 15, "-"), 
        rpad("deptno", 17, "-"))


display(empFixedDF)

# COMMAND ----------

# DBTITLE 1,when other
from pyspark.sql.functions import * 

emp_df.withColumn("duty",
 when(col("job") == 'CLERK', 'office work')
.when(col("job") == 'MANAGER', 'He is manager')
.when(col("job") == 'PRESIDENT', 'He is president')
.when(col("job") == 'SALESMAN','salling person')
 .otherwise(col("job"))).show()

# COMMAND ----------

from pyspark.sql.functions import *
df2 = emp_df.withColumn("acdffdf",when(col("job") == 'CLERK','Male').when(col("job") == 'PRESIDENT','Female').otherwise('NO gender')).show()
