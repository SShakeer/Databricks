# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

from pyspark.sql.functions import *
emp_df.filter(col('empno') == 7839).show()

# COMMAND ----------

#emp_df.where(col('empno') == 7839).show()

emp_df.where(emp_df['empno'] == 7839).show()
#emp_df.filter('empno = 7839').show()

# COMMAND ----------

# DBTITLE 1,Display emp details whose job is not manager
emp_df. \
    select('*'). \
    filter(col('job') != 'MANAGER'). \
    show()

# COMMAND ----------

emp_df. \
    select('empno', 'job'). \
    where((col('job') != 'CLERK') | (col('job').isNull())). \
    show()

# COMMAND ----------

df100=emp_df.select('empno','ename', 'job').filter("job != 'CLERK' OR job IS NULL")
df100.display()

# COMMAND ----------

emp_df. \
    select('empno', 'ename', 'hiredate','deptno'). \
    filter(col('deptno').between(10, 20)).show()

# COMMAND ----------

#emp_df.select('empno', 'ename', 'sal').filter("hiredate BETWEEN '1987-09-09' AND '1981-03-12'").show()
emp_df.select('empno', 'ename', 'sal').filter("deptno BETWEEN 10 AND 20").show()

# COMMAND ----------

emp_df.select('empno', 'sal').filter(col('sal').between(100, 900)).show()

# COMMAND ----------

emp_df. \
    select('ename', 'sal'). \
    filter('sal BETWEEN 3000 AND 9000'). \
    show()


# COMMAND ----------

from pyspark.sql.functions import *
emp_df.select('empno', 'ename','comm').filter(col('comm').isNotNull()).display()

# COMMAND ----------

emp_df. \
    select('empno', 'ename'). \
    filter(col('job').isNull()). \
    show()

# COMMAND ----------

emp_df. \
    select('empno', 'ename','hiredate'). \
    filter('comm IS NULL'). \
    show()

# COMMAND ----------

emp_df. \
    select('empno', 'job'). \
    filter((col('job') == '') | (col('empno').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') == '') | col('current_city').isNull()). \
    show()

# COMMAND ----------

dept_df. \
    select('deptno', 'dname'). \
    filter("deptno = '' OR dname IS NULL"). \
    show()

# COMMAND ----------

# DBTITLE 1,Display the dept data belongs to BOSTAN and DALLAS
dept_df. \
    select('deptno', 'loc','Dname'). \
    filter(col('loc').isin('NEW YORK', 'DALLAS')). \
    show()

# COMMAND ----------


dept_df. \
    select('deptno', 'dname'). \
    filter("loc = 'DALLAS' OR loc = 'ABC'"). \
    show()

# COMMAND ----------

emp_df. \
    select('empno', 'ename','sal','job'). \
    filter("job IN ('SALESMAN', 'MANAGER')"). \
    show()

# COMMAND ----------

emp_df. \
    select('empno', 'ename','sal','job'). \
    filter(col("job").isin('SALESMAN','CLERK')). \
    show()

# COMMAND ----------

from pyspark.sql.functions import *
dept_df. \
    select('deptno', 'dname','loc'). \
    filter(col('loc').isin('DALLAS', 'NEW YORK')). \
    show()

# COMMAND ----------

dept_df. \
    select('deptno', 'dname','loc'). \
    where((col('loc').isin('DALLAS', 'NEW YORK')) | (col('loc').isNull())). \
    show()

# COMMAND ----------

dept_df. \
    select('deptno', 'dname','loc'). \
    filter("loc IN ('DALLAS', 'NEW YORK', '') OR deptno IS NULL"). \
    show()

# COMMAND ----------

emp_df. \
    filter((col('sal') >= 2000) & ((col('comm')) <= 800)). \
    select('sal', 'comm','ename'). \
    show()

# COMMAND ----------




# COMMAND ----------

emp_df.select('*').filter(col('hiredate') < '1985-01-21').show()

# COMMAND ----------

emp_df. \
    select('*'). \
    filter('hiredate > "1985-01-21"'). \
    show()

# COMMAND ----------

emp_df. \
    filter((col('hiredate') >= '1985-01-21') & (col('hiredate') < '1990-01-21')). \
    select('*'). \
    show()

# COMMAND ----------

df1=emp_df.select("empno","ename","sal").show()
df1.filter(col("empno")==7839).show()
