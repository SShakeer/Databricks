# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

emp_df.alias('c').select('c.*').show()

# COMMAND ----------

# DBTITLE 1,INNER JOIN
#inner join
emp_df2=emp_df.withColumnRenamed("deptno","deptno_emp")
emp_df2.join(dept_df, emp_df2.deptno_emp == dept_df.deptno).select("deptno","dname","ename").show()

# COMMAND ----------

#df_join=emp_df.join(dept_df,"deptno")
df_join=emp_df.join(dept_df,emp_df["deptno"]==dept_df["deptno"])

# COMMAND ----------

df_join.select("ename","empno","sal","dname","loc").show()

# COMMAND ----------

# as both data frames have user id using same name, we can pass column name as string as well
emp_df. \
    join(dept_df, 'deptno'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno). \
    select(emp_df['*'], dept_df['loc'], dept_df['dname']). \
    show()

# COMMAND ----------

# using alias
emp_df.alias('e'). \
    join(dept_df.alias('ce'), emp_df.deptno == dept_df.deptno). \
    select('u.*', 'dname', 'loc'). \
    show()

# COMMAND ----------

# DBTITLE 1,display dept wise employee count
emp_df.alias('u'). \
    join(dept_df.alias('ce'), emp_df.deptno == dept_df.deptno). \
    groupBy('e.deptno'). \
    count(). \
    show()

# COMMAND ----------

emp_df.groupBy("deptno").count().show()

# COMMAND ----------

# DBTITLE 1,LEFT OUTER
emp_df. \
    join(dept_df, 'deptno', 'right_outer').\
    show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC e.ename,
# MAGIC e.sal,
# MAGIC e.deptno,
# MAGIC d.dname,
# MAGIC d.loc from emp e cross join dept d ;
# MAGIC

# COMMAND ----------

# left or left_outer or leftouter are same.

emp_df. \
    join(dept_df, 'deptno', 'left_outer'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'left'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'left'). \
    select(emp_df['*'], dept_df['dname'], dept_df['loc']). \
    show()

# COMMAND ----------

emp_df.alias('u'). \
    join(dept_df.alias('ce'), emp_df.deptno == dept_df.deptno, 'left'). \
    filter('ce.dname IS NOT NULL'). \
    select('u.*', 'dname', 'loc'). \
    show()

# COMMAND ----------

# DBTITLE 1,RIGHT OUTER
emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right_outer'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'right'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right'). \
    select(emp_df['*'], dept_df['dname'], dept_df['loc']). \
    show()

# COMMAND ----------

from pyspark.sql.functions import *
dept_df.alias('ce'). \
    join(emp_df.alias('u'), emp_df.deptno == dept_df.deptno, 'right'). \
    groupBy('ce.deptno'). \
    agg(sum(when(emp_df['sal'].isNull(), 0).otherwise(emp_df['sal'])).alias('total_salary')). \
    show()

# COMMAND ----------

# DBTITLE 1,FULL OUTER
emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'fullouter'). \
    show()

# COMMAND ----------

users1_df. \
    join(users2_df, users1_df.email == users2_df.email, 'full_outer'). \
    show()

# COMMAND ----------

emp_df.join(dept_df, 'deptno', 'left'). \
    union(
        emp_df. \
            join(dept_df, 'deptno', 'right')
    ). \
    distinct(). \
    show()

# COMMAND ----------

# DBTITLE 1,BROAD CAST JOIN
# MAGIC %%time
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC
# MAGIC # We can use broadcast function to override existing broadcast join threshold
# MAGIC # We can also override by using this code spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '1500m')
# MAGIC broadcast(clickstream).join(articles, articles.id == clickstream.curr_id).count()

# COMMAND ----------

# DBTITLE 1,CROSS JOIN
# Number of records will be equal to 
# number of records in first data frame multipled by number of records in second data frame
emp_df. \
    crossJoin(courses_df). \
    count()

# COMMAND ----------

emp_df. \
    join(dept_df, how='inner'). \
    show()

# COMMAND ----------

# DBTITLE 1,leftsemi
emp_df. \
    join(dept_df, 'deptno', 'leftsemi'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'leftanti'). \
    show()
