# Databricks notebook source


# COMMAND ----------

datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")
#datetimesDF.show(truncate=False)



# COMMAND ----------

from pyspark.sql.functions import *
df1=datetimesDF.withColumn("newdatw",current_date()).withColumn("tstamp",current_timestamp())

# COMMAND ----------

df2=df1.withColumn("year",date_format(col("date"),'yyyy')).withColumn("month",date_format(col("date"),'MM')).show()



# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date()

# COMMAND ----------

datetimesDF.show()

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

from pyspark.sql.functions import *

datetimesDF.select(current_date())

# COMMAND ----------

from pyspark.sql.functions import *
df1=emp_df.\
withColumn("adddays",current_date()+10).\
withColumn("addmonths",add_months(current_date(),10)).show()

# COMMAND ----------

df2=emp_df.withColumn("hiredate2",date_format("hiredate",'yyyyMMdd')).show()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import lit, to_date, to_timestamp
df3=dept_df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date'))
#dept_df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

# COMMAND ----------

datetimesDF. \
    withColumn("date_add_date", date_add("date", 10)). \
    withColumn("date_add_time", date_add("time", 10)). \
    withColumn("date_sub_date", date_sub("date", 10)). \
    withColumn("date_sub_time", date_sub("time", 10)). \
    show()

# COMMAND ----------

datetimesDF. \
    withColumn("datediff_date", datediff(current_date(), "date")). \
    withColumn("datediff_time", datediff(current_timestamp(), "time")). \
    show()

# COMMAND ----------

emp_df. \
    withColumn("exp", round(datediff(current_date(), "hiredate")/365)).\
    withColumn("monthsbetween", round(months_between(current_date(), "hiredate"), 2)).\
    withColumn("mots", round(datediff(current_date(),col("hiredate"))/30)).show()


# COMMAND ----------

datetimesDF. \
    withColumn("months_between_date", round(months_between(current_date(), "date"), 2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"), 2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_trunc", trunc("date", "MM")). \
    withColumn("time_trunc", trunc("time", "yy")). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)

# COMMAND ----------

emp_df. \
    withColumn("date_trunc", trunc("hiredate", "MM")). \
    withColumn("time_trunc", trunc("hiredate", "yy")). \
    show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import date_format

datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss")). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss")). \
    show(truncate=False)

# COMMAND ----------

emp_df. \
    withColumn("date_dt", date_format("hiredate", "yyyy-MM-dd HH:mm:ss")). \
    withColumn("ccc",date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")).\
    show(truncate=False)


# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    show(truncate=False)

# COMMAND ----------

datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format
datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

# yyyy
# MM
# dd
# DD
# HH
# hh
# mm
# ss
# SSS

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

datetimesDF. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    withColumn("unix_time", unix_timestamp("time")). \
    show()

# COMMAND ----------

unixtimes = [(1393561800, ),
    (1456713488, ),
    (1514701799, ),
    (1567189800, )
   ]

# COMMAND ----------

unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")

# COMMAND ----------

from pyspark.sql.functions import from_unixtime
unixtimesDF. \
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")). \
    withColumn("time", from_unixtime("unixtime")). \
    show()
#yyyyMMdd
