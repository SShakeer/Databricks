# Databricks notebook source
# MAGIC %fs
# MAGIC ls '/databricks-datasets'

# COMMAND ----------

# MAGIC %sh
# MAGIC tail /databricks-datasets/airlines/part-0000

# COMMAND ----------

# MAGIC %lsmagic
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

#dbutils.fs.head("/FileStore/tables/emp.csv",10)
#dbutils.fs.ls("/FileStore/tables")
dbutils.fs.mkdirs("/FileStore/tables/Azure")
#dbutils.fs.rm("/FileStore/tables/Reddy")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/wordcount.txt")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/AzureCourse/Dataframes"

# COMMAND ----------

dbutils.notebook.run("/Users/basha.ora11@gmail.com/AzureCourse/Dataframes",100)

# COMMAND ----------


