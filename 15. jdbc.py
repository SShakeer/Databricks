# Databricks notebook source
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = "azuresqlserver202310.database.windows.net"
database_port = 1433 
database_name = "azuretraining2023"
table = "emp"
user = "shakeer"
password =  "234566"

url = f"jdbc:sqlserver://{database_host}:{database_port};databaseName={database_name};user={user};password={password}"

df=spark.read.format("jdbc").option("url",url).option("dbtable","dbo.emp").load()

# COMMAND ----------

(df.write
  .format("jdbc")
  .option("url", url)
  .option("dbtable", "dbo.emp_output").option("driver",driver)
  .save()
)
