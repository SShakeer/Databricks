# Databricks notebook source
# MAGIC %md
# MAGIC ## ORC
# MAGIC * Apache ORC is a columnar format which has more advanced features like native zstd compression, bloom filter and columnar encryption
# MAGIC * Avro and ORC also supports schema evolution
# MAGIC * Users can start with a simple schema, and gradually add more columns to the schema as needed. In this way, users may end up with multiple ORC files with different but mutually compatible schemas.
# MAGIC * The ORC data source is now able to automatically detect this case and merge schemas of all these files.
# MAGIC * you can take advantage of Zstandard compression in ORC files
# MAGIC * CREATE TABLE compressed (
# MAGIC   key STRING,
# MAGIC   value STRING
# MAGIC )
# MAGIC USING ORC
# MAGIC OPTIONS (
# MAGIC   compression 'zstd'
# MAGIC )
# MAGIC
# MAGIC * You can control bloom filters and dictionary encodings for ORC data sources.

# COMMAND ----------

df=spark.read.format("csv").load("path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AVRO
# MAGIC * Avro is a row-based file format, similar to CSV, and was designed for minimizing write latency
# MAGIC * Rich data structures.
# MAGIC * A compact, fast, binary data format.
# MAGIC * Code generation is not required to read or write data files 
# MAGIC * Data is always accompanied by a schema that permits full processing of that data without code generation (Dynamic Typing)
# MAGIC * When Avro data is stored in a file, its schema is stored with it, so that files may be processed later by any program
# MAGIC * If the program reading the data expects a different schema this can be easily resolved, since both schemas are present.
# MAGIC * The most common use case for Avro is streaming data
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet
# MAGIC * Columnlar file format
# MAGIC * In Apache Parquet, each column’s values are stored together on disk. Because analytical queries often need only a subset of columns for an operation, this reduces the amount of data needed to be read.
# MAGIC * Apache Parquet supports highly efficient compression
# MAGIC * Parquet files can be slower to write than row-based file formats

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMGAES

# COMMAND ----------

df = spark.read.format("image").load("<path-to-image-data>")

# COMMAND ----------


