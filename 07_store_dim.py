#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Script to Load Dimesion


# In[2]:


# Import required libraries
import sys
from lib.spark_session import get_spark_session
from lib.utils import date_data, get_string_cols, get_rundate
from lib.job_control import insert_log, get_max_timestamp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, expr, to_date, date_format, udf
from pyspark.sql.types import StringType
from datetime import datetime
from delta import DeltaTable
import uuid


# In[3]:


# JOB Parameters
rundate = get_rundate()
schema_name = "edw"
table_name = "dim_store"
table_full_name = f"{schema_name}.{table_name}"
staging_table_full_name = "edw_stg.dim_store_stg"
print("SPARK_APP: JOB triggered for rundate - " + rundate)


# In[4]:


# Generate Spark Session
spark: SparkSession = get_spark_session(f"Dimension load - {table_full_name}")
print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)


# In[5]:


# Spark Configs
spark.conf.set("spark.sql.shuffle.partitions", 8)


# In[6]:


# Read data from Staging
df_stg = spark \
    .read \
    .table(staging_table_full_name)

print("SPARK_APP: Staging Data Count - " + str(df_stg.count()))
print("SPARK_APP: Printing Staging Schema --")
df_stg.printSchema()


# In[8]:


# Generated uuid UDF for Surrogate Key
uuidUDF = udf(lambda : str(uuid.uuid4()),StringType())


# In[9]:


# Generate SURROGATE KEYs
df_dim_temp = df_stg.withColumn("row_wid", uuidUDF())

print("SPARK_APP: Dim Temp Data Count - " + str(df_dim_temp.count()))
print("SPARK_APP: Printing Dim Temp Schema --")
df_dim_temp.printSchema()


# In[10]:


# Get the delta table for Upserts (SCD1)
dt_dim = DeltaTable.forName(spark, table_full_name)

# Validate if the load is full load
if get_max_timestamp(spark, schema_name, table_name) == "1900-01-01 00:00:00.000000":
    print("SPARK_APP: Table is set for full load") 
    # Truncate the Dimension table
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    dt_dim.delete(f"1=1")
    dt_dim.vacuum(0)
    
# Create the UPSERT logic
dt_dim.alias("dim_store") \
    .merge(df_dim_temp.alias("dim_temp"), "dim_store.store_id = dim_temp.store_id") \
    .whenMatchedUpdate(set =
        {
           "store_name" : "dim_temp.store_name",
            "address" : "dim_temp.address",
            "city" : "dim_temp.city",
            "state" : "dim_temp.state",
            "zip_code" : "dim_temp.zip_code",
            "phone_number" : "dim_temp.phone_number",
            "rundate" : "dim_temp.rundate",
            "update_dt" : "dim_temp.update_dt"
        }  
    ) \
    .whenNotMatchedInsertAll() \
    .execute()


# In[11]:


# Add job details in JOB CONTROL
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")


# In[12]:


spark.sql(f"select * from edw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)


# In[13]:


# Get the logs from delta table version
dt_dim.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numTargetRowsInserted",
                                "operationMetrics.numTargetRowsUpdated",
                                "operationMetrics.numOutputRows").show(1, False)


# In[14]:


# Generate Symlink manifest for Athena Access
dt = DeltaTable.forName(spark, table_full_name)
dt.generate("symlink_format_manifest")
print("SPARK_APP: Symlink Manifest file generated")


# In[15]:


# Validate data
spark.sql('select * from edw.dim_store').show()


# In[16]:


spark.sql('select * from edw_stg.dim_store_stg').show()


# In[17]:


spark.sql('select * from edw_ld.dim_store_ld').show()


# In[ ]:


spark.stop()

