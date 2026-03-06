from pyspark.sql.functions import *
from pyspark.sql.types import *

#ADLS configconfiguration 
spark.conf.set(
    "fs.azure.account.key.StorageAccountName.dfs.core.windows.net",
    "<<AccessKey>>"
)

bronze_path = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/patient_flow"

silver_path = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/patient_flow"


# read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# define schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

#parse it to dataframe
parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"),schema)).select("data.*")

# convert type to timestamp
clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))

#invalid admission time
clean_df = clean_df.withColumn("admission_time",
                               when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()), 
                                   current_timestamp())
                                   .otherwise(col("admission_time")))
#handle invalid age
clean_df = clean_df.withColumn("age", 
                               when(
                                   col("age") > 100, floor(rand()*90+1).cast("int"))
                                 .otherwise(col("age")))
#schema evolution
expected_cols = ['patient_id', 'gender', 'age', 'department', 'admission_time', 'discharge_time', 'bed_id', 'hospital_id']

for col_name in expected_cols:
  if col_name not in clean_df.columns:
      clean_df = clean_df.withColumn(col_name, lit(None))

# write to silver
checkpoint_path = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/_checkpoints/patient_flow"

(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_path)
    .start(silver_path)
)
    