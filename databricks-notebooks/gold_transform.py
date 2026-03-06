from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, sha2, concat_ws, coalesce, current_timestamp, monotonically_increasing_id
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors","false")

# 1. ADLS Configuration
spark.conf.set("fs.azure.account.key.StorageAccountName.dfs.core.windows.net",
    "<<AccessKey>>")

# 2. Paths
silver_path = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/patient_flow"
gold_dim_patient ="abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/dim_department"
gold_fact = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/fact_patient_flow"

# 3. Read Silver Data
silver_df = spark.read.format("delta").load(silver_path)

# -------------------------------------------------------------------------
# 4. Patient Dimension Logic (SCD Type 2)
# -------------------------------------------------------------------------

# A. Prepare Incoming History
incoming_all_history = silver_df.filter("patient_id IS NOT NULL") \
    .select("patient_id", coalesce(col("gender"), lit("Unknown")).alias("gender"), 
            coalesce(col("age"), lit(0)).alias("age"), "admission_time") \
    .withColumn("_hash", sha2(concat_ws("||", col("gender"), col("age").cast("string")), 256)) \
    .withColumn("rank", F.row_number().over(Window.partitionBy("patient_id", "_hash").orderBy(F.col("admission_time").desc()))) \
    .filter("rank == 1").drop("rank") \
    .withColumn("effective_from", col("admission_time"))

# B. DEFENSIVE INITIALIZATION 
table_exists = DeltaTable.isDeltaTable(spark, gold_dim_patient)
if table_exists:
    cols = spark.read.format("delta").load(gold_dim_patient).columns
    if "is_current" not in cols:
        print("Old schema detected. Wiping...")
        dbutils.fs.rm(gold_dim_patient, recurse=True)
        table_exists = False

if not table_exists:
    print("Initializing Patient Table...")
    window_init = Window.orderBy("patient_id", "effective_from")
    initial_df = incoming_all_history.withColumn("surrogate_key", (F.row_number().over(window_init) + 1).cast("long")) \
                        .withColumn("effective_to", lit(None).cast("timestamp")) \
                        .withColumn("is_current", lit(True)) \
                        .select("surrogate_key", "patient_id", "gender", "age", "effective_from", "effective_to", "is_current", "_hash")
    
    unknown_data = [(1, "Unknown", "Unknown", 0, None, None, True, "INITIAL")]
    unknown_df = spark.createDataFrame(unknown_data, initial_df.schema)
    
    unknown_df.unionByName(initial_df).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_dim_patient)
else:
    # C. SCD 2 MERGE (Only if table exists)
    incoming_all_history.createOrReplaceTempView("v_incoming")
    spark.sql(f"""
        MERGE INTO delta.`{gold_dim_patient}` AS t
        USING (SELECT *, ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY admission_time DESC) as latest_rank FROM v_incoming) AS u
        ON t.patient_id = u.patient_id AND t.is_current = true AND u.latest_rank = 1
        WHEN MATCHED AND t._hash <> u._hash THEN
          UPDATE SET t.is_current = false, t.effective_to = current_timestamp()
    """)

    # D. INSERT NEW RECORDS
    current_max_p_id = spark.read.format("delta").load(gold_dim_patient).select(F.max("surrogate_key")).collect()[0][0] or 1
    new_records = incoming_all_history.alias("inc").join(
        spark.read.format("delta").load(gold_dim_patient).alias("tgt"), 
        (col("inc.patient_id") == col("tgt.patient_id")) & (col("inc._hash") == col("tgt._hash")), "left") \
        .filter("tgt._hash IS NULL") \
        .withColumn("row_num", F.row_number().over(Window.orderBy("inc.patient_id"))) \
        .withColumn("surrogate_key", (col("row_num") + lit(current_max_p_id)).cast("long")) \
        .select("surrogate_key", "inc.patient_id", "inc.gender", "inc.age", "inc.effective_from", 
                lit(None).cast("timestamp").alias("effective_to"), lit(True).alias("is_current"), "inc._hash")

    if new_records.count() > 0:
        new_records.write.format("delta").mode("append").save(gold_dim_patient)

# -------------------------------------------------------------------------
# 5. Department Dimension (SCD Type 1)
# -------------------------------------------------------------------------
# Get latest snapshot for departments
silver_latest_df = silver_df.withColumn("row_num", F.row_number().over(Window.partitionBy("patient_id").orderBy(col("admission_time").desc()))).filter("row_num == 1").drop("row_num")

incoming_dept = silver_latest_df.select(coalesce(col("department"), lit("Unknown")).alias("department"), 
                                        coalesce(col("hospital_id"), lit(0)).alias("hospital_id")).dropDuplicates()

if not DeltaTable.isDeltaTable(spark, gold_dim_department):
    incoming_dept.withColumn("surrogate_key", F.row_number().over(Window.orderBy("department")).cast("long")) \
                 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_dim_department)
else:
    existing_dept = spark.read.format("delta").load(gold_dim_department)
    new_depts = incoming_dept.join(existing_dept, ["department", "hospital_id"], "left_anti")
    if new_depts.count() > 0:
        curr_max_d_id = existing_dept.select(F.max("surrogate_key")).collect()[0][0] or 0
        new_depts.withColumn("row_num", F.row_number().over(Window.orderBy("department"))) \
                 .withColumn("surrogate_key", (col("row_num") + lit(curr_max_d_id)).cast("long")).drop("row_num") \
                 .write.format("delta").mode("append").save(gold_dim_department)

# -------------------------------------------------------------------------# ==========================================================
# 6 FACT TABLE (OPTIMIZED)
# ==========================================================

if DeltaTable.isDeltaTable(spark, gold_dim_patient) and DeltaTable.isDeltaTable(spark, gold_dim_department):

    dim_p = spark.read.format("delta").load(gold_dim_patient).filter("is_current = true")
    dim_d = spark.read.format("delta").load(gold_dim_department)

    fact_df = (

        silver_df.alias("s")

        .join(dim_p.alias("p"),
              coalesce(col("s.patient_id"), lit("Unknown")) == col("p.patient_id"),
              "left")

        .join(dim_d.alias("d"),
              (coalesce(col("s.department"), lit("Unknown")) == col("d.department")) &
              (coalesce(col("s.hospital_id"), lit(0)) == col("d.hospital_id")),
              "left")

        .select(

            monotonically_increasing_id().alias("fact_id"),

            coalesce(col("p.surrogate_key"), lit(1)).cast("long").alias("patient_sk"),

            coalesce(col("d.surrogate_key"), lit(1)).cast("long").alias("department_sk"),

            col("s.admission_time"),

            col("s.discharge_time"),

            to_date("s.admission_time").alias("admission_date"),

            ((unix_timestamp("s.discharge_time") - unix_timestamp("s.admission_time")) / 3600.0)
            .alias("length_of_stay_hours"),

            when(col("s.discharge_time") > current_timestamp(), True)
            .otherwise(False)
            .alias("is_currently_admitted"),

            col("s.bed_id"),

            current_timestamp().alias("ingestion_time")

        )
    )

    # FIX SMALL FILE PROBLEM
    fact_df = fact_df.repartition(20)

    fact_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("admission_date") \
    .save(gold_fact)
    spark.sql(f"OPTIMIZE delta.`{gold_fact}`")

    print("Pipeline completed successfully")

else:

    print("Dimensions missing. Fact load skipped.")