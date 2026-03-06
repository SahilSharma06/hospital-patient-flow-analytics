from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<HotsName>>"
event_hub_name="<<EventHubName>>"  
event_hub_conn_str = <<ConnectionString>>

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read data from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
        )

#cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration
spark.conf.set(
    "fs.azure.account.key.StorageAccountName.dfs.core.windows.net",
    "<<AccessKey>>"
)

bronze_path = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net/patient_flow"

#write stream to bronze
checkpoint_path = "abfss://<<Container>>@<<StorageAccountName>>.dfs.core.windows.net/_checkpoints/patient_flow"

(
    json_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)

