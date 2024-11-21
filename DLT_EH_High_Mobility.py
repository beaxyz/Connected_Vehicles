# Databricks notebook source
#parameter to specify convesion function. Input & output units. Use case when within to define logic for conversion
#csv - different files for different grains

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

spark.conf.set("spark.databricks.variant.enabled","true")

# COMMAND ----------

EH_NAMESPACE= spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME_1= spark.conf.get("iot.ingestion.eh.name_1")
EH_NAME_2= spark.conf.get("iot.ingestion.eh.name_2")

EH_CONN_SHARED_ACCESS_KEY_NAME  = spark.conf.get("iot.ingestion.eh.accessKeyName")
EH_CONN_SHARED_ACCESS_KEY_VALUE = spark.conf.get("iot.ingestion.eh.accessKeyValue")
EH_CONN_STR                     = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE}"

KAFKA_OPTIONS_1 = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME_1,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"
  # "kafka.request.timeout.ms" : spark.conf.get("iot.ingestion.kafka.requestTimeout"),
  # "kafka.session.timeout.ms" : spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
  # "maxOffsetsPerTrigger"     : spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
  # "failOnDataLoss"           : spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
  # "startingOffsets"          : spark.conf.get("iot.ingestion.spark.startingOffsets")
}

KAFKA_OPTIONS_2 = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME_2,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"
}

# COMMAND ----------

schema = 'struct<oem: string, vin: string, ingestion_timestamp: timestamp, response: string>'

# COMMAND ----------

oems = {
"ford":{
  "name":"ford",
  "kafka_config":KAFKA_OPTIONS_1,
  "core_col_list":['oem','vin','ingestion_timestamp'],

  "silver_schema":{
    "dashboard_lights":"record_variant:dashboard_lights:dashboard_lights",
    "battery_voltage":"record_variant:diagnostics:battery_voltage",
    "fuel_level":"record_variant:diagnostics:fuel_level",
    "odometer":"record_variant:diagnostics:odometer",
    "tire_pressures":"record_variant:diagnostics:tire_pressures",
    "trouble_codes":"record_variant:diagnostics:trouble_codes",
    "vehicle_location":"record_variant:vehicle_location",
    "request_id":"record_variant:request_id"
  },

  "dashboard_light_explode_schema" : 'array<struct<data: struct<name:string, state: string>, failure: string, timestamp: timestamp>>',
  "dashboard_lights_schema":{
    "exploded_dashboard_lights":"dashboard_lights",
    "dashboard_light_name":"exploded_dashboard_lights.data.name",
    "dashboard_light_state":"exploded_dashboard_lights.data.state",
    "dashboard_light_failure":"exploded_dashboard_lights.failure",
    "dashboard_light_timestamp": "exploded_dashboard_lights.timestamp"
  },

  "metric_schema" : {
    "fuel_level":{
      "metric_value":"fuel_level:data",
      "metric_timestamp":"fuel_level:timestamp"
      },
    "odometer":{
      "metric_value":"odometer:data:value",
      "metric_unit":"odometer:data:unit",
      "metric_timestamp":"odometer:timestamp"
      },
    "altitude":{
      "metric_value":"vehicle_location:altitude.data:value",
      "metric_unit":"vehicle_location:altitude.data:unit",
      "metric_timestamp":"vehicle_location:altitude:timestamp"
      },
      "longitude":{
      "metric_value":"vehicle_location:coordinates.data:longitude",
      "metric_timestamp":"vehicle_location:coordinates:timestamp"
      },
    "latitude":{
      "metric_value":"vehicle_location:coordinates:data:latitude",
      "metric_timestamp":"vehicle_location:coordinates:timestamp"
      },
    "battery_voltage":{
      "metric_value":"battery_voltage:data:value",
      "metric_unit":"battery_voltage:data:unit",
      "metric_timestamp":"battery_voltage:timestamp"
      } 
  },

  "metric_array_schema" : {
    "tire_pressures":{
      "exploded_metric":"tire_pressures",
      "exploded_metric_schema": "array<struct<data: struct<location:string, pressure: struct<unit: string, value: double>>, failure: string, timestamp timestamp>>",
      "metric_value":"exploded_metric.data.pressure.value",
      "metric_value_detail":"exploded_metric.data.location",
      "metric_unit":"exploded_metric.data.pressure.unit",
      "metric_timestamp":"exploded_metric.timestamp"
    },

    "trouble_codes":{
      "exploded_metric":"trouble_codes",
      "exploded_metric_schema":"array<struct<data: struct<ecu_id:string, id: string, occurrences: integer, status: string, system: string>, failure: string, timestamp timestamp>>",
      "metric_value":"exploded_metric.data.occurrences",
      "metric_value_detail":"exploded_metric.data.id",
      "metric_timestamp":"exploded_metric.timestamp"
    }
  }
},

"porsche":{
  "name":"porsche",
  "kafka_config":KAFKA_OPTIONS_2,
  "core_col_list":['oem','vin','ingestion_timestamp'],

  "silver_schema":{
    "dashboard_lights":"record_variant:dashboard_lights:dashboard_lights",
    "fuel_level":"record_variant:diagnostics:fuel_level",
    "odometer":"record_variant:diagnostics:odometer",
    "tire_pressures":"record_variant:diagnostics:tire_pressures",
    "tire_pressures_differences":"record_variant:diagnostics:tire_pressures_differences",
    "tire_pressure_targets":"record_variant:diagnostics:tire_pressures_targets",
    "vehicle_location":"record_variant:vehicle_location",
    "request_id":"record_variant:request_id"
    },
  
  "dashboard_light_explode_schema" : 'array<struct<data: struct<name:string, state: string>, failure: string, timestamp: timestamp>>',
  "dashboard_lights_schema":{
    "exploded_dashboard_lights":"dashboard_lights",
    "dashboard_light_name":"exploded_dashboard_lights.data.name",
    "dashboard_light_state":"exploded_dashboard_lights.data.state",
    "dashboard_light_failure":"exploded_dashboard_lights.failure",
    "dashboard_light_timestamp": "exploded_dashboard_lights.timestamp"
    },
  
  "metric_schema" : {
    "fuel_level":{
      "metric_value":"fuel_level:data",
      "metric_timestamp":"fuel_level:timestamp"
      },
    "odometer":{
      "metric_value":"odometer:data:value",
      "metric_unit":"odometer:data:unit",
      "metric_timestamp":"odometer:timestamp"
      },
    "altitude":{
      "metric_value":"vehicle_location:altitude:data:value",
      "metric_unit":"vehicle_location:altitude:data:unit",
      "metric_timestamp":"vehicle_location:altitude:timestamp"
      },
      "longitude":{
      "metric_value":"vehicle_location:coordinates:data:longitude",
      "metric_timestamp":"vehicle_location:coordinates:timestamp"
      },
    "latitude":{
      "metric_value":"vehicle_location:coordinates:data:latitude",
      "metric_timestamp":"vehicle_location:coordinates:timestamp"
      }
    },
  
  "metric_array_schema" : {
    "tire_pressures":{
      "exploded_metric":"tire_pressures",
      "exploded_metric_schema":"array<struct<data: struct<location:string, pressure: struct<unit: string, value: double>>, failure: string, timestamp timestamp>>",
      "metric_value":"exploded_metric.data.pressure.value",
      "metric_value_detail":"exploded_metric.data.location",
      "metric_unit":"exploded_metric.data.pressure.unit",
      "metric_timestamp":"exploded_metric.timestamp"
    },

    "tire_pressures_differences":{
      "exploded_metric":"tire_pressures_differences",
      "exploded_metric_schema":"array<struct<data: struct<location:string, pressure: struct<unit: string, value: double>>, failure: string, timestamp timestamp>>",
      "metric_value":"exploded_metric.data.pressure.value",
      "metric_value_detail":"exploded_metric.data.location",
      "metric_unit":"exploded_metric.data.pressure.unit",
      "metric_timestamp":"exploded_metric.timestamp"
    },

    "tire_pressure_targets":{
      "exploded_metric":"tire_pressure_targets",
      "exploded_metric_schema":"array<struct<data: struct<location:string, pressure: struct<unit: string, value: double>>, failure: string, timestamp timestamp>>",
      "metric_value":"exploded_metric.data.pressure.value",
      "metric_value_detail":"exploded_metric.data.location",
      "metric_unit":"exploded_metric.data.pressure.unit",
      "metric_timestamp":"exploded_metric.timestamp"
      }
    }
  }
}

# COMMAND ----------

def create_raw_table(oem):
  @dlt.create_table(
    name = f"{oems[oem]['name']}_raw",
    table_properties = {"overwriteSchema": "true", "delta.feature.variantType-preview": "supported"}
  )

  def create_table():
    return (
    spark.readStream
    .format("Kafka")
    .options(**oems[oem]['kafka_config'])
    .load()
    .withColumn("records", F.col("value").cast("string"))
    .withColumn("records_json", F.from_json(F.col("records"), schema))
    .withColumn("record_variant", F.parse_json(F.col("records_json.response")).cast("variant"))
    .select("records_json.oem","records_json.vin","records_json.ingestion_timestamp","record_variant")
    )

# COMMAND ----------

def create_silver_table(oem):
  col_list = [f'{value} as {key}' for key, value in oems[oem]['silver_schema'].items()]   
  @dlt.create_table(
    name = f"{oems[oem]['name']}_silver",
    table_properties = {"overwriteSchema": "true", "delta.feature.variantType-preview": "supported"}
  )

  def create_table():
    return (
    dlt.readStream(f"{oems[oem]['name']}_raw")
    .selectExpr(*oems[oem]['core_col_list'], *col_list)
    )


# COMMAND ----------

def create_dashboard_lights_table(oem):
  col_list = [f'{value} as {key}' for key, value in oems[oem]['dashboard_lights_schema'].items()]   

  @dlt.create_table(
    name = f"{oems[oem]['name']}_dashboard_lights",
    table_properties = {"overwriteSchema": "true", "delta.feature.variantType-preview": "supported"}
  )

  def create_table():
    return (
    dlt.readStream(f"{oems[oem]['name']}_silver")
    .withColumn("exploded_dashboard_lights",
                F.explode(
                  F.from_json(
                    F.col(oems[oem]['dashboard_lights_schema']['exploded_dashboard_lights']).cast("string"),
                    oems[oem]['dashboard_light_explode_schema']
                    )
                  )
                )
    .selectExpr(*oems[oem]['core_col_list'],
                *col_list
                )
    .drop("exploded_dashboard_lights")
    )

# COMMAND ----------

def create_metrics_table(oem, metric):
  col_list = [f'{value} as {key}' for key, value in oems[oem]['metric_schema'][metric].items()]  
  @dlt.view(
  name = f"{oems[oem]['name']}_{metric}",
  
)

  def create_table(metric = metric):
    df = dlt.readStream(f"{oems[oem]['name']}_silver")\
      .selectExpr(*oems[oem]['core_col_list'],
                  f"cast('{metric}' as string) as metric_name",
                  *col_list)
    
    for col, data_type in [('metric_value','double'),('metric_unit','string'),('metric_timestamp','timestamp')]:
      if col in df.columns:
        df = df.withColumn(f"{col}_format", F.col(col).cast(data_type))\
               .drop(col)\
               .withColumnRenamed(f"{col}_format", col)
    
    return (
      df
    )

# COMMAND ----------

def create_metrics_array_table(oem, metric_array):
  col_list = [f'{value} as {key}' for key, value in oems[oem]['metric_array_schema'][metric_array].items() if key not in ['exploded_metric','exploded_metric_schema']]   

  @dlt.view(
  name = f"{oems[oem]['name']}_{metric_array}",
)

  def create_table():
    return (
    dlt.readStream(f"{oems[oem]['name']}_silver")
    .withColumn("exploded_metric",
                F.explode(
                  F.from_json(
                    F.col(oems[oem]['metric_array_schema'][metric_array]['exploded_metric']).cast("string"),
                    oems[oem]['metric_array_schema'][metric_array]['exploded_metric_schema']
                    )
                  )
                )
    .selectExpr(*oems[oem]['core_col_list'],
                f"cast('{metric_array}' as string) as metric_name",
                *col_list
                )
    .drop("exploded_metric","exploded_metric_array")
    )

# COMMAND ----------

def tire_pressure_joined(oem):
  @dlt.table(
    name = f"{oems[oem]['name']}_tire_pressure_joined",
  )
  def create_table():
    tire_pressure = dlt.readStream(f"{oems[oem]['name']}_tire_pressures").withWatermark("ingestion_timestamp", "30 seconds")
    tire_pressure_diff = dlt.readStream(f"{oems[oem]['name']}_tire_pressures_differences").withWatermark("ingestion_timestamp", "30 seconds")
    tire_pressure_target = dlt.readStream(f"{oems[oem]['name']}_tire_pressure_targets").withWatermark("ingestion_timestamp", "30 seconds")

    return( tire_pressure.alias("tp")\
    .join(
      tire_pressure_diff.alias("tpd"), 
      on = ["oem","vin","ingestion_timestamp","metric_value_detail"], 
      how = "left")\
    .selectExpr("tp.*",
                "tpd.metric_value as metric_value_diff",
                "tpd.metric_unit as metric_unit_diff", 
                "tpd.metric_timestamp as metric_timestamp_diff")\
    .alias("tpd")\
    .join(
      tire_pressure_target.alias("tpt"), 
      on = ["oem","vin","ingestion_timestamp","metric_value_detail"], 
      how = "left")\
      .selectExpr("tpd.*",
              "tpt.metric_value as metric_value_target", 
              "tpt.metric_unit as metric_unit_target", 
              "tpt.metric_timestamp as metric_timestamp_target")
    )


# COMMAND ----------

def union_tables(oem, target_table_name, table_list):
  @dlt.table(
    name = f"{oems[oem]['name']}_{target_table_name}",
  )

  def create_table():
    union_table = None
    for table in table_list:
      current_table = dlt.readStream(f"{oems[oem]['name']}_{table}")
      if union_table is None:
        union_table = current_table
      else:
        union_table = union_table.unionByName(current_table, allowMissingColumns=True)

    return union_table

# COMMAND ----------

def union_oem_metric_tables(table_list):
  @dlt.table(
    name = f"oem_metrics_union",
  )

  def create_table():
    union_table = None
    for table in table_list:
      current_table = dlt.readStream(f"{table}")
      if union_table is None:
        union_table = current_table
      else:
        union_table = union_table.unionByName(current_table, allowMissingColumns=True)

    return union_table

# COMMAND ----------

def standardise_metrics(length_standard_unit, pressure_standard_unit): 
  @dlt.table(
    name = f"oem_metrics_standardised",
  )

  def create_table():
    return(
    dlt.readStream("oem_metrics_union")\
       .withColumn("standard_metric_units", 
                    F.when(
                      F.col("metric_name").isin("odometer","altitude"),length_standard_unit)
                    .when(
                      F.col("metric_name").isin("tire_pressures"),pressure_standard_unit)
                    .otherwise(F.lit(None))
                    )\
       .withColumn("standard_metric_value",F.expr(
        'beatrice_liew.connected_vehicles.unit_conversion(metric_value,metric_unit, standard_metric_units)'))
       .withColumn("standard_metric_value_diff",F.expr(
        'beatrice_liew.connected_vehicles.unit_conversion(metric_value_diff,metric_unit_diff, standard_metric_units)'))
       .withColumn("standard_metric_value_target",F.expr(
        'beatrice_liew.connected_vehicles.unit_conversion(metric_value_target,metric_unit_target, standard_metric_units)'))
       .select(
         "oem",
         "vin",
         "ingestion_timestamp",
         "metric_name",
         "metric_value_detail",
         "metric_timestamp",
         "standard_metric_value",
         "standard_metric_value_target",
         "standard_metric_units")
    )

# COMMAND ----------

oem_list = ['ford','porsche']
metrics_table_union_list = []
pressure_standard_unit = "bars"
length_standard_unit = "meters"

for oem in oem_list:
  create_raw_table(oem)
  create_silver_table(oem)
  create_dashboard_lights_table(oem)
  
  metric_list = [f'{key}' for key in oems[oem]['metric_schema'].keys()]
  for metric in metric_list: 
    create_metrics_table(oem, metric)

  metric_array_list = [f'{key}' for key in oems[oem]['metric_array_schema'].keys()]
  for metric in metric_array_list: 
    create_metrics_array_table(oem, metric)

  if any('target' in metric for metric in metric_array_list):
    tire_pressure_joined(oem)
    metric_list.append("tire_pressure_joined")
    union_tables(oem, "metrics_union", metric_list)

  else:
    table_list = metric_list + metric_array_list
    union_tables(oem, "metrics_union",table_list)

  metrics_table_union_list.append(f"{oems[oem]['name']}_metrics_union")

union_oem_metric_tables(metrics_table_union_list) 
standardise_metrics(length_standard_unit, pressure_standard_unit)
