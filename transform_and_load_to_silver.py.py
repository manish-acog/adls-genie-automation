# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
import yaml
import os

# COMMAND ----------
def get_spark_type(field_type_str):
    import re
    type_mapping = {
        "string": StringType(),
        "date": DateType(),
        "integer": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
    }

    lower_type_str = field_type_str.lower()

    if lower_type_str.startswith("decimal"):
        match = re.match(r"decimal\((\d+),(\d+)\)", lower_type_str)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return DecimalType(precision, scale)
        else:
            return DecimalType(10, 2) 

    return type_mapping.get(lower_type_str, StringType())

# COMMAND ----------
def apply_silver_transformations(bronze_df, silver_schema_fields):
    select_expressions = []
    bronze_columns = bronze_df.columns
    
    for field_def in silver_schema_fields:
        col_name = field_def["name"]
        target_type_str = field_def["type"]
        date_formats = field_def.get("date_formats", [])
        
        if col_name not in bronze_columns:
            print(f"WARNING: Column '{col_name}' (type: {target_type_str}) defined in Silver schema not found in Bronze data. It will be added as NULL.")
            select_expressions.append(F.lit(None).cast(get_spark_type(target_type_str)).alias(col_name))
            continue
        
        current_col_expr = F.col(col_name)
        target_spark_type = get_spark_type(target_type_str)
        
        potential_cast_exprs = [
            current_col_expr.try_cast(target_spark_type)
        ]
        
        if isinstance(target_spark_type, (DateType, TimestampType)):
            for fmt in date_formats:
                if isinstance(target_spark_type, DateType):
                    potential_cast_exprs.append(F.to_date(current_col_expr, fmt))
                elif isinstance(target_spark_type, TimestampType):
                    potential_cast_exprs.append(F.to_timestamp(current_col_expr, fmt))
        
        final_col_expr = F.coalesce(*potential_cast_exprs)
        select_expressions.append(final_col_expr.cast(target_spark_type).alias(col_name))
    
    return bronze_df.select(*select_expressions)

# COMMAND ----------
def read_last_processed_version(checkpoint_path):
    try:
        version_file_path = f"{checkpoint_path}last_bronze_version.txt"
        content = dbutils.fs.head(version_file_path)
        return int(content.strip())
    except Exception:
        return None

def write_last_processed_version(checkpoint_path, version):
    version_file_path = f"{checkpoint_path}last_bronze_version.txt"
    dbutils.fs.mkdirs(checkpoint_path)
    dbutils.fs.put(version_file_path, str(version), overwrite=True)

# COMMAND ----------
config_file_path = "config.yaml"
schema_defs_file_path = "schema_definitions.json"
silver_base_checkpoint_path = "abfss://staging@bmrnpocstorage.dfs.core.windows.net/_checkpoints/silver_processing/"

try:
    with open(config_file_path, "r") as f:
        job_config = yaml.safe_load(f)
    print(f"Successfully loaded job configuration from {config_file_path}")

    with open(schema_defs_file_path, "r") as f:
        schema_definitions = json.load(f)["schemas"] 
    print(f"Successfully loaded schema definitions from {schema_defs_file_path}")

except Exception as e:
    print(f"ERROR: Could not load configuration or schema files. Please check paths and permissions.")
    print(f"Error details: {e}")
    raise e

catalog_name = job_config["job_settings"]["catalog_name"]
bronze_schema_name = job_config["job_settings"]["schema_name"]
silver_schema_name = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema_name}")
print(f"Silver Schema '{catalog_name}.{silver_schema_name}' ensured to exist.")

# COMMAND ----------
for dataset_info in job_config["datasets"]:
    bronze_source_subdir = dataset_info["subdir"]
    bronze_table_name_short = dataset_info["target_table"]

    full_bronze_table_name = f"{catalog_name}.{bronze_schema_name}.{bronze_table_name_short}"
    silver_target_table_short = f"{bronze_source_subdir}_silver" 
    full_silver_table_name = f"{catalog_name}.{silver_schema_name}.{silver_target_table_short}"
    
    silver_checkpoint_path_for_dataset = f"{silver_base_checkpoint_path}{bronze_source_subdir}/"

    print(f"\n--- Processing Dataset: {bronze_source_subdir} ---")
    print(f"Reading from Bronze table: {full_bronze_table_name}")
    print(f"Target Silver table: {full_silver_table_name}")
    print(f"Checkpoint Path: {silver_checkpoint_path_for_dataset}")

    dataset_silver_schema_fields = schema_definitions.get(bronze_source_subdir, {}).get("fields")

    if not dataset_silver_schema_fields:
        print(f"WARNING: No schema definition 'fields' found for dataset '{bronze_source_subdir}'. Skipping this dataset.")
        continue

    unique_key_columns = [
        field["name"] for field in dataset_silver_schema_fields if field.get("is_primary_key")
    ]

    if not unique_key_columns:
        print(f"ERROR: No primary key defined in schema for dataset '{bronze_source_subdir}'. MERGE INTO cannot proceed. Skipping this dataset.")
        continue

    print(f"Unique Key(s) for Merge: {', '.join(unique_key_columns)}")

    try:
        if not spark.catalog.tableExists(full_bronze_table_name):
            print(f"Bronze table '{full_bronze_table_name}' does not exist. Skipping this dataset.")
            continue

        last_processed_version = read_last_processed_version(silver_checkpoint_path_for_dataset)
        
        if last_processed_version is None:
            print("First run detected. Reading all data from Bronze table for initial Silver load.")
            bronze_df = spark.read.table(full_bronze_table_name)
            current_bronze_version = spark.sql(f"DESCRIBE HISTORY {full_bronze_table_name} LIMIT 1").collect()[0]["version"]
        else:
            start_version = last_processed_version + 1
            print(f"Incremental run. Reading changes from Bronze version {start_version} onwards.")
            
            current_bronze_version = spark.sql(f"DESCRIBE HISTORY {full_bronze_table_name} LIMIT 1").collect()[0]["version"]
            
            if current_bronze_version < start_version:
                print(f"No new changes in Bronze table. Current version: {current_bronze_version}, Last processed: {last_processed_version}")
                continue
            
            bronze_df = spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", start_version) \
                .table(full_bronze_table_name)
            
            bronze_df = bronze_df.filter(F.col("_change_type").isin(["insert", "update"]))
            
            data_columns = [col for col in bronze_df.columns if not col.startswith("_")]
            bronze_df = bronze_df.select(*data_columns)

        if bronze_df.isEmpty():
            print("No data to process. Skipping transformation and merge.")
            continue

        print("Applying transformations to Bronze data...")
        
        print("\n--- Schema Transformation Summary ---")
        print(f"{'Column Name':<20} {'Bronze Type':<15} {'Silver Type':<15}")
        print("-" * 50)
        
        bronze_schema_dict = {field.name: str(field.dataType) for field in bronze_df.schema.fields}
        
        for field_def in dataset_silver_schema_fields:
            col_name = field_def["name"]
            target_type_str = field_def["type"]
            bronze_type = bronze_schema_dict.get(col_name, "NOT_FOUND")
            print(f"{col_name:<20} {bronze_type:<15} {target_type_str:<15}")
        
        silver_transformed_df = apply_silver_transformations(bronze_df, dataset_silver_schema_fields)
        
        print("\nTransformed Silver DataFrame Schema:")
        silver_transformed_df.printSchema()
        print("Sample transformed data:")
        silver_transformed_df.show(5, truncate=False)

        silver_table_exists = spark.catalog.tableExists(full_silver_table_name)
        
        if not silver_table_exists:
            print(f"Creating Silver table {full_silver_table_name} with initial data.")
            silver_transformed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(full_silver_table_name)
            print(f"Silver table created: {full_silver_table_name}")
        else:
            print(f"Performing MERGE INTO Silver table: {full_silver_table_name}")
            
            silver_delta_table = DeltaTable.forName(spark, full_silver_table_name)
            
            merge_condition = " AND ".join([
                f"target.{col} = source.{col}" for col in unique_key_columns
            ])
            
            silver_data_columns = [field["name"] for field in dataset_silver_schema_fields]
            
            update_expressions = {col: f"source.{col}" for col in silver_data_columns}
            insert_expressions = {col: f"source.{col}" for col in silver_data_columns}
            
            silver_delta_table.alias("target") \
                .merge(
                    silver_transformed_df.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdate(set=update_expressions) \
                .whenNotMatchedInsert(values=insert_expressions) \
                .execute()
            
            print(f"MERGE INTO completed for Silver table: {full_silver_table_name}")

        write_last_processed_version(silver_checkpoint_path_for_dataset, current_bronze_version)
        print(f"Updated checkpoint with Bronze version: {current_bronze_version}")

        print(f"\n--- Verification for {bronze_source_subdir} ---")
        record_count = spark.read.table(full_silver_table_name).count()
        print(f"Silver table '{full_silver_table_name}' now contains {record_count} records.")
        print("Sample data from Silver table:")
        spark.read.table(full_silver_table_name).show(5, truncate=False)

    except Exception as e:
        print(f"\nERROR processing dataset {bronze_source_subdir}: {e}")
        import traceback
        traceback.print_exc()
        continue

print("\nAll datasets processed for Silver layer updates.")
print("Job execution completed.")