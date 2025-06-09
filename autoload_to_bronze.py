# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import yaml
import os

# New helper function to check and enable CDF
def ensure_cdf_enabled(spark_session, table_name):
    """
    Checks if CDF is enabled for a given Delta table and enables it if not.
    Returns True if CDF is enabled (or was just enabled), False otherwise (e.g., table not found).
    """
    try:
        # Check if table exists first, otherwise describe detail will fail
        if not spark_session.catalog.tableExists(table_name):
            print(f"INFO: Table '{table_name}' does not exist yet. CDF will be enabled upon creation by Auto Loader.")
            return True # Assume it will be enabled during creation
        
        # Get table properties
        df_properties = spark_session.sql(f"DESCRIBE DETAIL {table_name}").select("properties").collect()
        
        properties = df_properties[0]["properties"]
        cdf_enabled = properties.get("delta.enableChangeDataFeed", "false").lower() == "true"

        if not cdf_enabled:
            print(f"CDF is NOT enabled for table '{table_name}'. Enabling it now...")
            spark_session.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            print(f"CDF enabled for table '{table_name}'.")
            return True
        else:
            print(f"CDF is already enabled for table '{table_name}'.")
            return True
    except Exception as e:
        print(f"ERROR checking/enabling CDF for table '{table_name}': {e}")
        return False


# Function to encapsulate the Auto Loader logic
def run_autoloader_job():
    """
    Initializes Spark, configures Auto Loader streams for multiple datasets,
    and writes data to Unity Catalog Delta tables using configurations from a file.
    """
    print("Starting Auto Loader job execution...")

    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()
    config_file_path = "/Workspace/Shared/test1/config.yaml" 

    try:
        # Use standard Python file reading
        with open(config_file_path, 'r') as f:
            config_data = yaml.safe_load(f)
        print(f"Successfully loaded configuration from {config_file_path}")
    except Exception as e:
        print(f"ERROR: Could not load configuration file from {config_file_path}. Please check path and permissions.")
        print(f"Error details: {e}")
        sys.exit(1) # Exit the job if configuration fails

    # Extract values from the loaded configuration
    job_settings = config_data.get("job_settings", {})
    base_raw_source_path = job_settings.get("base_raw_source_path")
    catalog_name = job_settings.get("catalog_name")
    schema_name = job_settings.get("schema_name")
    autoloader_base_checkpoint_path = job_settings.get("autoloader_base_checkpoint_path")

    # The datasets configuration is a list
    datasets_config = config_data.get("datasets", [])

    # Basic validation for critical paths
    if not all([base_raw_source_path, catalog_name, schema_name, autoloader_base_checkpoint_path]):
        print("ERROR: One or more critical job settings are missing from the config file.")
        sys.exit(1)
    if not datasets_config:
        print("WARNING: No datasets configured in the 'datasets' section of the config file. No data will be processed.")

    # Create the target schema if it doesn't exist
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
        print(f"Schema '{catalog_name}.{schema_name}' ensured to exist.")
    except Exception as e:
        print(f"Error ensuring schema '{catalog_name}.{schema_name}': {e}")
        # Consider logging the error but allowing the job to continue if schema creation isn't critical
        # e.g., if you know it always exists, or individual table creation will fail later anyway.

    print("\nConfigured datasets and their target tables:")
    for ds in datasets_config:
        print(f"  - Subdirectory: {ds.get('subdir')} -> Target Table: {ds.get('target_table')}")
    print("-" * 50)

    # --- Loop through configured datasets and start a separate Auto Loader stream for each ---
    for dataset_info in datasets_config:
        dataset_subdir = dataset_info.get("subdir")
        target_delta_table_name = dataset_info.get("target_table")

        if not dataset_subdir or not target_delta_table_name:
            print(f"WARNING: Skipping malformed dataset entry: {dataset_info}. Missing 'subdir' or 'target_table'.")
            continue

        raw_source_path_for_this_dataset = f"{base_raw_source_path}{dataset_subdir}/"
        checkpoint_path_for_this_stream = f"{autoloader_base_checkpoint_path}{dataset_subdir}/"
        full_uc_table_name = f"{catalog_name}.{schema_name}.{target_delta_table_name}"

        print(f"\nConfiguring Auto Loader for dataset directory: {dataset_subdir}")
        print(f"  Source Path: {raw_source_path_for_this_dataset}")
        print(f"  Checkpoint Path: {checkpoint_path_for_this_stream}")
        print(f"  Target Table: {full_uc_table_name}")

        try:
            # --- NEW STEP: Ensure CDF is enabled on the Bronze table BEFORE streaming ---
            # This handles cases where the table might already exist but CDF wasn't on.
            # If the table doesn't exist, Auto Loader will create it, and the .option below will apply.
            if not ensure_cdf_enabled(spark, full_uc_table_name):
                print(f"WARNING: Could not ensure CDF is enabled for '{full_uc_table_name}'. It might affect downstream consumers.")
                # Decide if you want to fail here or continue without CDF guarantee
                # For critical CDF usage, you might want to `continue` to next dataset or `raise` an error.
                # For now, we'll proceed but be aware.

            (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", checkpoint_path_for_this_stream)
                .option("cloudFiles.includeExistingFiles", "true")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("recursiveFileLookup", "true")
                # This option ensures CDF is enabled if the table is newly created by this stream.
                # It also ensures subsequent writes adhere to it.
                .option("delta.enableChangeDataFeed", "true") 
                .option("cloudFiles.useNotifications", "false")
                .load(raw_source_path_for_this_dataset)
                .writeStream
                .option("checkpointLocation", checkpoint_path_for_this_stream)
                .trigger(availableNow=True)
                .toTable(full_uc_table_name)
                .awaitTermination())

            print(f"Auto Loader stream for {dataset_subdir} completed, data loaded into {full_uc_table_name}")
        except Exception as e:
            print(f"Error processing dataset {dataset_subdir}: {e}")
            # In a job, you might want to log this error and continue with other datasets,
            # or re-raise if failure of one dataset means the entire job should fail.

    print("\nAll Auto Loader streams configured and completed.")
    print("Please monitor the job status in the Databricks Jobs UI.")
    print("Job execution finished.")

if __name__ == "__main__":
    run_autoloader_job()

# COMMAND ----------

