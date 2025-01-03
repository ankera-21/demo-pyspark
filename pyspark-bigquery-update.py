from pyspark.sql import SparkSession
from google.cloud import storage
import tempfile

def create_spark_session(app_name="BigQuery-Update-Job"):
    """Create a Spark session with BigQuery connector configurations"""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .config("viewsEnabled", "true")
            .config("materializationDataset", "temp_dataset")
            .getOrCreate())

def update_bigquery_table(
    spark,
    project_id,
    dataset_id,
    table_id,
    temp_bucket,
    update_dataframe,
    join_columns
):
    """
    Update BigQuery table with data from PySpark DataFrame
    
    Args:
        spark: SparkSession object
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        temp_bucket: GCS bucket for temporary storage
        update_dataframe: PySpark DataFrame containing update data
        join_columns: List of columns to join on for the update
    """
    
    # Read existing data from BigQuery
    existing_data = (spark.read.format("bigquery")
                    .option("table", f"{project_id}.{dataset_id}.{table_id}")
                    .load())
    
    # Register both DataFrames as temporary views
    existing_data.createOrReplaceTempView("existing_data")
    update_dataframe.createOrReplaceTempView("update_data")
    
    # Create join condition string
    join_condition = " AND ".join([f"existing_data.{col} = update_data.{col}" 
                                 for col in join_columns])
    
    # Get all columns from the table
    all_columns = existing_data.columns
    update_columns = [col for col in all_columns if col not in join_columns]
    
    # Create update set clause
    set_clause = ", ".join([f"update_data.{col}" for col in update_columns])
    
    # Construct MERGE equivalent using LEFT OUTER JOIN
    merge_query = f"""
    SELECT 
        CASE 
            WHEN update_data.{join_columns[0]} IS NOT NULL THEN {set_clause}
            ELSE {', '.join([f'existing_data.{col}' for col in update_columns])}
        END
    FROM existing_data
    LEFT OUTER JOIN update_data
    ON {join_condition}
    """
    
    # Execute the merge
    merged_data = spark.sql(merge_query)
    
    # Write the merged data back to BigQuery
    (merged_data.write.format("bigquery")
     .option("table", f"{project_id}.{dataset_id}.{table_id}")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

def main():
    # Initialize Spark session
    spark = create_spark_session()
    
    # Example: Create your update DataFrame
    # Replace this with your actual data processing logic
    update_data = spark.createDataFrame([
        # Your update data here
    ])
    
    # Configuration
    config = {
        "project_id": "your-project-id",
        "dataset_id": "your_dataset",
        "table_id": "your_table",
        "temp_bucket": "your-temp-bucket",
        "join_columns": ["id"]  # Replace with your actual join columns
    }
    
    # Update BigQuery table
    update_bigquery_table(
        spark=spark,
        update_dataframe=update_data,
        **config
    )
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    main()
