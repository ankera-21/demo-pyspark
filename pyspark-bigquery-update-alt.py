from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage
import logging

def update_bigquery_with_dataframe(
    spark,
    update_df,
    project_id,
    dataset_id,
    table_id,
    temp_bucket,
    join_key
):
    try:
        # Configure BigQuery options
        bigquery_options = {
            "table": f"{project_id}.{dataset_id}.{table_id}",
            "temporaryGcsBucket": temp_bucket
        }
        
        # Read existing data
        existing_df = (spark.read
                      .format("bigquery")
                      .options(**bigquery_options)
                      .load())
        
        # Perform the update using DataFrame operations
        # First, identify columns to update (all except join key)
        update_columns = [c for c in update_df.columns if c != join_key]
        
        # Create column mapping for coalesce
        coalesce_expr = {
            col: F.coalesce(F.col(f"updates.{col}"), F.col(f"existing.{col}"))
            for col in update_columns
        }
        
        # Add the join key to the output columns
        coalesce_expr[join_key] = F.col(f"existing.{join_key}")
        
        # Perform left join and update
        updated_df = (existing_df.alias("existing")
                     .join(update_df.alias("updates"), 
                           on=join_key, 
                           how="left")
                     .select(*[expr.alias(col) 
                             for col, expr in coalesce_expr.items()]))
        
        # Write back to BigQuery
        (updated_df.write
         .format("bigquery")
         .options(**bigquery_options)
         .mode("overwrite")
         .save())
        
        logging.info(f"Successfully updated table {table_id}")
        return True
        
    except Exception as e:
        logging.error(f"Error updating BigQuery table: {str(e)}")
        raise

def main():
    # Initialize Spark
    spark = (SparkSession.builder
            .appName("BigQuery-Update-Alt")
            .config("spark.jars.packages", 
                   "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())
    
    # Example update data
    update_data = [
        ("1001", "John Doe", 75000),
        ("1002", "Jane Smith", 82000)
    ]
    columns = ["emp_id", "name", "salary"]
    
    update_df = spark.createDataFrame(update_data, columns)
    
    # Configuration
    config = {
        "project_id": "your-project-id",
        "dataset_id": "hr_dataset",
        "table_id": "employees",
        "temp_bucket": "your-temp-bucket",
        "join_key": "emp_id"
    }
    
    # Perform update
    update_bigquery_with_dataframe(
        spark=spark,
        update_df=update_df,
        **config
    )
    
    spark.stop()

if __name__ == "__main__":
    main()
