from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def update_employee_data(spark, existing_table, update_table, temp_bucket):
    # Read existing employee data
    existing_df = (spark.read.format("bigquery")
                  .option("table", "p1.d1.emp_1")
                  .load())
    
    # Read update data
    update_df = (spark.read.format("bigquery")
                .option("table", "p2.d2.updated_emp2")
                .load())
    
    # Perform update using DataFrame operations
    updated_df = (existing_df.alias("existing")
                 .join(update_df.alias("updates"), 
                      on="emp_id",  # Assuming emp_id is the join key
                      how="left")
                 .select(
                     F.col("existing.emp_id"),
                     # Add coalesce for each column you want to update
                     F.coalesce(F.col("updates.name"), 
                               F.col("existing.name")).alias("name"),
                     F.coalesce(F.col("updates.salary"), 
                               F.col("existing.salary")).alias("salary"),
                     F.coalesce(F.col("updates.department"), 
                               F.col("existing.department")).alias("department")
                 ))
    
    # Write back to original table
    (updated_df.write.format("bigquery")
     .option("table", "p1.d1.emp_1")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

def main():
    spark = (SparkSession.builder
            .appName("BigQuery-Table-Update")
            .config("spark.jars.packages", 
                   "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())
    
    update_employee_data(
        spark=spark,
        existing_table="p1.d1.emp_1",
        update_table="p2.d2.updated_emp2",
        temp_bucket="your-temp-bucket"
    )
    
    spark.stop()

if __name__ == "__main__":
    main()
