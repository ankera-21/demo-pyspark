from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    return (SparkSession.builder
            .appName("Employee-Salary-Update")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())

def update_employee_salaries(spark, project_id, dataset_id, temp_bucket):
    # Create sample existing employee data (simulating BigQuery table)
    existing_schema = StructType([
        StructField("emp_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("salary", IntegerType(), False),
        StructField("department", StringType(), False)
    ])
    
    existing_data = [
        ("E001", "John", 70000, "Sales"),
        ("E002", "Alice", 65000, "Marketing"),
        ("E003", "Bob", 80000, "Engineering")
    ]
    
    existing_df = spark.createDataFrame(existing_data, existing_schema)
    
    # Create sample salary updates
    update_schema = StructType([
        StructField("emp_id", StringType(), False),
        StructField("salary", IntegerType(), False)
    ])
    
    update_data = [
        ("E001", 75000),  # John's salary increase
        ("E003", 85000)   # Bob's salary increase
    ]
    
    update_df = spark.createDataFrame(update_data, update_schema)
    
    # Perform the update using DataFrame operations
    updated_df = (existing_df.alias("existing")
                 .join(update_df.alias("updates"), 
                       on="emp_id", 
                       how="left")
                 .select(
                     F.col("existing.emp_id"),
                     F.col("existing.name"),
                     F.coalesce(F.col("updates.salary"), 
                               F.col("existing.salary")).alias("salary"),
                     F.col("existing.department")
                 ))
    
    # Write back to BigQuery
    (updated_df.write.format("bigquery")
     .option("table", f"{project_id}.{dataset_id}.employees")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

    # Show results (for demonstration)
    print("Updated Employee Data:")
    updated_df.show()

def main():
    spark = create_spark_session()
    
    config = {
        "project_id": "your-project-id",
        "dataset_id": "hr_dataset",
        "temp_bucket": "your-temp-bucket"
    }
    
    update_employee_salaries(spark, **config)
    spark.stop()

if __name__ == "__main__":
    main()
