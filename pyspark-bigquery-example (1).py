from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark_session():
    return (SparkSession.builder
            .appName("Employee-Salary-Update")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())

def update_employee_salaries(spark, project_id, dataset_id, temp_bucket):
    # Create sample employee data for update
    schema = StructType([
        StructField("emp_id", StringType(), False),
        StructField("salary", IntegerType(), False)
    ])
    
    # Sample salary updates
    update_data = spark.createDataFrame([
        ("E001", 75000),  # Salary increase
        ("E003", 85000)   # Salary increase
    ], schema=schema)
    
    # Read existing employee data from BigQuery
    table_id = "employees"
    existing_data = (spark.read.format("bigquery")
                    .option("table", f"{project_id}.{dataset_id}.{table_id}")
                    .load())
    
    # Register DataFrames as views
    existing_data.createOrReplaceTempView("existing_employees")
    update_data.createOrReplaceTempView("salary_updates")
    
    # Merge query
    merge_query = """
    SELECT 
        e.emp_id,
        e.name,
        CASE 
            WHEN u.salary IS NOT NULL THEN u.salary
            ELSE e.salary
        END as salary,
        e.department
    FROM existing_employees e
    LEFT OUTER JOIN salary_updates u
    ON e.emp_id = u.emp_id
    """
    
    merged_data = spark.sql(merge_query)
    
    # Write back to BigQuery
    (merged_data.write.format("bigquery")
     .option("table", f"{project_id}.{dataset_id}.{table_id}")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

def main():
    # Configuration
    config = {
        "project_id": "your-project-id",
        "dataset_id": "hr_dataset",
        "temp_bucket": "your-temp-bucket"
    }
    
    spark = create_spark_session()
    update_employee_salaries(spark, **config)
    spark.stop()

if __name__ == "__main__":
    main()
