from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def update_salary_only(spark, temp_bucket):
    # Read existing employee data (all 100 columns)
    existing_df = (spark.read.format("bigquery")
                  .option("table", "p1.d1.emp_1")
                  .load())
    
    # Read only emp_id and salary from update table
    update_df = (spark.read.format("bigquery")
                .option("table", "p2.d2.updated_emp2")
                .select("emp_id", "salary")
                .load())
    
    # Keep all columns from existing table and update only salary
    updated_df = (existing_df.alias("existing")
                 .join(update_df.alias("updates"), 
                      on="emp_id",
                      how="left")
                 .select(
                     *[F.col("existing." + col) for col in existing_df.columns if col != "salary"],
                     F.coalesce(F.col("updates.salary"), 
                               F.col("existing.salary")).alias("salary")
                 ))
    
    # Write back to original table
    (updated_df.write.format("bigquery")
     .option("table", "p1.d1.emp_1")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

def main():
    spark = (SparkSession.builder
            .appName("Salary-Update")
            .config("spark.jars.packages", 
                   "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())
    
    update_salary_only(spark, "your-temp-bucket")
    spark.stop()

if __name__ == "__main__":
    main()
