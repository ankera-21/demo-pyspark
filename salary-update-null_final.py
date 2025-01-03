from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def update_null_salaries(spark, temp_bucket):
    existing_df = (spark.read.format("bigquery")
                  .option("table", "p1.d1.emp_1")
                  .load())
    
    update_df = (spark.read.format("bigquery")
                .option("table", "p2.d2.updated_emp2")
                .select("emp_id", "salary")
                .load())
    
    # Update salary only if existing salary is null
    updated_df = (existing_df.alias("existing")
                 .join(update_df.alias("updates"), 
                      on="emp_id",
                      how="left")
                 .select(
                     *[F.col("existing." + col) for col in existing_df.columns if col != "salary"],
                     F.when(F.col("existing.salary").isNull(), F.col("updates.salary"))
                      .otherwise(F.col("existing.salary"))
                      .alias("salary")
                 ))
    
    (updated_df.write.format("bigquery")
     .option("table", "p1.d1.emp_1")
     .option("temporaryGcsBucket", temp_bucket)
     .mode("overwrite")
     .save())

if __name__ == "__main__":
    spark = (SparkSession.builder
            .appName("Update-Null-Salaries")
            .config("spark.jars.packages", 
                   "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            .getOrCreate())
    
    update_null_salaries(spark, "your-temp-bucket")
    spark.stop()
