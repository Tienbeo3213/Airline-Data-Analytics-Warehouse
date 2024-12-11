from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("AirportsDataProcessing") \
    .enableHiveSupport() \
    .getOrCreate()

# Đọc dữ liệu từ Hive table
airports_df = spark.sql("SELECT * FROM airline_db_raw.airports")

# Biến đổi dữ liệu
data_transformed = airports_df.withColumn("load_dt", current_date().cast("string")) \
                              .withColumn("load_dtm", current_timestamp().cast("string"))

# Lưu dữ liệu vào vị trí cụ thể trên HDFS
data_transformed.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/airline_data/cur/airports")

# Dừng SparkSession
spark.stop()
