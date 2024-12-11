from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform Plane Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Tải dữ liệu từ bảng Hive
plane_data_df = spark.sql("SELECT * FROM airline_db_raw.plane_data")

# Biến đổi dữ liệu
data_transformed = plane_data_df.withColumn("load_dt", current_date().cast("string")) \
                                .withColumn("load_dtm", current_timestamp().cast("string"))

# Chọn các cột cần thiết và thêm các cột mới
selected_columns = [
    "tailnum", "type", "manufacturer", "issue_date", "model", "status", 
    "aircraft_type", "engine_type", "year", "load_dt", "load_dtm"
]

# Lưu dữ liệu vào vị trí cụ thể trên HDFS
data_transformed.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/airline_data/cur/plane_data")

# Dừng SparkSession
spark.stop()
