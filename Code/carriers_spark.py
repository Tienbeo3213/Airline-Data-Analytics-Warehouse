from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform Carriers Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Tải dữ liệu từ bảng Hive
carriers_df = spark.sql("SELECT * FROM airline_db_raw.carriers")

# Biến đổi dữ liệu
data_transformed = carriers_df.withColumn("load_dt", current_date().cast("string")) \
                              .withColumn("load_dtm", current_timestamp().cast("string"))

# Lưu dữ liệu vào vị trí cụ thể trên HDFS
data_transformed.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/airline_data/cur/carriers")

# Dừng SparkSession
spark.stop()
