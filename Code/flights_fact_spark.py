from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as max_, sum as sum_, current_date, current_timestamp, when

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform Flight Data") \
    .enableHiveSupport() \
    .getOrCreate()

# Tải dữ liệu từ các bảng Hive
plane_data_df = spark.sql("SELECT * FROM airline_db_cur.plane_data")
carriers_data_df = spark.sql("SELECT * FROM airline_db_cur.carriers")
airport_data_df = spark.sql("SELECT * FROM airline_db_cur.airports")
detailed_data_df = spark.sql("SELECT * FROM airline_db_cur.detailed_data")

# Biến đổi dữ liệu
plane_data_manu = plane_data_df.select("tailnum", "manufacturer")
carriers_man = carriers_data_df.select("code", "description")

derived_data_transformed = detailed_data_df.select(
    "year", "month", "flightnum", "tailnum", "uniquecarrier",
    col("arrdelay").cast("int"), col("depdelay").cast("int"),
    col("distance").cast("int"), col("cancelled").cast("int"),
    col("diverted").cast("int")
)

# Nhóm dữ liệu theo các cột cần thiết và tính các thống kê
derived_grouped_data = derived_data_transformed.groupBy("year", "month", "flightnum", "tailnum", "uniquecarrier").agg(
    avg("arrdelay").alias("avg_arr_delay"),
    avg("depdelay").alias("avg_dep_delay"),
    avg("distance").alias("avg_distance"),
    max_("distance").alias("max_distance"),
    sum_("cancelled").alias("tot_cancelled_flights"),
    sum_("diverted").alias("tot_diverted_flights")
)

# Thực hiện join với bảng carriers_data_df để lấy tên hãng hàng không
derived_avg_arr_flight = derived_grouped_data.join(carriers_data_df, derived_grouped_data.uniquecarrier == carriers_data_df.code)

# Chọn các cột cần thiết và đổi tên
result_crr = derived_avg_arr_flight.select(
    "year", "month", "flightnum", "tailnum", "uniquecarrier",
    "avg_arr_delay", "avg_dep_delay", "avg_distance", "max_distance",
    "tot_cancelled_flights", "tot_diverted_flights",
    col("description").alias("airline_name")
)

# Thực hiện join với bảng plane_data_manu để lấy thông tin nhà sản xuất
result_crr_man_join = result_crr.join(plane_data_manu, "tailnum")

# Chọn các cột cần thiết, tính toán cột mới và thêm cột ngày hiện tại và thời gian hiện tại
final = result_crr_man_join.select(
    "month", "flightnum", "tailnum", "uniquecarrier",
    "avg_arr_delay", "avg_dep_delay", "avg_distance", "max_distance",
    "tot_cancelled_flights", "tot_diverted_flights", "airline_name", "manufacturer",
    when(col("avg_arr_delay") < 0, "Punctual").otherwise("Not Punctual").alias("punctuality_code"),
    current_date().cast("string").alias("load_dt"),
    current_timestamp().cast("string").alias("load_dtm"),
    "year"
)

# Lưu dữ liệu vào vị trí cụ thể trên HDFS
final.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/airline_data/cur/flight_facts")

# Dừng SparkSession
spark.stop()
