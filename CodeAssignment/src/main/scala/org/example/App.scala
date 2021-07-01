  package org.example

  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.io.Text
  import org.apache.hadoop.io.LongWritable
  import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

  object CodeTest {

      def calculate_perc(x: Int, y:Int): Int = {
      return (x*100)/y
    }

    def main(arg: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("CodeTest").getOrCreate()
      val sc = spark.sparkContext

      val hconf = new Configuration
      hconf.set("textinputformat.record.delimiter", ";\r\n")
      val data = sc.newAPIHadoopFile("File:///home/ubuntu/Documents/assignment/dataset_flight_raw.csv", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hconf).map(_._2.toString)

      val textrdd = data.map(x => x.split(",")).filter(x => x.length > 2 & x(0) != "Year").map(x => Row.fromSeq(x))

      val dataschema = StructType(List(
        StructField("Year", StringType),
        StructField("Month", StringType),
        StructField("DayOfMonth", StringType),
        StructField("DayOfWeek", StringType),
        StructField("DepTime", StringType),
        StructField("CRSDepTime", StringType),
        StructField("ArrTime", StringType),
        StructField("CRSArrTime", StringType),
        StructField("UniqueCarrier", StringType),
        StructField("FlightNum", StringType),
        StructField("TailNum", StringType),
        StructField("ActualElapsedTime", StringType),
        StructField("CRSElapsedTime", StringType),
        StructField("AirTime", StringType),
        StructField("ArrDelay", StringType),
        StructField("DepDelay", StringType),
        StructField("Origin", StringType),
        StructField("Dest", StringType),
        StructField("Distance", StringType),
        StructField("TaxiIn", StringType),
        StructField("TaxiOut", StringType),
        StructField("Cancelled", StringType),
        StructField("CancellationCode", StringType),
        StructField("Diverted", StringType),
        StructField("CarrierDelay", StringType),
        StructField("WeatherDelay", StringType),
        StructField("NASDelay", StringType),
        StructField("SecurityDelay", StringType),
        StructField("LateAircraftDelay", StringType)
      ))

      val sqlContext = spark.sqlContext
      import sqlContext.implicits._
      val df1 = sqlContext.createDataFrame(textrdd, dataschema)
      val df2 = df1.withColumn("Year", col("Year")
        .cast(IntegerType)).withColumn("Month", col("Month")
        .cast(IntegerType)).withColumn("DayOfMonth", col("DayOfMonth")
        .cast(IntegerType)).withColumn("DayOfWeek", col("DayOfWeek")
        .cast(IntegerType)).withColumn("DepTime", col("DepTime")
        .cast(IntegerType)).withColumn("CRSDepTime", col("CRSDepTime").cast(IntegerType)).withColumn("ArrTime", col("ArrTime").cast(IntegerType)).withColumn("CRSArrTime", col("CRSArrTime").cast(IntegerType)).withColumn("ActualElapsedTime", col("ActualElapsedTime").cast(IntegerType)).withColumn("CRSElapsedTime", col("CRSElapsedTime").cast(IntegerType)).withColumn("AirTime", col("AirTime").cast(IntegerType)).withColumn("ArrDelay", col("ArrDelay").cast(IntegerType)).withColumn("DepDelay", col("DepDelay").cast(IntegerType)).withColumn("TaxiIn", col("TaxiIn").cast(IntegerType)).withColumn("TaxiOut", col("TaxiOut").cast(IntegerType)).withColumn("Cancelled", col("Cancelled").cast(BooleanType)).withColumn("Diverted", col("Diverted").cast(BooleanType)).withColumn("CarrierDelay", col("CarrierDelay").cast(IntegerType)).withColumn("WeatherDelay", col("WeatherDelay").cast(IntegerType)).withColumn("NASDelay", col("NASDelay").cast(IntegerType)).withColumn("SecurityDelay", col("SecurityDelay").cast(IntegerType)).withColumn("LateAircraftDelay", col("LateAircraftDelay").cast(IntegerType))

      val grouped_df_diverted = df2.groupBy("Year", "Month", "DayOfMonth", "Diverted").agg(count("Diverted").as("diverted_flights")).filter(col("Diverted")).select("Year", "Month", "DayOfMonth", "Diverted", "diverted_flights")

      val grouped_df_cancelled = df2.groupBy("Year", "Month", "DayOfMonth", "Cancelled").agg(count("Cancelled").as("cancelled_flights")).filter(col("Cancelled")).select("Year", "Month", "DayOfMonth", "Cancelled", "cancelled_flights")

      val prc_each_factor = df2.filter(col("ArrDelay") > 20).withColumn("LateAircraftDelay_prc", col("LateAircraftDelay") * lit(100) / col("ArrDelay")).withColumn("NASDelay_prc", col("NASDelay") * lit(100) / col("ArrDelay")).withColumn("SecurityDelay_prc", col("SecurityDelay") * lit(100) / col("ArrDelay")).withColumn("WeatherDelay_prc", col("WeatherDelay") * lit(100) / col("ArrDelay")).withColumn("CarrierDelay_prc", col("CarrierDelay") * lit(100) / col("ArrDelay"))

      val factors = Array("LateAircraftDelay_prc", "NASDelay_prc", "SecurityDelay_prc", "WeatherDelay_prc", "CarrierDelay_prc")
      val structs = factors.map(
        c => struct(col(c).as("v"), lit(c).as("k")))
      val kpi_per_row = prc_each_factor.withColumn("kpi_per_row", greatest(structs: _*).getItem("k"))

      val kpi_group = kpi_per_row.groupBy("Year", "Month", "DayOfMonth", "kpi_per_row").count().as("kpi_count")
      val kpi_group2 = kpi_group.groupBy("Year", "Month", "DayOfMonth").agg(max(col("kpi_count")).as("max_kpi_value"))

      val final_kpi = kpi_group.join(kpi_group2, kpi_group("Year") <=> kpi_group2("Year") && kpi_group("Month") <=> kpi_group2("Month") && kpi_group("DayOfMonth") <=> kpi_group2("DayOfMonth") && kpi_group("kpi_count") <=> kpi_group2("max_kpi_value")).select(kpi_group("*"))

      val final_kpi_with_cancelled_diverted = final_kpi.join(grouped_df_diverted, Seq("Year", "Month", "DayOfMonth"), "full").join(grouped_df_cancelled, Seq("Year", "Month", "DayOfMonth"), "full").withColumnRenamed("kpi_per_row", "kpi").select("Year", "Month", "DayOfMonth", "cancelled_flights", "diverted_flights", "kpi").withColumn("StartPeriod", concat(col("Year"), lit("-"), col("Month"), lit("-"), col("DayOfMonth"), lit(" 00:00:00")).cast(TimestampType)).withColumn("EndPeriod", concat(col("Year"), lit("-"), col("Month"), lit("-"), col("DayOfMonth"), lit(" 11:59:59")).cast(TimestampType)).withColumn("LoadDateTime", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

      //to be fetched from config
      val server_name = "jdbc:sqlserver:///localhost:1433"
      val database_name = "test"
      val url = server_name + ";" + "databaseName=" + database_name + ";"

      val table_name = "flight_data"
      val username = "admin"
      val password = "admin123"
      val loaded_data = spark.read.format("com.microsoft.sqlserver.jdbc.spark").option("url", url).option("dbtable", table_name).option("user", username).option("password", password).load()

      //Upsert operation
      val final_data_temp1 = loaded_data.join(final_kpi_with_cancelled_diverted,
        loaded_data("StartPeriod") <=> final_kpi_with_cancelled_diverted("StartPeriod")
          && loaded_data("EndPeriod") <=> final_kpi_with_cancelled_diverted("EndPeriod")
          && loaded_data("Cancelled") =!= final_kpi_with_cancelled_diverted("Cancelled")
          && loaded_data("Diverted") =!= final_kpi_with_cancelled_diverted("Diverted")
          && loaded_data("kpi") =!= final_kpi_with_cancelled_diverted("kpi")
      ).select(final_kpi_with_cancelled_diverted("*"))
      val final_data_temp2 = loaded_data.join(final_kpi_with_cancelled_diverted,
        loaded_data("StartPeriod") =!= final_kpi_with_cancelled_diverted("StartPeriod")
          && loaded_data("EndPeriod") =!= final_kpi_with_cancelled_diverted("EndPeriod")
      ).select(final_kpi_with_cancelled_diverted("*"))
      val final_data = final_data_temp1.union(final_data_temp2)

      final_data.write.format("com.microsoft.sqlserver.jdbc.spark").mode("overwrite").option("url", url).option("dbtable", table_name).option("user", username).option("password", password).save()
    }
  }

