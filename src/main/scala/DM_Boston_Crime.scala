import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DM_Boston_Crime{

  private def TS2Date():String = {
    val ts = System.currentTimeMillis()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSSZ")
    df.format(ts)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Invalid args")
      System.exit(1)
    }

    val in_path = args(0)
    val out_path = args(1)
    val HEADER_TRUE = true

    val spark = SparkSession
      .builder
      .appName("DM_BostonCrime")
      .getOrCreate()

    val crime =
      spark.read
        .option("header",HEADER_TRUE)
        .csv(in_path + "/crime.csv")

    val offense_codes =
      spark.read
        .option("header",HEADER_TRUE)
        .csv(in_path + "/offense_codes.csv")
        .distinct

    val crime_type =
          offense_codes
            .select(col("CODE"), split(col("NAME"), " - ")(0)
              .alias("crime_type_col"))

    // Datamart column crimes_total, lat, lng
    val crime_c1 =
          crime
            .join(offense_codes, crime("OFFENSE_CODE") === offense_codes("CODE"), "leftouter")
            .groupBy("DISTRICT")
            .agg(count("*").alias("crimes_total")
                , avg("Lat").alias("lat")
                , avg("Long").alias("lng")
              )


    val frequent_crime_types_total =
          crime
            .join(crime_type, crime("OFFENSE_CODE").cast("int") === crime_type("CODE"), "leftouter")
            .groupBy("DISTRICT", "crime_type_col")
            .agg(count("*").alias("crimes_total"))


    val window_frequent_crime_types =
          Window
            .partitionBy("DISTRICT")
            .orderBy(col("crimes_total").desc)

    // Datamart column frequent_crime_types
    val frequent_crime_types =
          frequent_crime_types_total
            .withColumn("row_number"
                , row_number.over(window_frequent_crime_types))
            .filter("row_number <= 3")
            .groupBy("DISTRICT")
            .agg(collect_list("crime_type_col").alias("frequent_crime_types_array"))
            .withColumn("frequent_crime_types", concat_ws(", ", col("frequent_crime_types_array")))
            .drop("frequent_crime_types_array")



    val crimes_monthly_total = crime.groupBy("DISTRICT", "YEAR", "MONTH").agg(count("*").alias("crimes_total"))

    // Datamart column crimes_monthly
    val crimes_monthly =
          crimes_monthly_total
            .groupBy("DISTRICT")
            .agg(expr("approx_percentile(crimes_total, array(0.5))")(0).as("crimes_monthly"))

    val crime_c2 =
          crime_c1
            .join(crimes_monthly, crime_c1("DISTRICT") === crimes_monthly("DISTRICT"))
            .drop(crimes_monthly("DISTRICT"))

    val crime_report =
          crime_c2
            .join(frequent_crime_types, crime_c2("DISTRICT") === frequent_crime_types("DISTRICT"))
            .drop(frequent_crime_types("DISTRICT"))

    //wirite to file
    crime_report
      .select("DISTRICT", "crimes_total", "crimes_monthly", "frequent_crime_types", "lat", "lng")
      .write
        .parquet(out_path + "/datamart_" + TS2Date() + ".parquet")

    spark.stop()
  }

}