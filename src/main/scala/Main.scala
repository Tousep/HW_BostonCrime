import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count}

object Main{

  def TS2Date():String = {
    val ts = System.currentTimeMillis()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss_SSSZ")
    df.format(ts)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }

    var in_path = args(1)
    var out_path = args(2)


    val spark = SparkSession
      .builder
      .appName("DM_BostonCrime")
      .getOrCreate()

    val crime =
      spark.read
        .option("header",true)
        .csv(in_path + "/crime.csv")

    val offense_codes =
      spark.read
        .option("header",true)
        .csv(in_path + "/offense_codes.csv")
        .distinct

    crime
      .join(offense_codes, crime("OFFENSE_CODE") === offense_codes("CODE"),"leftouter")
      .groupBy("DISTRICT")
      .agg(count("*").alias("crimes_total"))
      // .agg(percentile_approx("*",YEAR,MONTH)).alias("crimes_monthly")
      // .agg(("*")).alias("frequent_crime_types")
      // .agg(("*")).alias("crime_type")
      .agg(avg("Lat")).alias("lat")
      .agg(avg("Long")).alias("lng")
      .write.parquet(out_path + "/datamart" + TS2Date() + ".parquet")

    spark.stop()
  }

}