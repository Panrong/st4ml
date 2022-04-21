package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.Point
import st4ml.operators.selector.SelectionUtils.E
import st4ml.utils.Config

object point4GeoMesa {
  def main(args: Array[String]): Unit = {
    // datasets/nyc_example none
    val spark = SparkSession.builder()
      .appName("point4GeoMesa")
      .master(Config.get("master"))
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val resName = args(1)

    val eventDs = spark.read.parquet(fileName).as[E]
    eventDs.show(5, false)
    val eventRDD = eventDs.toRdd
    val rdd = eventRDD.map { x =>
      val data = str2Map(x.data)
      (x.entries.head.spatial.asInstanceOf[Point].getX,
        x.entries.head.spatial.asInstanceOf[Point].getY,
        x.entries.head.temporal.start,
        data.getOrElse("medallion", "none"),
        data.getOrElse("hack_license", "none"),
        data.getOrElse("vendor_id", "none"),
        data.getOrElse("rate_code", "none"),
        data.getOrElse("store_and_fwd_flag", "none"),
        data.getOrElse("pickup_datetime", "none"),
        data.getOrElse("dropoff_datetime", "none"),
        data.getOrElse("passenger_count", "none"),
        data.getOrElse("trip_time_in_secs", "none"),
        data.getOrElse("trip_distance", "none"),
        data.getOrElse("dropoff_longitude", "none"),
        data.getOrElse("dropoff_latitude", "none")
      )
    }
    val df = rdd.toDF()
    df.show(5)
    if (resName != "none") df.write.csv(resName)
    sc.stop()
  }

  def str2Map(str: String): Map[String, String] = {
    val contents = str.drop(4).dropRight(1).split(", ")
    contents.map(x => (x.split(" -> ")(0), x.split(" -> ")(1))).toMap
  }
}
