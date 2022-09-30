package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.Point
import st4ml.operators.selector.SelectionUtils.{E, EwP}
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

    val eventDs = spark.read.parquet(fileName).as[EwP]
    eventDs.show(5, false)
    val eventRDD = eventDs.toRdd
    val rdd = eventRDD.map { x =>
      val data = str2Map(x._1.data)
      (x._1.entries.head.spatial.asInstanceOf[Point].getX,
        x._1.entries.head.spatial.asInstanceOf[Point].getY,
        x._1.entries.head.temporal.start,
        data.getOrElse("puLocationId", "none").replace("null", "none"),
        data.getOrElse("rateCodeID", "none").replace("null", "none"),
        data.getOrElse("passengerCount", "none").replace("null", "none"),
        data.getOrElse("pickupLongitude", "none").replace("null", "none"),
        data.getOrElse("mtaTax", "none").replace("null", "none"),
        data.getOrElse("improvementSurcharge", "none").replace("null", "none"),
        data.getOrElse("tripType", "none").replace("null", "none"),
        data.getOrElse("doLocationId", "none").replace("null", "none"),
        data.getOrElse("paymentType", "none").replace("null", "none"),
        data.getOrElse("pickupLatitude", "none").replace("null", "none"),
        data.getOrElse("tripDistance", "none").replace("null", "none"),
        data.getOrElse("vendorID", "none").replace("null", "none"),
        data.getOrElse("totalAmount", "none").replace("null", "none"),
        data.getOrElse("lpepDropoffDatetime", "none").replace("null", "none"),
        data.getOrElse("dropoffLatitude", "none").replace("null", "none"),
        data.getOrElse("lpepPickupDatetime", "none").replace("null", "none"),
        data.getOrElse("dropoffLongitude", "none").replace("null", "none"),
        data.getOrElse("fareAmount", "none").replace("null", "none"),
      )
    }
    val df = rdd.toDF()
    val Array(df1, df2, df3, df4, df5) = df.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2))
    df.show(5)
    if (resName != "none") {
      df1.write.mode("append").csv(resName)
      println("df1 done")
      df2.write.mode("append").csv(resName)
      println("df2 done")
      df3.write.mode("append").csv(resName)
      println("df3 done")
      df4.write.mode("append").csv(resName)
      println("df4 done")
      df5.write.mode("append").csv(resName)
      println("df5 done")
    }
    sc.stop()
  }

  def str2Map(str: String): Map[String, String] = {
    val contents = str.drop(4).dropRight(1).split(", ")
    contents.map(x => (x.split(" -> ")(0), x.split(" -> ")(1))).toMap
  }
}


