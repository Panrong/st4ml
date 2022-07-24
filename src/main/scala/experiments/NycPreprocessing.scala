package experiments

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.storage.StorageLevel
import st4ml.instances.{Event, Point}
import st4ml.operators.selector.SelectionUtils.E
import st4ml.operators.selector.Selector
import st4ml.utils.Config
import st4ml.utils.TimeParsing.time2Long

import java.util.TimeZone

object NycPreprocessing {
  def main(args: Array[String]): Unit = {
    // C:\Users\kaiqi001\Desktop\nyc\green\puYear=2013\puMonth=8 green datasets/green_example
    val rootDir = args(0)
    val color = args(1)
    val resDir = args(2)

    val spark = SparkSession.builder()
      .appName("NycPreprocessing")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val taxiDf = spark.read.parquet(rootDir)
    taxiDf.printSchema()
    taxiDf.show(2, false)
    println(taxiDf.count())

    val taxiDs = if (color == "yellow") parseEventYellow(taxiDf) else parseEventGreen(taxiDf)

    taxiDs.show(2)
    taxiDs.write.parquet(resDir)


    //    /** test */
    //        val selector = new Selector[Event[Point, None.type, String]]()
    //        selector.selectEvent(resDir).take(2).foreach(println)

    sc.stop()
  }

  def parseEventGreen(df: DataFrame): Dataset[E] = {
    val spark = SparkSession.getActiveSession.get
    val colVals = df.columns.flatMap { c => Array(lit(c), col(c)) }
    val rdd = df.withColumn("auxi", map(colVals: _*))
      .select("lpepPickupDatetime", "lpepDropoffDatetime",
        "pickupLongitude", "pickupLatitude",
        "dropoffLongitude", "dropoffLatitude", "auxi")
      .filter(col("pickupLatitude").cast("double") <= 50 &&
        col("pickupLatitude").cast("double") >= 25 &&
        col("pickupLongitude").cast("double") <= -65 &&
        col("pickupLongitude").cast("double") >= -130 &&
        col("dropoffLatitude").cast("double") <= 50 &&
        col("dropoffLatitude").cast("double") >= 25 &&
        col("dropoffLongitude").cast("double") <= -65 &&
        col("dropoffLongitude").cast("double") >= -130
      ).rdd
    val reformattedRdd = rdd.flatMap { x =>
      val t1 = time2Long(x.getTimestamp(0).toString.dropRight(2), timeZone = TimeZone.getTimeZone("America/New_York"))
      val timeStamp1 = Array(t1, t1)
      val shape1 = Point(x.getDouble(2), x.getDouble(3)).toString
      val d = x.getMap(6).toString
      val t2 = time2Long(x.getTimestamp(1).toString, timeZone = TimeZone.getTimeZone("America/New_York"))
      val timeStamp2 = Array(t2, t2)
      val shape2 = Point(x.getDouble(4), x.getDouble(5)).toString
      val event1 = E(shape1, timeStamp1, None, d)
      val event2 = E(shape2, timeStamp2, None, d)
      Array(event1, event2)
    }
    import spark.implicits._
    reformattedRdd.toDS()
  }

  def parseEventYellow(df: DataFrame): Dataset[E] = {
    val spark = SparkSession.getActiveSession.get
    val colVals = df.columns.flatMap { c => Array(lit(c), col(c)) }
    val rdd = df.withColumn("auxi", map(colVals: _*))
      .select("tpepPickupDatetime", "tpepDropoffDatetime",
        "startLon", "startLat",
        "endLon", "endLat", "auxi")
      .filter(col("startLat").cast("double") <= 50 &&
        col("startLat").cast("double") >= 25 &&
        col("startLon").cast("double") <= -65 &&
        col("startLon").cast("double") >= -130 &&
        col("endLat").cast("double") <= 50 &&
        col("endLat").cast("double") >= -25 &&
        col("endLon").cast("double") <= -65 &&
        col("endLon").cast("double") >= -130
      )
      .rdd
    val reformattedRdd = rdd.flatMap { x =>
      val t1 = time2Long(x.getTimestamp(0).toString.dropRight(2), timeZone = TimeZone.getTimeZone("America/New_York"))
      val timeStamp1 = Array(t1, t1)
      val shape1 = Point(x.getDouble(2), x.getDouble(3)).toString
      val d = x.getMap(6).toString
      val t2 = time2Long(x.getTimestamp(1).toString, timeZone = TimeZone.getTimeZone("America/New_York"))
      val timeStamp2 = Array(t2, t2)
      val shape2 = Point(x.getDouble(4), x.getDouble(5)).toString
      val event1 = E(shape1, timeStamp1, None, d)
      val event2 = E(shape2, timeStamp2, None, d)
      Array(event1, event2)
    }
    import spark.implicits._
    reformattedRdd.toDS()
  }
}
