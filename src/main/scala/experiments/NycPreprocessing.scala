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
    val rootDir = args(0)

    val spark = SparkSession.builder()
      .appName("NycPreprocessing")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val greenTaxiDf = spark.read.parquet("../yellow") //.filter(col("startLon").isNotNull)
    greenTaxiDf.printSchema()
    greenTaxiDf.show(2, false)
    println(greenTaxiDf.count())

    val greenTaxiDs = parseEventYellow(greenTaxiDf.sample(1 / 10000.0))
    //    println(greenTaxiDs.count)
    //    greenTaxiDs.persist(StorageLevel.MEMORY_AND_DISK)
    //    greenTaxiDs.show(2)
    //    Thread.sleep(1000000)


    //    /** test */
    //    //    greenTaxiDs.sample(1/10000.0).write.parquet("datasets/green_example")
    //    val selector = new Selector[Event[Point, Option[String], String]]()
    //    val rdd = selector.selectEvent("datasets/green_example")
    //    println(rdd.count)
    //    rdd.take(5).foreach(println)

    //    spark.read.parquet("datasets/green_example").show(5,false)
    sc.stop()
  }

  def parseEventGreen(df: DataFrame): Dataset[E] = {
    val spark = SparkSession.getActiveSession.get
    val colVals = df.columns.flatMap { c => Array(lit(c), col(c)) }
    val rdd = df.withColumn("auxi", map(colVals: _*))
      .select("lpepPickupDatetime", "lpepDropoffDatetime",
        "pickupLongitude", "pickupLatitude",
        "dropoffLongitude", "dropoffLatitude", "auxi").rdd
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
      .filter(col("startLat").isNotNull &&
        col("startLon").isNotNull &&
        col("endLat").isNotNull &&
        col("endLon").isNotNull)
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
