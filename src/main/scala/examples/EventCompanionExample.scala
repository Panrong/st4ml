package examples

import instances.{Duration, Event, Point}
import operatorsNew.extractor.EventCompanionExtractor
import operatorsNew.selector.SelectionUtils.E
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.LongType
import utils.TimeParsing

import java.lang.System.nanoTime

object EventCompanionExample {

  def main(args: Array[String]): Unit = {
    val masterUrl = args(0)
    val cameraDir = args(1)
    val dataDir = args(2)
    val sThreshold = args(3).toDouble
    val tThreshold = args(4).toInt
    val parallelism = args(5).toInt
    val date = args.lift(6)
    val saveRes = args.lift(7)
    /** local test parameter:
     * local[4] "qingdaoTest/camera.parquet" "qingdaoTest/data.parquet" 200 600 8 2021-11-29 qingdaoTest/res */
    val tRange = if (date.isDefined) {
      Some(TimeParsing.date2Long(date.get))
    } else None
    val spark = SparkSession.builder()
      .appName("EventCompanionExample")
      .master(masterUrl)
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val t = nanoTime

    /** TODO load data */
    val cameraDf = spark.read.parquet(cameraDir) //.sample(false, 0.1)
      .select(col("deviceId"), col("latitude_w84"), col("longitude_w84"))
    //    cameraDf.printSchema()
    val eventsDf = if (tRange.isEmpty) spark.read.parquet(dataDir)
      .select(col("pid"), col("timestamp"), col("cameraId"))
    else {
      val t = tRange.get
      spark.read.parquet(dataDir).filter(col("timestamp").cast(LongType) >= t && col("timestamp").cast(LongType) <= t + 86400)
        .select(col("pid"), col("timestamp"), col("cameraId"))
    }
    //    eventsDf.show(5)
    val jointDf = eventsDf.join(broadcast(cameraDf), cameraDf.col("deviceId") === eventsDf.col("cameraId"))
      .drop(col("deviceId")).drop(col("cameraId"))
    //    jointDf.printSchema()
    //    jointDf.show(5)
    val eventRDD = jointDf.rdd.map { row =>
      val id = row.get(0).asInstanceOf[String]
      val t = row.get(1).asInstanceOf[String].toLong
      val lat = row.get(2).asInstanceOf[Double]
      val lon = row.get(3).asInstanceOf[Double]
      Event(Point(lon, lat), Duration(t), d = id)
    }

    println(s"Number of records: ${eventRDD.count}")

    /** extraction */
    val extractor = EventCompanionExtractor(sThreshold, tThreshold, parallelism)

    val extractedRDD = extractor.extract(eventRDD)
    println(s"Found ${extractedRDD.count} pairs of companions")
    extractedRDD.take(5).foreach(println)
    println(s"Companion extraction takes ${(nanoTime - t) * 1e-9} s")

    if (saveRes.isDefined) {
      extractedRDD.toDF("id1", "id2", "count").sort("id1").coalesce(1).write.csv(saveRes.get)
      println(s"results saved at '${saveRes.get}'")
    }

    /** correctness test */
    val gt = eventRDD.cartesian(eventRDD).filter {
      case (a, b) => extractor.isCompanion(a, b, sThreshold, tThreshold)
    }.map(x => ((x._1.data, x._2.data), 1))
      .reduceByKey(_ + _)
    println(gt.count / 2)
    println(gt.take(5).deep)

  }
}
