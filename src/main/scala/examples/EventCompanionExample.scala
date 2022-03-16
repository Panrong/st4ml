package examples

import st4ml.instances.{Duration, Event, Point}
import st4ml.operators.extractor.EventCompanionExtractor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.types.LongType
import st4ml.utils.TimeParsing
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import java.lang.System.nanoTime

object EventCompanionExample {

  def main(args: Array[String]): Unit = {
    val cameraDir = args(0)
    val dataDir = args(1)
    val sThreshold = args(2).toDouble
    val tThreshold = args(3).toInt
    val parallelism = args(4).toInt
    val samplingRate = args(5).toDouble
    val date = args.lift(6)
    val saveRes = args.lift(7)
    /** local test parameter:
     * "qingdaoTest/camera.parquet" "qingdaoTest/data.parquet" 200 600 8 0.01 2021-11-29 qingdaoTest/res */
    val tRange = if (date.isDefined) {
      Some(TimeParsing.date2Long(date.get))
    } else None
    val spark = SparkSession.builder()
      //      .master("local[8]") // TODO remove when deploying
      .appName("EventCompanionExample")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val t = nanoTime

    /** load data */
    val cameraDf = spark.read.parquet(cameraDir)
      .filter(col("latitude_w84") > 30).filter(col("latitude_w84") < 50)
      .filter(col("longitude_w84") > 110).filter(col("longitude_w84") < 130)
      .select(col("deviceId"), col("latitude_w84"), col("longitude_w84"))
    //    cameraDf.printSchema()
    val eventsRaw = if (samplingRate < 1) spark.read.parquet(dataDir).sample(false, samplingRate, 1)
    else spark.read.parquet(dataDir)
    val eventsDf = if (tRange.isEmpty) eventsRaw
      .select(col("pid"), col("timestamp"), col("cameraId"))
    else {
      val t = tRange.get
      eventsRaw.filter(col("timestamp").cast(LongType) >= t && col("timestamp").cast(LongType) <= t + 86400)
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
    eventRDD.cache
    /** extraction */
    val extractor = EventCompanionExtractor(sThreshold, tThreshold, parallelism)

    val extractedRDD = extractor.extractDetailV2(eventRDD)

    if (saveRes.isDefined) {
      extractedRDD.toDF("id1", "lon1", "lat1", "t1", "id2", "lon2", "lat2", "t2", "t_diff", "s_diff").write.csv(saveRes.get)
      println(s"results saved at '${saveRes.get}'")
    }
    else {
      println(extractedRDD.count)
      extractedRDD.take(5).foreach(println)
    }

    println(s"Companion extraction takes ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
