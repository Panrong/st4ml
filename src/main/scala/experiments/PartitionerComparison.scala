package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Event, Extent}
import st4ml.operators.extractor.{EventCompanionExtractor, TrajCompanionExtractor}
import st4ml.operators.selector.SelectionUtils.{E, T}
import st4ml.utils.Config

import java.lang.System.nanoTime

object PartitionerComparison {
  def main(args: Array[String]): Unit = {
    val t = nanoTime
    val mode = args(0)
    val fileName = args(1)
    val numPartitions = args(2).toInt
    val sThreshold = args(3).toDouble
    val tThreshold = args(4).toInt
    val opt = args(5).toBoolean
    val spark = SparkSession.builder()
      .appName("PartitionerComparison")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val ts = Range(1380585600, 1380585600+86400, 3600*6).sliding(2).toArray

    if (mode == "event") {
      for (i <- ts) {
        import spark.implicits._
        val eventRDD = spark.read.parquet(fileName).as[E].toRdd.map(x => Event(x.spatialCenter, x.entries.head.temporal, None, x.data))
          .filter(x => x.temporalCenter >= i(0) && x.temporalCenter <= i(1) && x.intersects(Extent(-74.023,40.701,-73.903,40.874).toPolygon))
        println(eventRDD.count)
        val extractor = EventCompanionExtractor(sThreshold, tThreshold, numPartitions)
        val extractedRDD = if (opt) extractor.extractDetailV2(eventRDD)
        else extractor.extractWith2DSTR(eventRDD)
        println(extractedRDD.count)
        extractedRDD.take(2).foreach(println)
        eventRDD.unpersist()
        extractedRDD.unpersist()
      }
    }
    else if (mode == "traj") {
      import spark.implicits._
      val trajRDD = spark.read.parquet(fileName).as[T].toRdd.map(x => x.mapValue(_ => None))
        .filter(x => x.temporalCenter >= 1380585600 && x.temporalCenter <= 1380672000)
      val extractor = TrajCompanionExtractor(sThreshold, tThreshold, numPartitions)
      val extractedRDD = if (opt) extractor.extractDetailV2(trajRDD)
      else extractor.extractWith2DSTR(trajRDD)
      println(extractedRDD.count)
      extractedRDD.take(2).foreach(println)
    }

    println(s"${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
