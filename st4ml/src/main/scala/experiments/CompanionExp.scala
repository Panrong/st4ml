package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Extent, Point}
import st4ml.operators.extractor.EventCompanionExtractor
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object CompanionExp {
  def main(args: Array[String]): Unit = {
    val dataDir = args(0)
    val metadataDir = args(1)
    val queryFile = args(2)
    val sThreshold = args(3).toDouble
    val tThreshold = args(4).toInt
    val parallelism = args(5).toInt
    val mode = args(6).toInt
    val spark = SparkSession.builder()
      .appName("AnomalyExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type EVENT = Event[Point, None.type, String]

    for ((spatial, temporal) <- ranges.take(5)) {
      val selector = new Selector[EVENT]().setSQuery(spatial).setTQuery(temporal)
      val eventRDD = selector.selectEvent(dataDir, metadataDir, partition = false).map(_.asInstanceOf[EVENT])
      val extractor = EventCompanionExtractor(sThreshold, tThreshold, parallelism)

      val extractedRDD = if (mode == 0) extractor.extractDetailV2(eventRDD) else extractor.extractWith2DSTR(eventRDD)
      println(extractedRDD.take(5).deep)
    }
    println(s"Anomaly extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()

  }
}
