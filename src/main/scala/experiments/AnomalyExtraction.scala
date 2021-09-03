package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.extractor.AnomalyExtractor
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object AnomalyExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val tRange = args(3).split(",").map(_.toInt)

    val spark = SparkSession.builder()
      .appName("anomalyExtraction")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon
    })

    // read events
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val eventRDD = readDs.as[P].rdd.map(x => Event(Point(x.lon, x.lat), Duration(x.t), d = x.id))
    eventRDD.cache()
    val t = nanoTime()
    val extractor = new AnomalyExtractor[Event[Point, None.type, String]]
    val res = extractor.extract(eventRDD, tRange, ranges)
    println(s"Extraction ${(nanoTime - t) * 1e-9} s")
  }
}
