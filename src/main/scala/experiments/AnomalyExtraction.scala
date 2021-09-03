package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.extractor.AnomalyExtractor
import operatorsNew.selector.DefaultSelector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Config
import utils.TimeParsing._

import java.lang.System.nanoTime
import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source

object AnomalyExtraction {
  /*
  find number of anomalies in each week
   */
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

    //    // extract
    //    val t = nanoTime()
    //    val extractor = new AnomalyExtractor[Event[Point, None.type, String]]
    //    val res = extractor.extract(eventRDD, tRange, ranges)
    //    println(s"Extraction ${(nanoTime - t) * 1e-9} s")

    val t = nanoTime()
    val res = extractPerWeekAnomaly(tRange)(eventRDD)
    println(res)

    println(s"Multi range query ${(nanoTime - t) * 1e-9} s")
  }

  def longToWeek(t: Long): Int = {
    val d = new Date(t* 1000)
    val formatter = new SimpleDateFormat("w")
    val week = Integer.parseInt(formatter.format(d))
    week
  }

  def getHour(t: Long): Int =
    timeLong2String(t).split(" ")(1).split(":")(0).toInt

  def extractPerWeekAnomaly(threshold: Array[Int])(rdd: RDD[Event[Point, None.type, String]]): Map[Int, Int] = {
    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)
    val filteredRDD = rdd.filter(point => condition(getHour(point.duration.start)))
      .map(e => (longToWeek(e.duration.start), 1))
      .reduceByKey(_+_)
    filteredRDD.collect.toMap
  }
}
