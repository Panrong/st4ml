package preprocessing

import instances.{Duration, Event, Extent, Point}
import operatorsNew.selector.DefaultLegacySelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object DataLoadingTestEvent {
  case class E(lon: Double, lat: Double, t: Long, id: String)

  def main(args: Array[String]): Unit = {
    val t = nanoTime
    val spark = SparkSession.builder()
      .appName("DataLoadingTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val spatialRange = args(1).split(",").map(_.toDouble)
    val temporalRange = args(2).split(",").map(_.toLong)
    val numPartitions = args(3).toInt

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))


    // read parquet
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val eventRDD = readDs.as[E].rdd.map(x => {
      Event(Point(x.lon, x.lat), new Duration(x.t, x.t), None, x.id)
    })
    val selector = new DefaultLegacySelector[Event[Point, None.type, String]](sQuery, tQuery, numPartitions)

    val res = selector.query(eventRDD).count
    println(res)
    println(fileName)
    println((nanoTime - t) * 1e-9)

  }
}
