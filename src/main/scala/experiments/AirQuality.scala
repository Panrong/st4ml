package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances._
import st4ml.operators.converter.{Event2RasterConverter, Event2SpatialMapConverter}
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object AirQuality {
  case class AirRaw(station_id: Int, PM25_Concentration: Double, PM10_Concentration: Double, NO2_Concentration: Double,
                    CO_Concentration: Double, O3_Concentration: Double, SO2_Concentration: Double, t: Long, longitude: Double, latitude: Double)

  def main(args: Array[String]): Unit = {
    val t = nanoTime()
    val dataDir = args(0)
    val mapDir = args(1)
    val queryFile = args(2)
    val numPartitions = args(3).toInt
    val spark = SparkSession.builder()
      .appName("AirQuality")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val aqRDD = spark.read.option("header", true).parquet(dataDir).as[AirRaw].rdd
    val eventRDD = aqRDD.map(x => Event(Point(x.longitude, x.latitude), Duration(x.t), Array(x.PM25_Concentration, x.PM10_Concentration, x.NO2_Concentration,
      x.CO_Concentration, x.O3_Concentration, x.SO2_Concentration), x.station_id))
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    for ((sRange, tRange) <- ranges) {
      val selector = new Selector[Event[Point, Array[Double], Int]](sRange, tRange, numPartitions)
      val selectedRDD = selector.selectRDD(eventRDD)
      val map = spark.read.option("delimiter", " ").csv(mapDir).rdd
      val mapRDD = map.map(x => {
        val lsString = x.getString(1)
        val points = lsString.drop(1).dropRight(1).split("\\),").map { x =>
          val p = x.replace("(", "").replace(")", "").split(",").map(_.toDouble)
          Point(p(0), p(1))
        }
        LineString(points)
      })
      val polygons = mapRDD.collect.map(x => Polygon(x.getCoordinates ++ x.getCoordinates.reverse)).filter(x => x.intersects(sRange))
      val ts = Range(tRange.start.toInt, (tRange.end + 1).toInt, 86400).sliding(2).toArray
      val st = for (i <- polygons; j <- ts) yield (i, j)
      val converter = new Event2RasterConverter(st.map(_._1), st.map(x => Duration(x._2(0).toLong, x._2(1).toLong)))
      val convertedRDD = converter.convert(selectedRDD).map(x => (1, x.toString))
      println(convertedRDD.count)
    }
    println(s"Air aggregation ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
