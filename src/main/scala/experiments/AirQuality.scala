package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import st4ml.instances._
import st4ml.operators.converter.{Event2RasterConverter, Event2SpatialMapConverter}
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.lang.System.nanoTime

object AirQuality {
  case class AirRaw(
                     station_id: Int,
                     PM25_Concentration: Double,
                     PM10_Concentration: Double,
                     NO2_Concentration: Double,
                     CO_Concentration: Double,
                     O3_Concentration: Double,
                     SO2_Concentration: Double,
                     t: Long,
                     longitude: Double,
                     latitude: Double)

  def main(args: Array[String]): Unit = {
    val t = nanoTime()

    // datasets/aq.parquet "datasets/map_bj.csv" 64
    val dataDir = args(0)
    val mapDir = args(1)
    val numPartitions = args(2).toInt

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

    val sRange = Extent(115.42, 39.45, 117.43, 40.99)
    val tRange = Duration(1398873600, 1430409600)
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
    val polygons = mapRDD.collect.map(x => Polygon(x.getCoordinates ++ x.getCoordinates.reverse))
    val converter = new Event2SpatialMapConverter(polygons)
    val convertedRDD = converter.convert(selectedRDD).map(x => (1, x.toString))
    val resDf = spark.createDataFrame(convertedRDD)
    resDf.write.format("noop")
    convertedRDD.take(2).foreach(println)
    println(s"Grid hourly aggregation ${(nanoTime - t) * 1e-9} s")

    sc.stop()
  }
}
