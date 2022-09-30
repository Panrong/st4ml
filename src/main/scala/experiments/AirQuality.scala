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

    // ../air-quality/ 64 1
    val dataDir = args(0)
    val numPartitions = args(1).toInt
    val mode = args(2)

    val spark = SparkSession.builder()
      .appName("AirQuality")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // grid-hourly agg
    if (mode == "1") {
      val stationSchema = StructType(Array(
        StructField("station_id", IntegerType, false),
        StructField("name_chinese", StringType, false),
        StructField("name_english", StringType, false),
        StructField("latitude", DoubleType, false),
        StructField("longitude", DoubleType, false),
        StructField("district_id", StringType, false)
      ))

      val stationDf = spark.read.option("header", true).schema(stationSchema).csv(dataDir + "station.csv")

      val airSchema = StructType(Array(
        StructField("station_id", IntegerType, false),
        StructField("time", StringType, false),
        StructField("PM25_Concentration", DoubleType, true),
        StructField("PM10_Concentration", DoubleType, true),
        StructField("NO2_Concentration", DoubleType, true),
        StructField("CO_Concentration", DoubleType, true),
        StructField("O3_Concentration", DoubleType, true),
        StructField("SO2_Concentration", DoubleType, true)
      ))
      val aqDf = spark.read.option("header", true).schema(airSchema).csv(dataDir + "airquality.csv")
        .withColumn("t", unix_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
        .drop("time")
        .filter(col("PM25_Concentration").isNotNull && col("PM10_Concentration").isNotNull
          && col("NO2_Concentration").isNotNull && col("CO_Concentration").isNotNull
          && col("O3_Concentration").isNotNull && col("SO2_Concentration").isNotNull)
        .join(stationDf, "station_id")
        .drop("name_chinese", "name_english", "district_id")

      import spark.implicits._
      val aqRDD = aqDf.as[AirRaw].rdd
      val eventRDD = aqRDD.map(x => Event(Point(x.longitude, x.latitude), Duration(x.t), Array(x.PM25_Concentration, x.PM10_Concentration, x.NO2_Concentration,
        x.CO_Concentration, x.O3_Concentration, x.SO2_Concentration), x.station_id))
      eventRDD.take(2).foreach(println)

      val sRange = Extent(115.42, 39.45, 117.43, 40.99)
      val tRange = Duration(1398873600, 1430409600)
      val selector = new Selector[Event[Point, Array[Double], Int]](sRange, tRange, numPartitions)
      val selectedRDD = selector.selectRDD(eventRDD)
      println(selectedRDD.count)

      val tCells = Range(tRange.start.toInt, tRange.end.toInt + 1, 3600).sliding(2).map(x => Duration(x(0), x(1))).toArray
      val longCells = Range(0, 11).map(x => x * (sRange.xMax - sRange.xMin) / 10 + sRange.xMin).sliding(2).toArray
      val latCells = Range(0, 11).map(x => x * (sRange.yMax - sRange.yMin) / 10 + sRange.yMin).sliding(2).toArray
      val sCells = for (i <- longCells; j <- latCells) yield Extent(i(0), j(0), i(1), j(1)).toPolygon
      val rasterCells = for (i <- sCells; j <- tCells) yield (i, j)
      val converter = new Event2RasterConverter(rasterCells.map(_._1), rasterCells.map(_._2))
      val convertedRDD = converter.convert(selectedRDD)
      convertedRDD.collect.take(2).foreach(println)
      println(s"Grid hourly aggregation ${(nanoTime - t) * 1e-9} s")
    }
    else if (mode == "2") {
      val stationSchema = StructType(Array(
        StructField("station_id", IntegerType, false),
        StructField("name_chinese", StringType, false),
        StructField("name_english", StringType, false),
        StructField("latitude", DoubleType, false),
        StructField("longitude", DoubleType, false),
        StructField("district_id", StringType, false)
      ))

      val stationDf = spark.read.option("header", true).schema(stationSchema).csv(dataDir + "station.csv")

      val airSchema = StructType(Array(
        StructField("station_id", IntegerType, false),
        StructField("time", StringType, false),
        StructField("PM25_Concentration", DoubleType, true),
        StructField("PM10_Concentration", DoubleType, true),
        StructField("NO2_Concentration", DoubleType, true),
        StructField("CO_Concentration", DoubleType, true),
        StructField("O3_Concentration", DoubleType, true),
        StructField("SO2_Concentration", DoubleType, true)
      ))
      val aqDf = spark.read.option("header", true).schema(airSchema).csv(dataDir + "airquality.csv")
        .withColumn("t", unix_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
        .drop("time")
        .filter(col("PM25_Concentration").isNotNull && col("PM10_Concentration").isNotNull
          && col("NO2_Concentration").isNotNull && col("CO_Concentration").isNotNull
          && col("O3_Concentration").isNotNull && col("SO2_Concentration").isNotNull)
        .join(stationDf, "station_id")
        .drop("name_chinese", "name_english", "district_id")

      import spark.implicits._
      val aqRDD = aqDf.as[AirRaw].rdd
      val eventRDD = aqRDD.map(x => Event(Point(x.longitude, x.latitude), Duration(x.t), Array(x.PM25_Concentration, x.PM10_Concentration, x.NO2_Concentration,
        x.CO_Concentration, x.O3_Concentration, x.SO2_Concentration), x.station_id))
      eventRDD.take(2).foreach(println)

      val sRange = Extent(115.42, 39.45, 117.43, 40.99)
      val tRange = Duration(1398873600, 1430409600)
      val selector = new Selector[Event[Point, Array[Double], Int]](sRange, tRange, numPartitions)
      val selectedRDD = selector.selectRDD(eventRDD)
      println(selectedRDD.count)
      val map = spark.read.option("delimiter", " ").csv("datasets/map_bj.csv").rdd
      val mapRDD = map.map(x => {
        //        val id = x.getString(0)
        val lsString = x.getString(1)
        val points = lsString.drop(1).dropRight(1).split("\\),").map { x =>
          val p = x.replace("(", "").replace(")", "")
            .split(",").map(_.toDouble)
          Point(p(0), p(1))
        }
        LineString(points)
      })
      val polygons = mapRDD.collect.map(x => Polygon(x.getCoordinates ++ x.getCoordinates.reverse))
      println(polygons.length)
      val converter = new Event2SpatialMapConverter(polygons)
      val convertedRDD = converter.convert(selectedRDD)
      convertedRDD.collect.take(2).foreach(println)
      println(s"Grid hourly aggregation ${(nanoTime - t) * 1e-9} s")
    }
    sc.stop()
  }
}
