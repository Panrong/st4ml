package experiments

import com.sun.xml.internal.ws.api.model.ExceptionType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.{GeometryFactory, MultiPolygon}
import org.locationtech.jts.io.WKTReader
import st4ml.instances.{Duration, Event, Point, Polygon}
import st4ml.utils.Config

import java.lang.System.nanoTime

object Osm {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OSM")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val areas = readArea()
    println(s"== Loaded ${areas.length} postal code areas")

    val poiRDD = readPOI()
    println(s"== Loaded ${poiRDD.count} POIs")
    //    Thread.sleep(1000000)
    sc.stop()
  }

  def readPOI(poiDir: String = "../poi_small.json"): RDD[Event[Point, None.type, String]] = {
    val spark = SparkSession.getActiveSession.get
    val poiDf = spark.read.json(poiDir)
      .select("g", "id").filter(col("g").isNotNull && col("id").isNotNull)
    poiDf.rdd.mapPartitions { x =>
      val wktReader = new WKTReader()
      x.map { p =>
        val id = p.getLong(1).toString
        val point = wktReader.read(p.getString(0)).asInstanceOf[Point]
        Event(point, Duration.empty, d = id)
      }
    }
  }

  def readArea(areaDir: String = "../postal_all.json"): Array[(Long, Polygon)] = {
    val spark = SparkSession.getActiveSession.get
    spark.read.json(areaDir).printSchema()
    //    spark.read.json(areaDir).persist(StorageLevel.MEMORY_AND_DISK) // check size in UI
    val areaDf = spark.read.json(areaDir)
      .select("g", "id").filter(col("g").isNotNull && col("id").isNotNull &&
      !col("g").startsWith("POINT") &&
      !col("g").startsWith("LINESTRING"))
    val areaRdd = areaDf.rdd.map { x =>
      val wktReader = new WKTReader()
      val p = wktReader.read(x.getString(0))
      val id = x.getLong(1)
      val gf = new GeometryFactory()
      var polygon: Option[Polygon] = None
      try {
        polygon = Some(gf.createPolygon(p.getEnvelope.getCoordinates))
      }
      catch {
        case _: Exception => println(p)
      }
      (id, polygon)
    }.filter(_._2.isDefined).map(x => (x._1, x._2.get))

    areaRdd.cache()
    areaRdd.collect()
  }
}
