package st4ml.operators.converter

import st4ml.instances.{Duration, Entry, Extent, Geometry, Polygon, Raster, SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import scala.reflect.ClassTag

// only fully contained cells are converted
class Raster2SmConverter[S <: Geometry : ClassTag,
  V: ClassTag, D: ClassTag, V2: ClassTag](sArray: Array[Polygon]) extends Converter {
  type I = Raster[S, V, D]
  type O = SpatialMap[Polygon, V2, None.type]
  override val optimization: String = ""

  def convert(input: RDD[I], f: Array[V] => V2): RDD[O] = {
    input.map { raster =>
      val grouped = raster.entries.map(entry =>
        (sArray.zipWithIndex.find(_._1.contains(entry.spatial)), entry))
        .filter(_._1.isDefined)
        .map(x => (x._1.get, x._2.value))
        .groupBy(_._1).map(x => (x._1, f(x._2.map(_._2)))).toArray
      new SpatialMap[Polygon, V2, None.type](grouped.map(x =>
        new Entry(x._1._1, Duration.empty, x._2)), None)
    }
  }
}

object R2TTest extends App {
  val rasters = Array(new Raster(
    Array(new Entry(Extent(0, 0, 1, 1).toPolygon, Duration(0, 1), 1),
      new Entry(Extent(1, 1, 2, 2).toPolygon, Duration(0, 1), 2),
      new Entry(Extent(1.5, 1.5, 2.5, 2.5).toPolygon, Duration(1, 2), 3),
      new Entry(Extent(0, 0, 1, 1).toPolygon, Duration(1, 2), 4),
      new Entry(Extent(1, 1, 2, 2).toPolygon, Duration(1, 2), 5)), None
  ))
  val spark = SparkSession.builder()
    .appName("addNoise")
    .master(Config.get("master"))
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val rasterRDD = sc.parallelize(rasters)
  val map = Array(
    Extent(0, 0, 1, 1).toPolygon, Extent(1, 1, 2, 2).toPolygon
  )

  def f(x: Array[Int]): Int = x.sum

  val converter = new Raster2SmConverter[Polygon, Int, None.type, Int](map)
  val cRDD = converter.convert(rasterRDD, f)
  println(cRDD.collect.deep)

}