// for figure 7(h)
package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Event, Extent, Point}
import st4ml.operators.converter.Event2SpatialMapConverter
import st4ml.operators.selector.SelectionUtils._
import st4ml.operators.selector.partitioner.HashPartitioner
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object OsmPoiCount {
  def main(args: Array[String]): Unit = {
    val t = nanoTime()
    val poiDir = args(0)
    val postalDir = args(1)
    val queryFile = args(2)
    val numPartitions = args(3).toInt
    val spark = SparkSession.builder()
      .appName("OSM")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon
    })
    for (sRange <- ranges) {
      val areas = readArea(postalDir).filter(_._2.intersects(sRange))
      if (areas.length > 0) {
        val poiRDD = readPOI(poiDir).filter(_.intersects(sRange)).stPartition(new HashPartitioner(numPartitions))
        val converter = new Event2SpatialMapConverter(areas.map(_._2), optimization = "rtree")
        val agg = (x: Array[Event[Point, None.type, String]]) => x.length
        val convertedRDD = converter.convert(poiRDD, agg = agg)
        println(convertedRDD.count)
      }
    }
    println(s"poi aggregation ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
