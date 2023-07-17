// For figure 7(f)

package experiments

import experiments.ExpUtils.{splitSpatial, splitTemporal}
import st4ml.instances.{Duration, Entry, Extent, Polygon, Raster, Trajectory}
import st4ml.operators.converter.Traj2RasterConverter
import st4ml.operators.extractor.RasterTransitionExtractor
import st4ml.operators.selector.Selector
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config
import java.lang.System.nanoTime
import scala.io.Source

object RasterTransitionExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val gridSize = args(3).toDouble
    val tStep = args(4).toInt
    val numPartitions = args(5).toInt
    val spark = SparkSession.builder()
      .appName("RasterTransitionExtraction")
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
    type TRAJ = Trajectory[Option[String], String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, false)
      val sRanges = splitSpatial(spatial, gridSize)
      val tRanges = splitTemporal(Array(temporal.start, temporal.end), tStep)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      val convertedRDD = converter.convert(trajRDD)
      val extractor = new RasterTransitionExtractor[Trajectory[Option[String], String]]
      val extracted = extractor.extract(convertedRDD)
      convertedRDD.unpersist()
      trajRDD.unpersist()
      extracted.entries.take(5).foreach(println)
    }
    println(s"Raster transition extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
