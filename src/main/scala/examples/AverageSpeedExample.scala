package examples

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Extent}
import st4ml.operators.converter.Traj2RasterConverter
import st4ml.operators.extractor.RasterSpeedExtractor
import st4ml.operators.selector.SelectionUtils.ReadRaster
import st4ml.operators.selector.Selector

object AverageSpeedExample {
  def main(args: Array[String]): Unit = {
    // example inputs: local[*] datasets/porto_toy datasets/porto_raster.csv 64
    val master = args(0)
    val trajDir = args(1)
    val rasterDir = args(2)
    val parallelism = args(3).toInt

    val spark = SparkSession.builder()
      .appName("AverageSpeedExample")
      .master(master)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val (sArray, tArray) = ReadRaster(rasterDir)
    val sRange = Extent(sArray).toPolygon
    val tRange = Duration(tArray)
    val selector = Selector(sRange, tRange, parallelism)
    val converter = new Traj2RasterConverter(sArray, tArray)
    val extractor = new RasterSpeedExtractor

    val selectedRDD = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD.count} trajectories")
    val convertedRDD = converter.convert(selectedRDD)
    println(s"--- Converted to ${converter.numCells} raster cells")
    val extractedRDD = extractor.extract(convertedRDD, metric = "greatCircle", convertKmh = true)
    println("=== Top 2 raster cells with the highest speed:")
    extractedRDD.sortBy(_._2, ascending = false).take(2).foreach(println)
    sc.stop()
  }

}
