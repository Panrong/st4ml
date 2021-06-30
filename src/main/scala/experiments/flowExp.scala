package experiments

import operators.convertion.Traj2PointConverter
import operators.extraction.PointsAnalysisExtractor
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile
import utils.Config

/**
 * divide the whole spatial range into grids and find how many points inside each grid
 * for every hour
 */
object flowExp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SelectorExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = Config.get("portoData")
    val trajRDD = ReadTrajFile(trajectoryFile, 16)
    val converter = new Traj2PointConverter()
    val rdd2 = converter.convert(trajRDD)
    val extractor = new PointsAnalysisExtractor
    val res = extractor.extractSpatialRange(rdd2)
    println(res.deep)
    //    val grids = genGrids(Array(), 5)
  }

  def genGrids(range: Array[Double], size: Int): Array[Array[Double]] = {
    val lonMin = range(0)
    val latMin = range(1)
    val lonMax = range(2)
    val latMax = range(3)
    val lons = ((lonMin until lonMax by (lonMax - lonMin) / size) :+ lonMax).sliding(2).toArray
    val lats = ((latMin until latMax by (latMax - latMin) / size) :+ latMax).sliding(2).toArray
    lons.flatMap(x => lats.map(y => Array(x(0), y(0), x(1), y(1))))
  }
}

