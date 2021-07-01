package experiments

import geometry.{Point, Rectangle, TimeSeries, Trajectory}
import operators.OperatorSet
import operators.convertion.{Converter, DoNothingConverter, Point2TimeSeriesConverter, Traj2PointConverter}
import operators.extraction.{BaseExtractor, FakePlateExtractor, FlowExtractor, PointsAnalysisExtractor}
import operators.selection.DefaultSelector
import operators.selection.partitioner.STRPartitioner
import org.apache.spark.sql.SparkSession
import preprocessing.{ReadParquet, ReadTrajFile}
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

    /**
     * "-8.65, 41.13, -8.57, 41.17" "1372636800,1404172800" 5 3600
     */
    val sQuery = args(0).split(",").map(_.toDouble)
    val tQuery = args(1).split(",").map(_.toLong)
    val sSize = args(2).toInt
    val tSplit = args(3).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)

    val pointFile = Config.get("portoPoints")

    val pointRDD = ReadParquet.ReadFaceParquet(pointFile)

    val operators = new OperatorSet(Rectangle(sQuery), (tQuery(0), tQuery(1))) {
      type I = Point
      type O = TimeSeries[Point]
      val converter = new Point2TimeSeriesConverter(
        tQuery(0),
        tSplit,
        new STRPartitioner(stGrids.length),
        Some(grids.zipWithIndex.map(x => (x._2, Rectangle(x._1))).toMap)
      )
      override val extractor = new FlowExtractor
    }

    val rdd1 = operators.selector.query(pointRDD)
    val rdd2 = operators.converter.convert(rdd1)
    val rdd3 = operators.extractor.extract(rdd2)

    rdd3.take(5).foreach(x => println(x._1.deep, x._2, x._3))
    sc.stop()
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

  def genSTGrids(grids: Array[Array[Double]], tRange: (Long, Long), tSplit: Int): Array[(Array[Double], Array[Long])] = {
    val tSlots = ((tRange._1 until tRange._2 by tSplit.toLong).toArray :+ tRange._2).sliding(2).toArray
    grids.flatMap(grid => tSlots.map(t => (grid, t)))
  }
}

