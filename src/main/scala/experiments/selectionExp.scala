package experiments

import geometry.Rectangle
import operators.convertion.Traj2PointConverter
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import utils.Config

import java.lang.System.nanoTime
import scala.util.Random

/**
 * Compare the time usage of selection(repartition + rtree) vs filtering on different selectivities
 */
object selectionExp extends App {
  val spark = SparkSession.builder()
    .appName("SelectorExp")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val trajectoryFile = Config.get("hzData")
  val numPartitions = Config.get("numPartitions").toInt

  /** use pointRDD as input data */
  val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
    .persist(StorageLevel.MEMORY_AND_DISK)
  val dataSize = trajRDD.count
  val converter = new Traj2PointConverter()
  val pointRDD = converter.convert(trajRDD.map((0, _)))
  pointRDD.take(1)

  val spatialRange = Rectangle(Array(118.35, 29.183, 120.5, 30.55))
  val temporalRange = (pointRDD.map(_.timeStamp._1).min, pointRDD.map(_.timeStamp._1).max)

  for (selectivity <- List(0.5, 0.1, 0.01)) {
    println(s"=== Selectivity: $selectivity")
    val tQuery = genTQuery(temporalRange, selectivity)
    val sQuery = genSQuery(spatialRange, selectivity)

    var t = nanoTime()
    val selector = DefaultSelector(numPartitions)
    val selected = selector.query(pointRDD, sQuery, tQuery)
    println(s"--- ${selected.count} points selected ")
    println(s"... ST-Tool takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    t = nanoTime()
    val benchmark = pointRDD.filter(_.inside(sQuery)).filter(x => temporalOverlap(x.timeStamp, tQuery))
    println(s"--- ${benchmark.count} points selected ")
    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  }
  sc.stop()


  def genSQuery(spatialRange: Rectangle, selectivity: Double, seed: Int = 1): Rectangle = {
    val xLength = (spatialRange.xMax - spatialRange.xMin) * math.sqrt(selectivity)
    val yLength = (spatialRange.yMax - spatialRange.yMin) * math.sqrt(selectivity)
    val xMargin = (spatialRange.xMax - spatialRange.xMin) * (1 - math.sqrt(selectivity))
    val yMargin = (spatialRange.yMax - spatialRange.yMin) * (1 - math.sqrt(selectivity))
    val r = Random
    r.setSeed(seed)
    val newXMin = spatialRange.xMin + r.nextDouble() * xMargin
    val newXMax = newXMin + xLength
    val newYMin = spatialRange.yMin + r.nextDouble() * yMargin
    val newYMax = newYMin + yLength
    Rectangle(Array(newXMin, newYMin, newXMax, newYMax))
  }

  def genTQuery(temporalRange: (Long, Long), selectivity: Double, seed: Int = 1): (Long, Long) = {
    val r = Random
    r.setSeed(seed)
    val margin = (temporalRange._2 - temporalRange._1) * (1 - selectivity)
    val length = (temporalRange._2 - temporalRange._1) * selectivity
    val start = (r.nextDouble() * margin + temporalRange._1).toLong
    val end = (start + length).toLong
    (start, end)
  }

  def temporalOverlap(t1: (Long, Long), t2: (Long, Long)): Boolean = {
    if (t1._1 >= t2._1 && t1._1 <= t2._2) true
    else if (t2._1 >= t1._1 && t2._1 <= t1._2) true
    else false
  }
}
