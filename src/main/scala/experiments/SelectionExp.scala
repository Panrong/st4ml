package experiments

import geometry.{Point, Rectangle, Shape}
import operators.convertion.Traj2PointConverter
import operators.selection.DefaultSelector
import operators.selection.indexer.RTree
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import utils.Config

import java.lang.System.nanoTime
import scala.math.max
import scala.util.Random

/**
 * Compare the time usage of selection(repartition + rtree) vs filtering on different selectivities
 */
object SelectionExp extends App {
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
  pointRDD.cache()
  pointRDD.take(1)

  val spatialRange = Rectangle(Array(118.35, 29.183, 120.5, 30.55))
  val temporalRange = (pointRDD.map(_.timeStamp._1).min, pointRDD.map(_.timeStamp._1).max)

  //  for (selectivity <- List(0.5, 0.1, 0.01)) {
  //    println(s"=== Selectivity: $selectivity")
  //    val tQuery = genTQuery(temporalRange, selectivity)
  //    val sQuery = genSQuery(spatialRange, selectivity)
  //
  //    var t = nanoTime()
  //    val selector = DefaultSelector(numPartitions)
  //    val selected = selector.query(pointRDD, sQuery, tQuery)
  //    println(s"--- ${selected.count} points selected ")
  //    println(s"... ST-Tool takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  //
  //    t = nanoTime()
  //    val benchmark = pointRDD.filter(_.inside(sQuery)).filter(x => temporalOverlap(x.timeStamp, tQuery))
  //    println(s"--- ${benchmark.count} points selected ")
  //    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  //  }

  /** experiments on multiple queries */
  val partitioner = new HashPartitioner(numPartitions)
  val partitionRange = partitioner.partitionRange
  val temporalSelector = new TemporalSelector()
  val pRDD = partitioner.partition(pointRDD)
  val spatialSelector = RTreeHandlerMultiple(partitionRange)

  var t = nanoTime()
  for (selectivity <- List(0.5, 0.1, 0.01)) {
    t = nanoTime()
    println(s"\n=== Selectivity: $selectivity")
    val tQuery = genTQuery(temporalRange, selectivity)
    val sQuery = genSQuery(spatialRange, selectivity)
    val selected = temporalSelector.query(
      spatialSelector.query(pRDD)(sQuery)
    )(tQuery)
    println(s"--- ${selected.count} points selected ")
    println(s"... Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

  }

  println("\nBenchmark: ==")
  t = nanoTime()
  for (selectivity <- List(0.5, 0.1, 0.01)) {
    t = nanoTime()
    println(s"\n=== Selectivity: $selectivity")
    val tQuery = genTQuery(temporalRange, selectivity)
    val sQuery = genSQuery(spatialRange, selectivity)
    val benchmark = pointRDD.filter(_.inside(sQuery)).filter(x => temporalOverlap(x.timeStamp, tQuery))
    println(s"--- ${benchmark.count} points selected ")
    println(s"... Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

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

  // not generalized
  case class RTreeHandlerMultiple(partitionRange: Map[Int, Rectangle],
                                  var capacity: Option[Int] = None) {

    SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
      Array(classOf[RTree[_]],
        classOf[Rectangle],
        classOf[Shape]))

    var rTreeRDD: Option[RDD[(Int, RTree[Point])]] = None

    def genRTreeRDD(dataRDD: RDD[(Int, Point)]): RDD[(Int, RTree[Point])] = {
      val c = capacity.getOrElse({
        val dataSize = dataRDD.count
        max((dataSize / dataRDD.getNumPartitions / 100).toInt, 100)
      }) // rule for capacity calculation if not given
      val dataRDDWithIndex = dataRDD
        .map(x => x._2)
        .mapPartitionsWithIndex {
          (index, partitionIterator) => {
            val partitionsMap = scala.collection.mutable.Map[Int, List[Point]]()
            var partitionList = List[Point]()
            while (partitionIterator.hasNext) {
              partitionList = partitionIterator.next() :: partitionList
            }
            partitionsMap(index) = partitionList
            partitionsMap.iterator
          }
        }
      dataRDDWithIndex
        .mapPartitions(x => {
          val (pIndex, contents) = x.next
          val entries = contents.map(x => (x, x.id))
            .zipWithIndex.toArray.map(x => (x._1._1, x._1._2, x._2))
          val rtree = entries.length match {
            case 0 => RTree(entries, 0)
            case _ => RTree(entries, c)
          }
          List((pIndex, rtree)).iterator
        })
    }

    def query(dataRDD: RDD[(Int, Point)])(queryRange: Rectangle): RDD[(Int, Point)] = {
      rTreeRDD.getOrElse {
        println("generated RTree RDD")
        rTreeRDD = Some(genRTreeRDD(dataRDD))
        rTreeRDD.get
      }
        .filter {
          case (_, rtree) => rtree.numEntries != 0 && rtree.root.m_mbr.intersect(queryRange)
        }
        .flatMap { case (partitionID, rtree) => rtree.range(queryRange)
          .filter(x => queryRange.referencePoint(x._1).get.inside(partitionRange(partitionID))) // filter by reference point
          .map(x => (partitionID, x._1))
        }
    }
  }
}
