package experiments

import geometry.{Point, Rectangle, Shape}
import operators.convertion.Traj2PointConverter
import operators.selection.indexer.RTree
import operators.selection.partitioner.STRPartitioner
import operators.selection.selectionHandler.TemporalSelector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.{ReadTrajFile, ReadTrajJson}
import utils.Config

import java.lang.System.nanoTime
import scala.math.{max, sqrt}
import scala.io.Source

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

  val trajectoryFile = Config.get("portoData")
  val numPartitions = Config.get("numPartitions").toInt


  val queries = readQueries(Config.get("portoQuery"))

  /** use trajRDD as input data */
  val trajRDD = ReadTrajFile(trajectoryFile, numPartitions)
    .persist(StorageLevel.MEMORY_AND_DISK)
  trajRDD.take(1)

  val spatialRange = Rectangle(Array(118.35, 29.183, 120.5, 30.55))
  val temporalRange = (trajRDD.map(_.timeStamp._1).min, trajRDD.map(_.timeStamp._1).max)


  /** experiments on multiple queries */
  val partitioner = new STRPartitioner(numPartitions, Some(0.1))
  partitioner.getPartitionRange(trajRDD)
  val partitionRange = partitioner.partitionRange
  //  println(partitionRange)
  val temporalSelector = new TemporalSelector()
  val pRDD = partitioner.partition(trajRDD)
  val spatialSelector = RTreeHandlerMultiple(partitionRange)

  var t = nanoTime()
  println(s"start ${queries.length} queries")
  for (query <- queries) {
    val tQuery = (query(4).toLong, query(5).toLong)
    val sQuery = Rectangle(query.slice(0, 4))
    val selected = temporalSelector.query(
      spatialSelector.query(pRDD)(sQuery)
    )(tQuery)

    //    val selected = pRDD.filter(x => x.intersect(sQuery) && temporalOverlap(x.timeStamp, tQuery))
    val c = selected.count
    println(s"--- $c points selected ")
  }
  println(s"... Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

  sc.stop()

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

    def genRTreeRDD(dataRDD: RDD[Point]): RDD[(Int, RTree[Point])] = {
      val rtreeT = nanoTime()
      val c = capacity.getOrElse({
        val dataSize = dataRDD.count
        max((dataSize / dataRDD.getNumPartitions / 100).toInt, 100)
      }) // rule for capacity calculation if not given
      val dataRDDWithIndex = dataRDD
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
      val res = dataRDDWithIndex
        .mapPartitions(x => {
          val (pIndex, contents) = x.next
          val entries = contents.map(x => (x, x.id))
            .zipWithIndex.toArray.map(x => (x._1._1, x._1._2, x._2))
          val rtree = entries.length match {
            case 0 => RTree(entries, 0)
            case _ => RTree(entries, 1000)
          }
          List((pIndex, rtree)).iterator
        })
      println(s"... Generating RTree takes ${((nanoTime() - rtreeT) * 1e-9).formatted("%.3f")} s.")
      res
    }

    def query(dataRDD: RDD[Point])(queryRange: Rectangle): RDD[Point] = {
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
          .map(x => x._1)
        }
    }
  }

  def readQueries(filename: String): Array[Array[Double]] = {
    var res = new Array[Array[Double]](0)
    for (line <- Source.fromFile(filename).getLines) {
      val query = line.split(" ").map(_.toDouble)
      res = res :+ query
    }
    res
  }
}
