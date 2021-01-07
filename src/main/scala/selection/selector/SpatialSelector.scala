package selection.selector

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.io.Source
import scala.reflect.ClassTag

import geometry.{Rectangle, Shape}

import selection.indexer._
import selection.partitioner._

/**
 * Selecting a subset of the original dataset with spatial range constraints.
 * @param dataRDD : input data RDD after partitioning, with type (partitionID, Shape)
 * @param query : rectangle for querying
 * @tparam T : the type of input RDD should extend Shape
 */
class SpatialSelector[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)], query: Rectangle) extends Serializable {
  /**
   * default query by scanning each element in datRDD
   *
   * @return : queried RDD
   */
  def query(partitionRange: Map[Int, Rectangle]): RDD[(Int, T)] = {
    val spark = SparkContext.getOrCreate()
    spark.broadcast(query)
    dataRDD
      .filter(x =>
        x._2.intersect(query) &&
          query.referencePoint(x._2).get.inside(partitionRange(x._1))) // filter by reference point
      .distinct
  }

  /**
   * query using RTree indexing as optimization
   *
   * @param capacity       : RTree leaf capacity
   * @param partitionRange : map recording the boundary of each partition (partitionID -> range)
   * @return : queried RDD
   */
  def queryWithRTreeIndex(capacity: Int, partitionRange: Map[Int, Rectangle]): RDD[(Int, T)] = {
    val dataRDDWithIndex = dataRDD
      .map(x => x._2)
      .mapPartitionsWithIndex {
        (index, partitionIterator) => {
          val partitionsMap = scala.collection.mutable.Map[Int, List[Shape]]()
          var partitionList = List[Shape]()
          while (partitionIterator.hasNext) {
            partitionList = partitionIterator.next() :: partitionList
          }
          partitionsMap(index) = partitionList
          partitionsMap.iterator
        }
      }
    val indexedRDD = dataRDDWithIndex
      .mapPartitions(x => {
        val (pIndex, contents) = x.next
        val entries = contents.map(x => (x.mbr, x.id))
          .zipWithIndex.toArray.map(x => (x._1._1, x._1._2.toInt, x._2))
        val rtree = RTree(entries, capacity)
        List((pIndex, rtree)).iterator
      })
    indexedRDD
      .flatMap { case (partitionID, rtree) => rtree.range(query)
        .filter(x => query.referencePoint(x._1).get
          .inside(partitionRange(partitionID))) // filter by reference point
        .map(x => (partitionID, x._1.asInstanceOf[T]))
      }
  }
}

object ScannerTest extends App {
  override def main(args: Array[String]): Unit = {
    import preprocessing.ReadTrajFile
    import java.lang.System.nanoTime
    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    val spark = SparkSession.builder().master(config("master")).appName(config("appName")).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = args(0)
    val numPartitions = args(2).toInt
    val samplingRate = args(3).toDouble
    val rtreeCapacity = args(4).toInt
    val dataSize = args(5).toInt
    val trajDS: Dataset[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize)
    val trajRDD = trajDS.rdd.map(x => x.mbr)
    val query = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))

    println(s"\nOriginal dataset contains ${trajRDD.count} entries")

    /**
     * Usage of selector (+ indexer + partitioner)
     */

    /** partition */
    var t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions, samplingRate)
    val pRDD = partitioner.partition(trajRDD, numPartitions, samplingRate)
    val partitionRange = partitioner.partitionRange
    val selector = new SpatialSelector(pRDD, query)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** query by filtering */
    t = nanoTime()
    val queriedRDD1 = selector.query(partitionRange)
    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** query with index */
    t = nanoTime()
    val queriedRDD2 = selector.queryWithRTreeIndex(rtreeCapacity, partitionRange)
    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")
  }
}