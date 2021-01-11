package selection.selector

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import geometry.{Rectangle, Shape}
import selection.indexer._

/**
 * Selecting a subset of the original dataset with spatial range constraints.
 *
 * @param dataRDD : input data RDD after partitioning, with type (partitionID, Shape)
 * @param query   : rectangle for querying
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
        x._2.intersect(query)
          && query.referencePoint(x._2).get.inside(partitionRange(x._1))
      )// filter by reference point
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