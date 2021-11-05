package operators.selection.selectionHandler

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD
import operators.selection.indexer.RTreeDeprecated
import org.apache.spark.sql.SparkSession

import scala.math.max
import scala.reflect.ClassTag

case class RTreeHandler(override val partitionRange: Map[Int, Rectangle],
                        var capacity: Option[Int] = None) extends SpatialHandler {

  SparkSession.builder.getOrCreate().sparkContext.getConf.registerKryoClasses(
    Array(classOf[RTreeDeprecated[_]],
      classOf[Rectangle],
      classOf[Shape]))

  override def query[T <: Shape : ClassTag](dataRDD: RDD[T])
                                           (queryRange: Rectangle): RDD[T] = {
    val c = capacity.getOrElse({
      val dataSize = dataRDD.count
      max((dataSize / dataRDD.getNumPartitions / 100).toInt, 100)
    }) // rule for capacity calculation if not given
    val dataRDDWithIndex = dataRDD
      .mapPartitionsWithIndex {
        (index, partitionIterator) => {
          val partitionsMap = scala.collection.mutable.Map[Int, List[T]]()
          var partitionList = List[T]()
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
        val entries = contents.map(x => (x, x.id))
          .zipWithIndex.toArray.map(x => (x._1._1, x._1._2, x._2))
        val rtree = entries.length match {
          case 0 => RTreeDeprecated(entries, 0)
          case _ => RTreeDeprecated(entries, c)
        }
        List((pIndex, rtree)).iterator
      })
    indexedRDD
      .filter {
        case (_, rtree) => rtree.numEntries != 0 && rtree.root.m_mbr.intersect(queryRange)
      }
      .flatMap { case (partitionID, rtree) => rtree.range(queryRange)
        .filter(x => queryRange.referencePoint(x._1).get.inside(partitionRange(partitionID))) // filter by reference point
        .map(x => x._1)
      }
  }
}
