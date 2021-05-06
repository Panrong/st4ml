package operators.selection

import geometry.{Rectangle, Shape}
import operators.selection.partitioner._
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DefaultSelectorOld
(partitioner: HashPartitioner, var spatialSelector: RTreeHandler, temporalSelector: TemporalSelector)
(sQuery: Rectangle, tQuery: (Long, Long)) extends Selector {
  override def query[R <: Shape : ClassTag](dataRDD: RDD[R]): RDD[R] = {
    val pRDD = partitioner.partition(dataRDD)
    spatialSelector = spatialSelector.copy(partitionRange = partitioner.partitionRange)
    temporalSelector.query(
      spatialSelector.query(pRDD)(sQuery)
    )(tQuery)
  }
}

object DefaultSelectorOld {
  def apply(numPartitions: Int, sQuery: Rectangle, tQuery: (Long, Long)): DefaultSelectorOld = {
    val partitioner = new HashPartitioner(numPartitions)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = RTreeHandler(partitionRange)
    val temporalSelector = new TemporalSelector()
    new DefaultSelectorOld(partitioner, spatialSelector, temporalSelector)(sQuery,tQuery)
  }
}