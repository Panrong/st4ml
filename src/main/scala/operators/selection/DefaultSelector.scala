package operators.selection

import geometry.{Rectangle, Shape}
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DefaultSelector
(partitioner: HashPartitioner,
 var spatialSelector: RTreeHandler,
 temporalSelector: TemporalSelector) extends Selector {
  override def query[R <: Shape : ClassTag](dataRDD: RDD[R],
                                            sQuery: Rectangle,
                                            tQuery: (Long, Long)): RDD[(Int, R)] = {
    val pRDD = partitioner.partition(dataRDD)
    spatialSelector = spatialSelector.copy(partitionRange = partitioner.partitionRange)
    temporalSelector.query(
      spatialSelector.query(pRDD)(sQuery)
    )(tQuery)
  }
}

object DefaultSelector {
  def apply(numPartitions: Int): DefaultSelector = {
    val partitioner = new HashPartitioner(numPartitions)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = RTreeHandler(partitionRange)
    val temporalSelector = new TemporalSelector()
    new DefaultSelector(partitioner, spatialSelector, temporalSelector)
  }

}