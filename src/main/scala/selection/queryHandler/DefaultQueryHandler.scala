package selection.queryHandler

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD
import selection.partitioner.HashPartitioner
import selection.selector.{RTreeSelector, TemporalSelector}

import scala.reflect.ClassTag

class DefaultQueryHandler
(partitioner: HashPartitioner,
 var spatialSelector: RTreeSelector,
 temporalSelector: TemporalSelector) extends QueryHandler {
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

object DefaultQueryHandler {
  def apply(numPartitions: Int): DefaultQueryHandler = {
    val partitioner = new HashPartitioner(numPartitions)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = RTreeSelector(partitionRange)
    val temporalSelector = new TemporalSelector()
    new DefaultQueryHandler(partitioner, spatialSelector, temporalSelector)
  }

}