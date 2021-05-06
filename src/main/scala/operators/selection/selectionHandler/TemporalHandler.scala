package operators.selection.selectionHandler

import geometry.Shape
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Select data within a temporal range
 */
class TemporalSelector() extends Serializable {
  def query[T <: Shape : ClassTag](dataRDD: RDD[T])
                                  (query: (Long, Long)): RDD[T] =
    dataRDD.filter(x => {
      val (ts, te) = x.timeStamp
      (ts <= query._2 && ts >= query._1) || (te <= query._2 && te >= query._1)
    })
}

/**
 * Select data within a batch of temporal ranges, return selected data with query id attached
 */
class TemporalBatchSelector() extends Serializable {
  def query[T <: Shape : ClassTag](dataRDD: RDD[T])
                                  (query: Array[(Long, Long)]): RDD[(T, Int)] = {
    dataRDD.map(x =>
      (x, query
        .zipWithIndex
        .filter(q => {
          val (ts, te) = x.timeStamp
          (ts <= q._1._2 && ts >= q._1._1) || (te <= q._1._2 && te >= q._1._1)
        })
        .map(x => x._2)))
      .flatMapValues(x => x)
  }
}