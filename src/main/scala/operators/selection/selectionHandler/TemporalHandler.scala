package operators.selection.selectionHandler

import geometry.Shape
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Select data within a temporal range
 *
 * @param query : (startTime, endTime)
 */
class TemporalSelector() extends Serializable {
  def query[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)])
                                  (query: (Long, Long)): RDD[(Int, T)] =
    dataRDD.filter(x => {
      val (ts, te) = x._2.timeStamp
      ts <= query._2 && te >= query._1
    })
}

/**
 * Select data within a batch of temporal ranges, return selected data with query id attached
 *
 * @param query : Array of (startTime, endTime)
 */
class TemporalBatchSelector() extends Serializable {
  def query[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)])
                                  (query: Array[(Long, Long)]): RDD[(Int, T, Int)] = {
    dataRDD.map(x =>
      (x, query
        .zipWithIndex
        .filter(q => {
          val (ts, te) = x._2.timeStamp
          ts <= q._1._2 && te >= q._1._1
        })
        .map(x => x._2)))
      .flatMapValues(x => x)
      .map {
        case (k, v) => (k._1, k._2, v)
      }
  }
}