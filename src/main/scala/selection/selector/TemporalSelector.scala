package selection.selector

import geometry.Shape
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Select data within a temporal range
 *
 * @param dataRDD : input data set
 * @param query   : (startTime, endTime)
 */
class TemporalSelector[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)], query: (Long, Long)) extends Serializable {
  def query(): RDD[(Int, T)] = {
    dataRDD.filter(x => {
      val (ts, te) = x._2.timeStamp
      ts <= query._2 && ts >= query._1 || te <= query._2 && te >= query._1
    })
  }
}

/**
 * Select data within a batch of temporal ranges, return selected data with query id attached
 *
 * @param dataRDD : input data set
 * @param query   : Array of (startTime, endTime)
 */
class TemporalBatchSelector[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)], query: Array[(Long, Long)]) extends Serializable {
  def query(): RDD[(Int, T, Int)] = {
    dataRDD.map(x =>
      (x, query
        .zipWithIndex
        .filter(q => {
          val (ts, te) = x._2.timeStamp
          ts <= q._1._2 && ts >= q._1._1 || te <= q._1._2 && te >= q._1._1
        })
        .map(x => x._2)))
      .flatMapValues(x => x)
      .map {
        case (k, v) => (k._1, k._2, v)
      }
  }
}