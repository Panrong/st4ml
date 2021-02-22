package operators.selection

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class Selector extends Serializable {
  def query[R <: Shape : ClassTag](dataRDD: RDD[R],
                                   sQuery: Rectangle,
                                   tQuery: (Long, Long)): RDD[(Int, R)]
}