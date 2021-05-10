package operators.selection

import geometry.Shape
import org.apache.spark.rdd.RDD

abstract class Selector[R <: Shape] extends Serializable {
  def query(dataRDD: RDD[R]): RDD[R]
}