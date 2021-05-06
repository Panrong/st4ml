package operators.selection

import geometry.Shape
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Selector extends Serializable {
  def query[R <: Shape : ClassTag](dataRDD: RDD[R]): RDD[R]
}