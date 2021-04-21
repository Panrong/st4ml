package operators.convertion

import geometry.Shape
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DoNothingConverter extends Converter {
  def convert[T <: Shape : ClassTag, O](rdd: RDD[(Int, T)]): RDD[T] = {
    rdd.map(_._2)
  }
}
