package operators.convertion

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DoNothingConverter[T: ClassTag] extends Converter {
  override type I = T
  override type O = T

  override def convert(rdd: RDD[(Int, T)]): RDD[T] = {
    rdd.map(_._2)
  }
}
