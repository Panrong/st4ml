package operators.convertion

import org.apache.spark.rdd.RDD

class DoNothingConverter[T] extends Converter {
  override type I = T
  override type O = T

  override def convert(rdd: RDD[T]): RDD[T] = rdd
}
