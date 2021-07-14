package operatorsNew.converter

import org.apache.spark.rdd.RDD

class DoNothingConverter[T] extends Converter {
  override type I = T
  override type O = T

  override def convert(input: RDD[I]): RDD[O] = input
}
