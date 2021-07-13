package operatorsNew.converter

import instances.Instance
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DoNothingConverter[T] extends Converter {
  override type I = T
  override type O = T

  override def convert(input: RDD[I]): RDD[O] = input
}
