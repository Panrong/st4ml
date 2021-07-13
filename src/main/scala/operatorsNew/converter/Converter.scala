package operatorsNew.converter

import instances.Instance
import org.apache.spark.rdd.RDD

abstract class Converter {
  type I <: Instance[_, _, _]
  type O <: Instance[_, _, _]

  def convert(input: RDD[I]): RDD[O]
}
