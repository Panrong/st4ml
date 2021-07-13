package operatorsNew.converter

import instances.Instance
import org.apache.spark.rdd.RDD

abstract class Converter extends Serializable {
  type I
  type O

  def convert(input: RDD[I]): RDD[O]
}
