package operators.convertion

import org.apache.spark.rdd.RDD

abstract class Converter extends Serializable {
  type I
  type O
  def convert(rdd: RDD[(Int, I)]): RDD[O]
}
