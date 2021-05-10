package operators.repartitioner

import org.apache.spark.rdd.RDD

abstract class Repartitioner[T] {
  def repartition(rdd: RDD[T]): RDD[T]
}
