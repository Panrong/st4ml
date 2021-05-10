package operators.repartitioner

import org.apache.spark.rdd.RDD

abstract class Repartitioner[T] {
  def partition(rdd: RDD[T]): RDD[T]
}
