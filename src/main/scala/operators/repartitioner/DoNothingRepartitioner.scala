package operators.repartitioner

import org.apache.spark.rdd.RDD

class DoNothingRepartitioner[T] extends Repartitioner[T] {
  override def repartition(rdd: RDD[T]): RDD[T] = rdd
}
