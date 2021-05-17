package operators.repartitioner

import geometry.Shape
import operators.selection.partitioner.KeyPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TRepartitioner[T <: Shape:ClassTag](numPartitions: Int,
                                          startTime: Long,
                                          timeInterval: Int) extends Repartitioner[T] {
  override def partition(rdd: RDD[T]): RDD[T] = {
    rdd.filter(_.timeStamp._1 >= startTime)
      .map(x =>
      ((x.timeStamp._1 - startTime).toInt / timeInterval, x))
      .filter(_._1 < numPartitions)
      .partitionBy(new KeyPartitioner(numPartitions)).map(_._2)
  }
}
