package operators.selection.partitioner

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class FastPartitioner(numPartitions: Int) extends SpatialPartitioner {
  var partitionRange: Map[Int, Rectangle] = {
    (0 until numPartitions)
      .zipAll(List(Rectangle(Array(-180, -90, 180, 90))), 0, Rectangle(Array(-180, -90, 180, 90)))
      .toMap
  }

  override var samplingRate: Option[Double] = None

  override def partition[T <: Shape : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val oldNumPartitions = dataRDD.getNumPartitions
    if (oldNumPartitions > numPartitions) dataRDD.coalesce(numPartitions)

    else if (oldNumPartitions < numPartitions) dataRDD.repartition(numPartitions)
    else dataRDD
  }
}

