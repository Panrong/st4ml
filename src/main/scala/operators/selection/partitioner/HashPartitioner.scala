package operators.selection.partitioner

import geometry.Rectangle
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class HashPartitioner(numPartitions: Int) extends SpatialPartitioner with Serializable {
  override var samplingRate: Option[Double] = None

  override def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val pRDD = dataRDD.map(x => (x.id.hashCode.abs % numPartitions, x))
      .partitionBy(partitioner)
    pRDD.map(_._2)
  }

  var partitionRange: Map[Int, Rectangle] =
    (0 until numPartitions)
      .zipAll(List(Rectangle(Array(-180, -90, 180, 90))), 0, Rectangle(Array(-180, -90, 180, 90)))
      .toMap
}
