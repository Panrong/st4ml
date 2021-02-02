package selection.partitioner
import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class QuadTreePartitioner(numPartitions: Int, override var samplingRate: Option[Double] = None, threshold: Double = 0)
  extends SpatialPartitioner {
  override var partitionRange: Map[Int, Rectangle] = _

  override def partition[T <: Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)] = ???
}
