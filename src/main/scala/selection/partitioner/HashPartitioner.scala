package selection.partitioner

import geometry.Rectangle
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class HashPartitioner(numPartitions: Int) extends Serializable {
  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val pRDD = dataRDD.map(x => (x.id.hashCode.abs % numPartitions, x))
      .partitionBy(partitioner)
    pRDD
  }

  var partitionRange: Map[Int, Rectangle] = Map()
  for (i <- 0 until numPartitions) {
    partitionRange = partitionRange + (i -> Rectangle(Array(-180, -90, 180, 90)))
  }

}
