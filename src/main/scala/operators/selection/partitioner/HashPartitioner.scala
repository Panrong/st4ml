package operators.selection.partitioner

import geometry.Rectangle
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random
import scala.collection.immutable.HashMap

class HashPartitioner(numPartitions: Int) extends SpatialPartitioner with Serializable {
  override var samplingRate: Option[Double] = None
  val multiplier = math.sqrt(numPartitions).toInt

  def partitionOld[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val pRDD = dataRDD.map(x => {
      val rawMod = x.hashCode % numPartitions
      (rawMod + (if (rawMod < 0) numPartitions else 0), x)
    })
      .partitionBy(partitioner)
    pRDD.map(_._2)
  }

  override def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val mapping = Random.shuffle[Int, IndexedSeq](0 until multiplier * numPartitions)
      .sliding(multiplier, multiplier).toArray.zipWithIndex.flatMap(x => x._1.map(i => (i, x._2))).toMap
    val pRDD = dataRDD.map(x => {
      val rawMod = x.hashCode % (numPartitions * multiplier)
      (mapping(rawMod + (if (rawMod < 0) numPartitions * multiplier else 0)), x)
    })
      .partitionBy(partitioner)
    pRDD.map(_._2)
  }

  var partitionRange: Map[Int, Rectangle] =
    (0 until numPartitions)
      .zipAll(List(Rectangle(Array(-180, -90, 180, 90))), 0, Rectangle(Array(-180, -90, 180, 90)))
      .toMap
}
