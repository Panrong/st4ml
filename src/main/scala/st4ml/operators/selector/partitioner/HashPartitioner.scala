package st4ml.operators.selector.partitioner

import st4ml.instances.{Extent, Instance}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

class HashPartitioner(override val numPartitions: Int) extends STPartitioner with Serializable {
  override var samplingRate: Option[Double] = None
  val multiplier = 16

  override def partition[T <: Instance[_,_,_] : ClassTag](dataRDD: RDD[T]):
  RDD[T] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val mapping = Random.shuffle[Int, IndexedSeq](0 until multiplier * numPartitions)
      .sliding(multiplier, multiplier).toArray
      .zipWithIndex.flatMap(x => x._1.map(i => (i, x._2))).toMap
    val pRDD = dataRDD.map(x => {
      val rawMod = x.hashCode % (numPartitions * multiplier)
      (mapping(rawMod + (if (rawMod < 0) numPartitions * multiplier else 0)), x)
    })
      .partitionBy(partitioner)
    pRDD.map(_._2)
  }

  override def partitionWDup[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] =
    partition(dataRDD)

  var partitionRange: Map[Int, Extent] =
    (0 until numPartitions)
      .map(x => (x, new Extent(-180, -90, 180, 90)))
      .toMap

}