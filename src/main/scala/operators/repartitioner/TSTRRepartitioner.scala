package operators.repartitioner

import geometry.Shape
import operators.selection.partitioner.TemporalPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TSTRRepartitioner[T <: Shape : ClassTag](tPartition: Int, sThreshold: Double, tThreshold: Double, samplingRate: Double)
  extends Repartitioner[T] {
  def repartition(rdd: RDD[T]): RDD[T] = {
    val numPartitions = rdd.getNumPartitions
    val partitioner = new TemporalPartitioner(startTime = rdd.map(_.timeStamp._1).min,
      endTime = rdd.map(_.timeStamp._2).max, numPartitions = numPartitions)
    val res = partitioner.partitionSTR[T](rdd, tPartition, tThreshold, sThreshold, samplingRate)

    //    /** for debug */
    //    val rddWIndex = rdd.zipWithIndex().map(_.swap)
    //    val pointsPerPartition = rddWIndex.mapPartitions(iter => Iterator(iter.length)).collect
    //    println("--- After partitioning:")
    //    println(s"... Number of points per partition: " +
    //      s"${pointsPerPartition.deep}")
    //    println(s"... Total: ${pointsPerPartition.sum}")
    //    println(s"... Distinct: ${rddWIndex.map(x => x._2.id + x._2.timeStamp._1.toString).distinct.count}")

    res
  }
}
