package operators.selection.partitioner

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TemporalPartitioner(startTime: Long, endTime: Long, timeInterval: Int, numPartitions: Int) extends Serializable {
  val numSlots: Int = (endTime - startTime).toInt / timeInterval + 1
  val numSlotsPerPartition: Int = numSlots / numPartitions + 1

  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)] = {
    dataRDD.map(x =>
      ((x.timeStamp._1 - startTime).toInt / timeInterval / numSlotsPerPartition, x))
      .filter(_._1 < numPartitions)
      .partitionBy(new KeyPartitioner(numPartitions))
  }

}

