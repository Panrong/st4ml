package selection.partitioner

import org.apache.spark.Partitioner

class KeyPartitioner(num: Int) extends Partitioner with Serializable {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int =
    key.asInstanceOf[Int]
}

