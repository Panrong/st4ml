package operators.selection.partitioner

import org.apache.spark.Partitioner

class KeyPartitioner(num: Int) extends Partitioner with Serializable {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.asInstanceOf[Int]
  }
}

