package partitioner

import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.MutablePair

/**
 * Linear Hash Partitioner with Java hashcode
 */
class HashPartitioner (num_partitions: Int) extends Partitioner {
  override def numPartitions: Int = num_partitions

  override def getPartition(key: Any): Int = {
    key.hashCode() % num_partitions
  }
}

object HashPartitioner {
  def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Any, InternalRow)], num_partitions: Int): RDD[(Any, InternalRow)] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, row._2.copy()))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Any, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new HashPartitioner(num_partitions)
    new ShuffledRDD[Any, InternalRow, InternalRow](rdd, part)
  }
}
