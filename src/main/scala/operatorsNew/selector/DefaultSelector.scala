package operatorsNew.selector

import instances.{Duration, Extent, Instance}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.rdd.RDD

class DefaultSelector[R <: Instance[_, _, _]](sQuery: Extent,
                                              tQuery: Duration,
                                              numPartitions: Int) extends Selector[R] {
  val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

  override def query(dataRDD: RDD[R]): RDD[R] = {
    val repartitionedRDD = partitioner.partition(dataRDD)
    repartitionedRDD.filter {
      o => o.intersects(sQuery, tQuery)
    }
  }
}
