package operatorsNew.selector

import instances.{Duration, Extent, Instance}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon

import scala.reflect.ClassTag

class DefaultSelector[R <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
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

