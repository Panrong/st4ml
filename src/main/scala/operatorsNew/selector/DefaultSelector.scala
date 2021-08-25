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
  var partitionedRDD: Option[RDD[R]] = None

  def partition(dataRDD: RDD[R]): RDD[R] = {
    val pRDD = partitioner.partition(dataRDD)
    pRDD.cache()
    partitionedRDD = Some(pRDD)
    pRDD
  }

  override def query(dataRDD: RDD[R]): RDD[R] = {
    val rdd = partitionedRDD.getOrElse(partition(dataRDD))
    rdd.filter(_.intersects(sQuery, tQuery))
  }

}

