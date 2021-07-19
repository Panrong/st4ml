package operatorsNew.selector

import instances.{Duration, Instance}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon

import scala.reflect.ClassTag

class MultiTemporalRangeSelector[R <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                                    tQuery: Array[Duration],
                                                                    numPartitions: Int) extends Selector[R] {
  val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

  override def query(dataRDD: RDD[R]): RDD[R] = {
    val repartitionedRDD = partitioner.partition(dataRDD)
    repartitionedRDD.filter(o =>
      intersects(o, sQuery, tQuery)
    )
  }

  def queryWithInfo(dataRDD: RDD[R]): RDD[(R, Array[Int])] = {
    val repartitionedRDD = partitioner.partition(dataRDD)
    repartitionedRDD.map(instance => {
      val intersections = {
        if (!instance.intersects(sQuery)) Array[Int]()
        else {
          tQuery.zipWithIndex
            .filter(range => instance.intersects(range._1)).map(_._2)
        }
      }
      (instance, intersections)
    })
      .filter(x => !x._2.isEmpty)
  }

  def intersects(instance: R, sQuery: Polygon, tQuery: Array[Duration]): Boolean = {
    if (instance.intersects(sQuery)) {
      for (range <- tQuery) {
        if (instance.intersects(range)) return true
      }
    }
    false
  }
}