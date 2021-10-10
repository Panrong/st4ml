package operatorsNew.selector

import instances.{Duration, Extent, Instance}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon

import scala.language.implicitConversions
import scala.reflect.ClassTag

class MultiSpatialRangeLegacySelector[R <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                                                         tQuery: Duration,
                                                                         numPartitions: Int) extends LegacySelector[R] {

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
        if (!instance.intersects(tQuery)) Array[Int]()
        else {
          sQuery.zipWithIndex
            .filter(range => instance.intersects(range._1)).map(_._2)
        }
      }
      (instance, intersections)
    })
      .filter(x => !x._2.isEmpty)
  }

  def intersects(instance: R, sQuery: Array[Polygon], tQuery: Duration): Boolean = {
    if (instance.intersects(tQuery)) {
      for (range <- sQuery) {
        if (instance.intersects(range)) return true
      }
    }
    false
  }
}