package operatorsNew.selector

import instances.{Duration, Instance}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon

import scala.reflect.ClassTag

class MultiSTRangeLegacySelector[R <: Instance[_, _, _] : ClassTag](sQuery: Array[Polygon],
                                                                    tQuery: Array[Duration],
                                                                    numPartitions: Int,
                                                                    partition: Boolean = true) extends LegacySelector[R] {
  val partitioner: HashPartitioner = new HashPartitioner(numPartitions)

  assert(sQuery.length == tQuery.length, "The spatial and temporal queries must have the same length.")

  override def query(dataRDD: RDD[R]): RDD[R] = {
    val repartitionedRDD = if(partition) partitioner.partition(dataRDD) else dataRDD
    val queries = sQuery zip tQuery
    repartitionedRDD.filter(o =>
      queries.exists(q => o.intersects(q._1, q._2))
    )
  }

  def queryWithInfo(dataRDD: RDD[R], accurate: Boolean = false): RDD[(R, Array[Int])] = {
    val repartitionedRDD = if(partition) partitioner.partition(dataRDD) else dataRDD
    val queries = sQuery.zip(tQuery).zipWithIndex
    repartitionedRDD.map(o => {
      lazy val geom =  o.toGeometry
      val intersections = if (accurate)
        queries.filter(q =>
        geom.intersects(q._1._1) && o.intersects(q._1._2))
        .map(_._2)
      else queries.filter(q =>
        o.intersects(q._1._1, q._1._2))
        .map(_._2)
      (o, intersections)
    })
      .filter(!_._2.isEmpty)
  }

}
