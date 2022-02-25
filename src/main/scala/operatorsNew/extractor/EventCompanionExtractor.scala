package operatorsNew.extractor

import instances.{Event, Point}
import operatorsNew.selector.partitioner.TSTRPartitioner
import org.apache.spark.rdd.RDD
import instances.GeometryImplicits._

/**
 * Extract companion relationship from point-shaped events
 *
 * @param sThreshold : the max distance (in meter) of considering companion
 * @param tThreshold : the max time difference (in second) of considering companion
 * @param sPartition : the partition granularity of spatial axis
 * @param tPartition : the partition granularity of temporal axis
 */
class EventCompanionExtractor(sThreshold: Double,
                              tThreshold: Int,
                              sPartition: Int,
                              tPartition: Int) extends Extractor {
  def extract(rdd: RDD[Event[Point, None.type, String]]): RDD[(String, String, Int)] = {
    val partitioner = new TSTRPartitioner(tPartition, sPartition, sThreshold = sThreshold, tThreshold = tThreshold, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd)
    //    println(partitionedRDD.count)
    partitionedRDD.mapPartitions { partition =>
      val events = partition.toArray
      val companion = for {i <- events; j <- events
                           if i.data.hashCode < j.data.hashCode && // remove duplicated comparisons
                             isCompanion(i, j, sThreshold, tThreshold)} yield ((i.data, j.data), Set(i.duration.start))
      companion.toIterator
    }.reduceByKey(_ ++ _)
      .map(x => (x._1._1, x._1._2, x._2.size))
  }

  def isCompanion(a: Event[Point, None.type, String], b: Event[Point, None.type, String],
                  sThreshold: Double, tThreshold: Double): Boolean = {
    if (a.data != b.data &&
      math.abs(a.temporalCenter - b.temporalCenter) <= tThreshold &&
      a.spatialCenter.greatCircle(b.spatialCenter) <= sThreshold) true
    else false
  }
}

object EventCompanionExtractor {
  def apply(sThreshold: Double, tThreshold: Int, parallelism: Int): EventCompanionExtractor = {
    val tPartition = math.pow(parallelism, 1 / 3.0).toInt
    val sPartition = parallelism / tPartition
    new EventCompanionExtractor(sThreshold, tThreshold, sPartition, tPartition)
  }
}