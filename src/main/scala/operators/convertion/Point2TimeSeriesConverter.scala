package operators.convertion

import geometry.{Point, Rectangle, TimeSeries}
import operators.selection.partitioner.SpatialPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Convert points to timeSeries, where the dataset is spatially partitioned
 * and each partition consists of one time series.
 *
 * @param startTime    : start time of all time series
 * @param timeInterval : the length for temporal slicing
 * @param partitioner  : extends spatial partitioner
 * @tparam T : type of partitioner
 * @return : rdd of timeSeries
 */
class Point2TimeSeriesConverter[T <: SpatialPartitioner : ClassTag](startTime: Long,
                                                                    timeInterval: Int,
                                                                    partitioner: T) extends Converter {

  override type I = Point
  override type O = TimeSeries[Point]

  override def convert(rdd: RDD[Point])
  : RDD[TimeSeries[Point]] = {
    val repartitionedRDD = partitioner.partition(rdd).mapPartitionsWithIndex((id, p) => p.map((id, _)))
    val pointsPerPartition = repartitionedRDD.mapPartitions(iter => Iterator(iter.length)).collect
    println(s"... Number of points per partition: " + s"${pointsPerPartition.deep}")
    repartitionedRDD.mapPartitions(partition =>
      if (partition.isEmpty) {
        Iterator(TimeSeries[Point]("Empty", startTime, timeInterval, Rectangle(Array(0, 0, 0, 0)), new Array[Array[Point]](0)))
      } else {
        val slotMap = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Point]]()
        var partitionID = 0
        while (partition.hasNext) {
          val (i, point) = partition.next()
          val slot = ((point.timeStamp._1 - startTime) / timeInterval).toInt
          slotMap += ((slot, if (slotMap.contains(slot)) slotMap(slot) ++ Array(point) else scala.collection.mutable.ArrayBuffer(point)))
          partitionID = i
        }
        val l = slotMap.keys.max
        val empty = Array.fill[Array[Point]](l)(new Array[Point](0)).zipWithIndex.map(_.swap).toMap // take positions of empty slots
        val slots = (empty ++ slotMap.mapValues(_.toArray)).toArray.sortBy(_._1).map(_._2)

        //        /** takes more memory */
        //        val partitionArray = partition.toArray
        //        val partitionID = partitionArray.head._1
        //        val points = partitionArray.map(_._2)
        //        val l = (points.map(_.t).max.toInt - startTime).toInt / timeInterval + 1
        //        val slotsMap = points.map(p => (((p.t - startTime) / timeInterval).toInt, p)).groupBy(_._1).mapValues(_.map(_._2))
        //        val empty = Array.fill[Array[Point]](l)(new Array[Point](0)).zipWithIndex.map(_.swap)
        //        val slots = (slotsMap.toArray ++ empty).groupBy(_._1).toArray.sortBy(_._1).map(_._2.head._2)

        val spatialRange = partitioner.partitionRange(partitionID)
        val ts = TimeSeries(partitionID.toString, startTime, timeInterval, spatialRange, slots)
        Iterator(ts)
      }).filter(x => x.id != "Empty")
  }
}
