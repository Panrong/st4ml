package operators.convertion

import geometry.{Rectangle, TimeSeries, Trajectory}
import operators.selection.partitioner.SpatialPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Traj2TimeSeriesConverter[T <: SpatialPartitioner : ClassTag](startTime: Long,
                                                                   timeInterval: Int, partitioner: T) extends Converter {
  // partition the whole space into cells, find sub-trajectories inside each cell, grouping as time series
  def convert
  (rdd: RDD[(Int, Trajectory)]): RDD[TimeSeries[Trajectory]] = {
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    val partitionRange = partitioner.partitionRange
    repartitionedRDD.map(x => (x._1, x._2.windowBy(partitionRange(x._1))))
      .filter(_._2.isDefined)
      .map { case (id, trajs) => trajs.get.map((id, _)) }
      .flatMap(x => x)
      .mapPartitions(partition =>
        if (partition.isEmpty) {
          Iterator(TimeSeries[Trajectory]("Empty", startTime, timeInterval, Rectangle(Array(0, 0, 0, 0)), new Array[Array[Trajectory]](0)))
        } else {
          val slotMap = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Trajectory]]()
          var partitionID = 0
          while (partition.hasNext) {
            val (i, traj) = partition.next()
            val slots = ((traj.startTime - startTime) / timeInterval).toInt to
              ((traj.endTime._2 - startTime) / timeInterval).toInt
            for (slot <- slots)
              slotMap += ((slot, if (slotMap.contains(slot)) slotMap(slot) ++ Array(traj)
              else scala.collection.mutable.ArrayBuffer(traj)))
            partitionID = i
          }
          val l = slotMap.keys.max
          val empty = Array.fill[Array[Trajectory]](l)(new Array[Trajectory](0)).zipWithIndex.map(_.swap).toMap // take positions of empty slots
          val slots = (empty ++ slotMap.mapValues(_.toArray)).toArray.sortBy(_._1).map(_._2)

          //        val partitionArray = partition.toArray
          //        val partitionID = partitionArray.head._1
          //        val trajs = partitionArray.map(_._2).filter(_.isDefined).flatMap(_.get)
          //        val l = (trajs.map(_.endTime._2).max - startTime).toInt / timeInterval + 1
          //        val slotsMap = trajs.flatMap(traj => {
          //          val slots = ((traj.startTime - startTime) / timeInterval).toInt to ((traj.endTime._2 - startTime) / timeInterval).toInt
          //          slots.toArray.map((_, traj))
          //        }).groupBy(_._1).mapValues(_.map(_._2))
          //        val empty = Array.fill[Array[Trajectory]](l)(new Array[Trajectory](0)).zipWithIndex.map(_.swap)
          //        val slots = (slotsMap.toArray ++ empty).groupBy(_._1).toArray.sortBy(_._1).map(_._2.head._2)

          val spatialRange = partitioner.partitionRange(partitionID)
          val ts = TimeSeries(partitionID.toString, startTime, timeInterval, spatialRange, slots)
          Iterator(ts)
        }).filter(x => x.id != "Empty")
  }
}
