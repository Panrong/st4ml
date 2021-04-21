package operators.convertion

import geometry.{Rectangle, SpatialMap, Trajectory}
import operators.selection.partitioner.TemporalPartitioner
import org.apache.spark.rdd.RDD

class Traj2SpatialMapConverter(startTime: Long,
                               endTime: Long,
                               regions: Map[Int, Rectangle],
                               timeInterval: Option[Int] = None) extends Converter {
  def convert(rdd: RDD[(Int, Trajectory)]): RDD[SpatialMap[Trajectory]] = {
    val numPartitions = if (timeInterval.isEmpty) rdd.getNumPartitions
    else (endTime - startTime).toInt / timeInterval.get + 1
    val duration = timeInterval.getOrElse((endTime - startTime).toInt / numPartitions + 1)
    val timeRanges = (0 until numPartitions).map(x => (startTime + x * duration, startTime + (x + 1) * duration)).toArray
    val partitioner = new TemporalPartitioner(startTime, endTime, numPartitions = numPartitions)
    val repartitionedRDD = partitioner.partitionToMultiple(rdd.map(_._2))
    repartitionedRDD.map(traj => (traj._1, traj._2.windowBy(timeRanges(traj._1))))
      .filter(_._2.isDefined).map { case (id, trajs) => trajs.get.map((id, _)) }
      .flatMap(x => x).mapPartitions(partition => { // now each (sub) trajectory only belongs to one partition
      if (partition.isEmpty) {
        Iterator(SpatialMap[Trajectory]("Empty", (startTime, endTime), new Array[(Rectangle, Array[Trajectory])](0)))
      } else {
        val regionMap = scala.collection.mutable.Map[Rectangle, scala.collection.mutable.ArrayBuffer[Trajectory]]()
        var partitionID = 0
        while (partition.hasNext) {
          val (i, traj) = partition.next()
          val subRegionMap = regions.filter(region => traj.intersect(region._2))
          for ((k, v) <- subRegionMap) {
            regionMap += ((v, if (regionMap.contains(v)) regionMap(v) ++ Array(traj)
            else scala.collection.mutable.ArrayBuffer(traj)))
          }
          partitionID = i
        }
        Iterator(SpatialMap(timeRanges(partitionID).toString,
          timeRanges(partitionID),
          regionMap.mapValues(_.toArray).toArray))
      }
    }).filter(_.id != "Empty")
  }
}
