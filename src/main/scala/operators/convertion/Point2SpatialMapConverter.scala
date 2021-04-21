package operators.convertion

import geometry.{Point, Rectangle, SpatialMap}
import operators.selection.partitioner.TemporalPartitioner
import org.apache.spark.rdd.RDD

import scala.math.{max, min}

class Point2SpatialMapConverter(startTime: Long,
                                endTime: Long,
                                regions: Map[Int, Rectangle],
                                timeInterval: Option[Int] = None) extends Converter {
  override type I = Point
  override type O = SpatialMap[Point]

  override def convert(rdd: RDD[(Int, Point)]): RDD[SpatialMap[Point]] = {
    val numPartitions = if (timeInterval.isEmpty) rdd.getNumPartitions
    else (endTime - startTime).toInt / timeInterval.get + 1
    val partitioner = new TemporalPartitioner(startTime, endTime, numPartitions = numPartitions)
    val duration = timeInterval.getOrElse((endTime - startTime).toInt / numPartitions + 1)
    val repartitionedRDD = partitioner.partition(rdd.map(_._2))
    val boundary = genBoundary(regions)
    repartitionedRDD.mapPartitionsWithIndex {
      case (_, partition) =>
        if (partition.isEmpty) {
          Iterator(SpatialMap[Point]("Empty", (startTime, endTime), new Array[(Rectangle, Array[Point])](0)))
        } else {
          val regionMap = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Point]]()
          var partitionID = 0
          while (partition.hasNext) {
            val (i, point) = partition.next()
            val pointShrink = Point(Array(
              min(max(point.x, boundary.head), boundary(2)),
              min(max(point.y, boundary(1)), boundary(3))))
            val regionID = regions.filter { case (_, r) => pointShrink.inside(r) }.head._1
            regionMap += ((regionID, if (regionMap.contains(regionID)) regionMap(regionID) ++ Array(point)
            else scala.collection.mutable.ArrayBuffer(point)))
            partitionID = i
          }
          val regionContents = regionMap.toArray.sortBy(_._1).map(x => (regions(x._1), x._2.toArray))
          Iterator(SpatialMap(id = partitionID.toString,
            timeStamp = (startTime + partitionID * duration, startTime + (partitionID + 1) * duration),
            contents = regionContents))
        }
    }.filter(_.id != "Empty")
  }

  def genBoundary(partitionMap: Map[Int, Rectangle]): Array[Double] = {
    val boxes = partitionMap.values.map(_.coordinates)
    val minLon = boxes.map(_.head).min
    val minLat = boxes.map(_ (1)).min
    val maxLon = boxes.map(_ (2)).max
    val maxLat = boxes.map(_.last).max
    Array(minLon, minLat, maxLon, maxLat)
  }
}
