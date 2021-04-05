package operators.selection.partitioner

import geometry.{Rectangle, Shape}
import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Partition a ST RDD along its temporal axis
 *
 * @param startTime     : the startTime of the RDD
 *                      (can be the exact smallest value or the point of time that you want to start at)
 * @param endTime       : the endTime of the RDD
 *                      (can be the exact biggest value or the point of time that you want to stop at)
 * @param timeInterval  : the "resolution" of the partition: from the start,
 *                      sliding by timeInterval and partition by the subset
 *                      (in order to convert to TimeSeries without redundancy or missing).
 *                      If no requirement, then by default set to 1.
 * @param numPartitions : number of partitions
 */
class TemporalPartitioner(startTime: Long,
                          endTime: Long,
                          timeInterval: Int = 1, numPartitions: Int) extends Serializable {
  val numSlots: Int = (endTime - startTime).toInt / timeInterval + 1
  val numSlotsPerPartition: Int = numSlots / numPartitions + 1

  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)] = {
    dataRDD.map(x =>
      ((x.timeStamp._1 - startTime).toInt / timeInterval / numSlotsPerPartition, x))
      .filter(_._1 < numPartitions)
      .partitionBy(new KeyPartitioner(numPartitions))
  }

  //timeInterval is ignored
  def partitionWithOverlap[T <: geometry.Shape : ClassTag]
  (dataRDD: RDD[T], overlap: Double = 0, tPartition: Int = numPartitions): RDD[(Int, T)] = {
    val rangeLength = (endTime - startTime) / tPartition + 1
    val temporalRanges = (0 until tPartition)
      .map(t => ((startTime + t * rangeLength - overlap).toLong,
        (startTime + (t + 1) * rangeLength + overlap).toLong)).toArray
    dataRDD.map(x => allocateTemporalPartitions(x.timeStamp._1, temporalRanges).map((_, x)))
      .flatMap(x => x)
      .partitionBy(new KeyPartitioner(numPartitions))
  }

  def partitionGrid[T <: geometry.Shape : ClassTag](dataRDD: RDD[T],
                                                    gridSize: Int,
                                                    tOverlap: Double = 0,
                                                    sOverlap: Double = 0,
                                                    spatialRange: Option[Array[Double]] = None): RDD[(Int, T)] = {
    val sRange = spatialRange.getOrElse {
      val coordinatesRDD = dataRDD.map(_.mbr.coordinates)
      Array(coordinatesRDD.map(x => x(0)).min,
        coordinatesRDD.map(x => x(1)).min,
        coordinatesRDD.map(x => x(2)).max, coordinatesRDD.map(x => x(3)).max)
    }
    val sPartitions = gridPartition(sRange, gridSize)
    val tPartition = numPartitions / gridSize / gridSize
    assert(tPartition >= 2, "the square of gridSize should be less than half of numPartitions")
    val tPartitionedRDD = partitionWithOverlap(dataRDD, tOverlap, tPartition)
    tPartitionedRDD.map {
      case (tId, x) => {
        allocateSpatialPartitions(x, sPartitions, sOverlap)
          .map(sId => (tId * gridSize * gridSize + sId, x))
      }
    }.flatMap(x => x)
      .partitionBy(new KeyPartitioner(numPartitions))
  }

  def partitionSTR[T <: geometry.Shape : ClassTag](dataRDD: RDD[T],
                                                   tPartition: Int,
                                                   tOverlap: Double = 0,
                                                   sOverlap: Double = 0,
                                                   samplingRate: Double = 0.5): RDD[(Int, T)] = {

    val rangeLength = (endTime - startTime) / tPartition + 1
    val temporalRanges = (0 until tPartition)
      .map(t => ((startTime + t * rangeLength - tOverlap).toLong,
        (startTime + (t + 1) * rangeLength + tOverlap).toLong)).toArray.zipWithIndex

    val samplePoints = dataRDD.take((samplingRate * dataRDD.count).toInt)
    var temporalPartitionedSamples = temporalRanges.map((_, new Array[T](0))).toMap
    for (s <- samplePoints) {
      for (k <- temporalPartitionedSamples.keys) {
        if ((s.timeStamp._1 >= k._1._1 && s.timeStamp._1 <= k._1._2)
          || (s.timeStamp._2 >= k._1._1 && s.timeStamp._2 <= k._1._2)) {
          temporalPartitionedSamples = temporalPartitionedSamples + (k -> (temporalPartitionedSamples(k) :+ s))
        }
      }
    }
    val stRanges = temporalPartitionedSamples.mapValues(points => str(points, samplingRate)).toArray.flatMap {
      case (t, s) => s.map(x => (x._1 + t._2 * temporalRanges.length, t._1, x._2))
    }
    allocateSTPartitions(dataRDD,stRanges,sOverlap)
  }

  def gridPartition(sRange: Array[Double], gridSize: Int): Array[Rectangle] = {
    val longInterval = (sRange(2) - sRange(0)) / gridSize
    val latInterval = (sRange(3) - sRange(1)) / gridSize
    val longSeparations = (0 until gridSize)
      .map(t => (sRange(0) + t * longInterval, sRange(0) + (t + 1) * longInterval)).toArray
    val latSeparations = (0 until gridSize)
      .map(t => (sRange(1) + t * latInterval, sRange(1) + (t + 1) * latInterval)).toArray
    for ((longMin, longMax) <- longSeparations;
         (latMin, latMax) <- latSeparations)
      yield Rectangle(Array(longMin, latMin, longMax, latMax))
  }

  def allocateTemporalPartitions(t: Long, ranges: Array[(Long, Long)]): Array[Int] = {
    ranges.zipWithIndex.filter {
      case ((s, e), _) => t >= s && t <= e
    }.map(_._2)
  }

  def allocateSpatialPartitions[T <: Shape : ClassTag](t: T, ranges: Array[Rectangle],
                                                       sOverlap: Double): Array[Int] = {
    ranges.zipWithIndex.filter {
      case (r, _) => t.intersect(r.dilate(sOverlap))
    }.map(_._2)
  }

  def str[T <: Shape : ClassTag](contents: Array[T], samplingRate: Double): Array[(Int, Rectangle)] = {
    val sc = spark.SparkContext.getOrCreate()
    val rdd = sc.parallelize(contents)
    val partitioner = new STRPartitioner(numPartitions, samplingRate = Some(samplingRate))
    partitioner.getPartitionRange(rdd).toArray
  }

  def allocateSTPartitions[T <: Shape : ClassTag](rdd: RDD[T],
                                                  ranges: Array[(Int, (Long, Long), Rectangle)],
                                                  sOverlap: Double): RDD[(Int, T)] = {
    rdd.flatMap(x => ranges.filter {
      case (_, tRange, sRange) => (
        ((x.timeStamp._1 >= tRange._1 && x.timeStamp._1 <= tRange._2)
          || (x.timeStamp._2 >= tRange._1 && x.timeStamp._2 <= tRange._2))
          && x.mbr.dilate(sOverlap).intersect(sRange))
    }.map(i => (i._1, x)))
  }
}

