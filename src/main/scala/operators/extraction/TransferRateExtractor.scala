package operators.extraction

import geometry.{Point, Rectangle}
import operators.repartitioner.TRepartitioner
import org.apache.spark.rdd.RDD

class TransferRateExtractor extends BaseExtractor[Point] {
  def extract(ranges: Map[Int, Rectangle],
              rdd: RDD[Point],
              numPartitions: Int,
              tStart: Long,
              timeInterval: Int): RDD[(Int, Array[(Int, Int, Int)])] = {
    val repartitioner = new TRepartitioner[Point](numPartitions, tStart, timeInterval)
    val rRDD = repartitioner.partition(rdd)
    println(s" ${rRDD.count} point after repartitioning")
    val rddWPartitionID = rRDD.mapPartitionsWithIndex { //(partitionID, (tripID, rangeID))
      case (id, partition) => partition.map(point => {
        val range = ranges.filter(x => point.inside(x._2)).take(1).headOption
        (id, point.attributes("tripID"), range)
      })
        .filter(_._3.isDefined)
        .map(x => (x._1, (x._2, x._3.get)))
        .map(x => (x._1, (x._2._1, x._2._2._1)))
    }
    rddWPartitionID.join(rddWPartitionID).filter {
      case (_, (p1, p2)) => p1._1 == p2._1
    }.map {
      case (pId, (p1, p2)) => ((pId, p1._2, p2._2), 1)
    }.reduceByKey(_ + _)
      .map {
        case ((pId, startRegion, endRegion), num) => (pId, Array((startRegion, endRegion, num)))
      }.reduceByKey(_ ++ _)
  }
}
