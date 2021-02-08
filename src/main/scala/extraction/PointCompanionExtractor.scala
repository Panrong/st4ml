package extraction

import geometry.Point
import org.apache.spark.rdd.RDD
import scala.math.abs
import selection.partitioner.STRPartitioner

class PointCompanionExtractor(sThreshold: Double, tThreshold: Double) extends Serializable {
  def extract(pRDD: RDD[Point]): Array[(String, String)] = {
    val pairRDD = pRDD.cartesian(pRDD).filter{
      case (p1, p2) =>
        abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold && p1.id != p2.id
    }.map{
      case(p1, p2) =>
        List(p1.id, p2.id).sorted
    }.distinct
      .map(x => (x.head, x(1)))
    pairRDD.collect
  }
  def optimizedExtract(pRDD: RDD[Point]): Array[(String, String)] = {
    val partitioner = new STRPartitioner(pRDD.getNumPartitions, Some(0.5), threshold = sThreshold * 2)
    val repartitionedRDD = partitioner.partition(pRDD)

    repartitionedRDD.mapPartitions(x => {
      val points = x.toArray.map(_._2)
      points.flatMap(x => points.map(y => (x, y))).filter{
        case (p1, p2) => abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold && p1.id != p2.id
      }.map {
        case (p1, p2) =>
          List(p1.id, p2.id).sorted
      }.map(x => (x.head, x(1))).toIterator
    }).collect.distinct
  }
}