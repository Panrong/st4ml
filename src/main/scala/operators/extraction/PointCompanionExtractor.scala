package operators.extraction

import geometry.Point
import org.apache.spark.rdd.RDD

import scala.math.abs
import operators.selection.partitioner._

class PointCompanionExtractor extends Extractor with Serializable {
  def extract(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point]): Array[(String, String)] = {
    val pairRDD = pRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold &&
          p1.attributes("tripID") != p2.attributes("tripID")
    }.map {
      case (p1, p2) =>
        List(p1.attributes("tripID"), p2.attributes("tripID")).sorted
    }.distinct
      .map(x => (x.head, x(1)))
    pairRDD.collect
  }

  def optimizedExtract(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point]): Array[(String, String)] = {
    val partitioner = new QuadTreePartitioner(pRDD.getNumPartitions, Some(0.5), threshold = sThreshold * 2)
    val repartitionedRDD = partitioner.partition(pRDD)
    repartitionedRDD.mapPartitions(x => {
      val points = x.toArray.map(_._2)
      points.flatMap(x => points.map(y => (x, y))).filter {
        case (p1, p2) => abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold &&
          p1.attributes("tripID") != p2.attributes("tripID")
      }.map {
        case (p1, p2) =>
          List(p1.attributes("tripID"), p2.attributes("tripID")).sorted
      }.map(x => (x.head, x(1))).toIterator
    }).collect.distinct
  }

  def queryWithIDs(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point], queryRDD: RDD[Point]): Map[String, Array[String]] = {
    val partitioner = new QuadTreePartitioner(pRDD.getNumPartitions, Some(0.5), threshold = sThreshold * 2)
    val (repartitionedRDD, repartitionedQueryRDD) = partitioner.copartition(pRDD, queryRDD)
    repartitionedRDD.zipPartitions(repartitionedQueryRDD) {
      (pIterator, qIterator) => {
        val points = pIterator.toArray.map(_._2)
        val queries = qIterator.toArray.map(_._2)
        points.flatMap(x => queries.map(y => (x, y))).filter {
          case (p1, p2) => abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold &&
            p1.attributes("tripID") != p2.attributes("tripID")
        }.map(x => (x._2.attributes("tripID"), x._1.attributes("tripID")))
          .toIterator
      }
    }.mapValues(Array(_))
      .reduceByKey(_++_)
      .collect
      .toMap
  }

  def queryWithIDsFS(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point], queryRDD: RDD[Point]): Map[String, Array[String]] = {
    queryRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold && abs(p1.geoDistance(p2)) <= sThreshold &&
          p1.attributes("tripID") != p2.attributes("tripID")
    }.map{
      case(p,q) => (p.attributes("tripID"), q.attributes("tripID"))
    }.mapValues(Array(_))
      .reduceByKey(_++_)
      .collect
      .toMap
  }
}