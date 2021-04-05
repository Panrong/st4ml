package operators.extraction

import geometry.Point
import org.apache.spark.rdd.RDD

import scala.math.abs
import operators.selection.partitioner._
import org.apache.spark.storage.StorageLevel

class PointCompanionExtractor extends Extractor with Serializable {

  def isCompanion(tThreshold: Double, sThreshold: Double)(p1: Point, p2: Point): Boolean = {
    abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold &&
      abs(p1.geoDistance(p2)) <= sThreshold &&
      p1.attributes("tripID") != p2.attributes("tripID")
  }

  // find all companion pairs
  def extract(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {
    pRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        isCompanion(tThreshold, sThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.id, (p1.timeStamp._1, p2.id))
    }.groupByKey.mapValues(_.toMap).reduceByKey(_ ++ _)
  }

  // find all companion pairs
  def optimizedExtract(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {
    val numPartitions = pRDD.getNumPartitions
    //    // Spatial partitioner
    //    val partitioner = new STRPartitioner(numPartitions, Some(1), threshold = sThreshold * 2)
    //    val repartitionedRDD = partitioner.partition(pRDD)

    // Temporal partitioner
    val partitioner = new TemporalPartitioner(startTime = pRDD.map(_.t).min, endTime = pRDD.map(_.t).max, numPartitions = numPartitions)
    // val repartitionedRDD = partitioner.partitionGrid(pRDD, 2, tOverlap = tThreshold * 2, sOverlap = sThreshold * 2) // temporal + spatial
    val repartitionedRDD = partitioner.partitionWithOverlap(pRDD, tThreshold * 2) // temporal only

    println(s" Number of points per partition: ${repartitionedRDD.mapPartitions(iter => Iterator(iter.length)).collect.deep}")

    repartitionedRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    repartitionedRDD.mapPartitions(x => {
      val points = x.toStream.map(_._2)
      for (p1 <- points;
           p2 <- points
           if isCompanion(tThreshold, sThreshold)(p1, p2)
           ) yield (p1.id, Array((p1.timeStamp._1, p2.id)))
    }.toIterator)
      .mapValues(_.toMap)
      .reduceByKey(_ ++ _, 1000)

    //    val rRDD = repartitionedRDD.map(_._2).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    rRDD.cartesian(rRDD).filter {
    //      case (p1, p2) =>
    //        isCompanion(tThreshold, sThreshold)(p1, p2)
    //    }
    //      .map {
    //        case (p1, p2) => (p1.id, Map((p1.timeStamp._1, p2.id)))
    //      }
    //      .reduceByKey(_ ++ _, 1000)

  }

  //find companion pairs of some queries
  def queryWithIDs(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point], queryRDD: RDD[Point]): Map[String, Array[String]] = {
    val partitioner = new STRPartitioner(pRDD.getNumPartitions, Some(0.5), threshold = sThreshold * 2)
    val (repartitionedRDD, repartitionedQueryRDD) = partitioner.copartition(pRDD, queryRDD)
    repartitionedRDD.zipPartitions(repartitionedQueryRDD) {
      (pIterator, qIterator) => {
        val points = pIterator.toArray.map(_._2)
        val queries = qIterator.toArray.map(_._2)
        points.flatMap(x => queries.map(y => (x, y))).filter {
          case (p1, p2) => isCompanion(tThreshold, sThreshold)(p1, p2)
        }.map(x => (x._2.attributes("tripID"), x._1.attributes("tripID")))
          .toIterator
      }
    }.mapValues(Array(_))
      .reduceByKey(_ ++ _)
      .collect
      .toMap
  }

  def queryWithIDsFS(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point], queryRDD: RDD[Point]): Map[String, Array[String]] = {
    queryRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        isCompanion(tThreshold, sThreshold)(p1, p2)
    }.map {
      case (p, q) => (p.attributes("tripID"), q.attributes("tripID"))
    }.mapValues(Array(_))
      .reduceByKey(_ ++ _)
      .collect
      .toMap
  }
}