package operators.extraction

import geometry.{Point, Trajectory}
import operators.selection.partitioner.{QuadTreePartitioner, STRPartitioner}
import org.apache.spark.rdd.RDD

import scala.math.abs

class TrajCompanionExtractor extends Serializable {
  def isCompanion(sThreshold: Double, tThreshold: Double)(p1: Point, p2: Point): Boolean = {
    abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold &&
      abs(p1.geoDistance(p2)) <= sThreshold
  }

  def queryWithIDsFS(sThreshold: Double, tThreshold: Double)
                    (pRDD: RDD[Trajectory], queryRDD: RDD[Trajectory]): Map[String, Array[String]] = {
    pRDD.cartesian(queryRDD).filter {
      case (p, q) => p.points.flatMap(x => q.points.map(y => (x, y))).exists {
        case (x, y) => isCompanion(sThreshold, tThreshold)(x, y)
      }
    }.map(x => (x._2.tripID, x._1.tripID))
      .mapValues(Array(_))
      .reduceByKey(_ ++ _)
      .collect
      .toMap
  }

  def queryWithIDs(sThreshold: Double, tThreshold: Double)
                  (pRDD: RDD[Trajectory], queryRDD: RDD[Trajectory]): Map[String, Array[String]] = {
    val partitioner = new QuadTreePartitioner(pRDD.getNumPartitions, Some(0.2))
    val (repartitionedRDD, repartitionedQueryRDD) = partitioner.copartition(pRDD, queryRDD)
    repartitionedRDD.zipPartitions(repartitionedQueryRDD) {
      (pIterator, qIterator) => {
        val trajs = pIterator.toArray.map(_._2)
        val queries = qIterator.toArray.map(_._2)
        trajs.flatMap(x => queries.map(y => (x, y))).filter {
          case (p, q) => p.points.flatMap(x => q.points.map(y => (x, y))).exists {
            case (x, y) => isCompanion(sThreshold, tThreshold)(x, y)
          }
        }.map(x => (x._2.tripID, x._1.tripID))
          .toIterator
      }
    }.mapValues(Array(_))
      .reduceByKey(_ ++ _)
      .collect
      .toMap
  }
}
