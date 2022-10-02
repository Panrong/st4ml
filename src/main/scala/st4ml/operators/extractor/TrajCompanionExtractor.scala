package st4ml.operators.extractor

import org.apache.spark.rdd.RDD
import st4ml.instances.GeometryImplicits._
import st4ml.instances.Trajectory
import st4ml.operators.selector.partitioner.TSTRPartitioner

class TrajCompanionExtractor(sThreshold: Double,
                             tThreshold: Int,
                             sPartition: Int,
                             tPartition: Int) extends Extractor {
  def extractDetailV2(rdd: RDD[Trajectory[None.type, String]]):
  RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new TSTRPartitioner(tPartition, sPartition,
      sThreshold = sThreshold / 111000 / 2, tThreshold = tThreshold / 2, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd).mapPartitionsWithIndex { case (id, p) => p.map(x => (id, x)) }
    val joinedRDD = partitionedRDD.join(partitionedRDD).filter(x => x._2._1.data.hashCode < x._2._2.data.hashCode && // for (a, b) and (b, a), calculate only once
      isCompanion(x._2._1, x._2._2, sThreshold, tThreshold))
      .map { case (_, (i, j)) =>
        (i.data, i.entries.head.spatial.x, i.entries.head.spatial.y, i.entries.head.temporal.start,
          j.data, j.entries.head.spatial.x, j.entries.head.spatial.y, j.entries.head.temporal.start,
          i.entries.head.temporal.start - j.entries.head.temporal.start,
          i.entries.head.spatial.greatCircle(j.entries.head.spatial))
      }
    val resRDD = joinedRDD.distinct
    resRDD
  }

  def extractWith2DSTR(rdd: RDD[Trajectory[None.type, String]]):
  RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new TSTRPartitioner(1, tPartition * sPartition,
      sThreshold = sThreshold / 111000 / 2, tThreshold = 0, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd).mapPartitionsWithIndex { case (id, p) => p.map(x => (id, x)) }
    val joinedRDD = partitionedRDD.join(partitionedRDD).filter(x => x._2._1.data.hashCode < x._2._2.data.hashCode && // for (a, b) and (b, a), calculate only once
      isCompanion(x._2._1, x._2._2, sThreshold, tThreshold))
      .map { case (_, (i, j)) =>
        (i.data, i.entries.head.spatial.x, i.entries.head.spatial.y, i.entries.head.temporal.start,
          j.data, j.entries.head.spatial.x, j.entries.head.spatial.y, j.entries.head.temporal.start,
          i.entries.head.temporal.start - j.entries.head.temporal.start,
          i.entries.head.spatial.greatCircle(j.entries.head.spatial))
      }
    val resRDD = joinedRDD.distinct
    resRDD
  }

  def isCompanion(a: Trajectory[None.type, String], b: Trajectory[None.type, String],
                  sThreshold: Double, tThreshold: Double): Boolean = {
    if (math.abs(a.temporalCenter - b.temporalCenter) <= tThreshold &&
      a.spatialCenter.greatCircle(b.spatialCenter) <= sThreshold) true
    else false
  }
}

object TrajCompanionExtractor {
  def apply(sThreshold: Double, tThreshold: Int, parallelism: Int): TrajCompanionExtractor = {
    val tPartition = math.pow(parallelism, 1 / 2.0).toInt
    val sPartition = parallelism / tPartition
    new TrajCompanionExtractor(sThreshold, tThreshold, sPartition, tPartition)
  }
}