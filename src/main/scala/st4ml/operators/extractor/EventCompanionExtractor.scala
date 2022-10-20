package st4ml.operators.extractor

import org.apache.spark.rdd.RDD
import st4ml.instances.GeometryImplicits._
import st4ml.instances.{Event, Point}
import st4ml.operators.selector.partitioner.{STR3DPartitioner, TSTRPartitioner}

/**
 * Extract companion relationship from point-shaped events
 *
 * @param sThreshold : the max distance (in meter) of considering companion
 * @param tThreshold : the max time difference (in second) of considering companion
 * @param sPartition : the partition granularity of spatial axis
 * @param tPartition : the partition granularity of temporal axis
 */
class EventCompanionExtractor(sThreshold: Double,
                              tThreshold: Int,
                              sPartition: Int,
                              tPartition: Int) extends Extractor {
  def extract(rdd: RDD[Event[Point, None.type, String]]): RDD[(String, String, Int)] = {
    val partitioner = new TSTRPartitioner(tPartition, sPartition, sThreshold = sThreshold, tThreshold = tThreshold, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd)
    //    println(partitionedRDD.count)
    partitionedRDD.mapPartitions { partition =>
      val events = partition.toArray
      val companion = for {i <- events; j <- events
                           if i.data.hashCode < j.data.hashCode && // remove duplicated comparisons
                             isCompanion(i, j, sThreshold, tThreshold)} yield ((i.data, j.data), Set(i.duration.start))
      companion.toIterator
    }.reduceByKey(_ ++ _)
      .map(x => (x._1._1, x._1._2, x._2.size))
  }

  def extractDetail(rdd: RDD[Event[Point, None.type, String]]): RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new TSTRPartitioner(tPartition, sPartition, sThreshold = sThreshold, tThreshold = tThreshold, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd)
    // println(rdd.count, partitionedRDD.count)
    partitionedRDD.mapPartitions { partition =>
      val events = partition.toArray
      val companion = for {i <- events; j <- events
                           if i.data.hashCode < j.data.hashCode && // remove duplicated comparisons
                             isCompanion(i, j, sThreshold, tThreshold)} yield
        (i.data, i.entries.head.spatial.x, i.entries.head.spatial.y, i.entries.head.temporal.start,
          j.data, j.entries.head.spatial.x, j.entries.head.spatial.y, j.entries.head.temporal.start, i.temporalCenter - j.temporalCenter,
          i.spatialCenter.greatCircle(j.spatialCenter))

      companion.toIterator
    }.distinct
  }

  def extractDetailV2(rdd: RDD[Event[Point, None.type, String]]):
  RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new TSTRPartitioner(tPartition, sPartition,
      sThreshold = sThreshold / 111000 / 2, tThreshold = tThreshold / 2, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partitionWDup(rdd).mapPartitionsWithIndex { case (id, p) => p.map(x => (id, x)) }
    //    println(partitionedRDD.count)
    //    partitionedRDD.map(x => (x._2, x._1)).calPartitionInfo.foreach(println)
    val joinedRDD = partitionedRDD.join(partitionedRDD).filter(x => x._2._1.data.hashCode < x._2._2.data.hashCode && // for (a, b) and (b, a), calculate only once
      isCompanion(x._2._1, x._2._2, sThreshold, tThreshold))
      .map { case (_, (i, j)) =>
        (i.data, i.entries.head.spatial.x, i.entries.head.spatial.y, i.entries.head.temporal.start,
          j.data, j.entries.head.spatial.x, j.entries.head.spatial.y, j.entries.head.temporal.start,
          i.entries.head.temporal.start - j.entries.head.temporal.start,
          i.entries.head.spatial.greatCircle(j.entries.head.spatial))
      }
    val resRDD = joinedRDD//.distinct
    resRDD
  }

  def extractWith2DSTR(rdd: RDD[Event[Point, None.type, String]]):
  RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new TSTRPartitioner(1, tPartition * sPartition,
      sThreshold = sThreshold / 111000 / 2, tThreshold = 0, samplingRate = Some(0.01))
    val partitionedRDD = partitioner.partitionWDup(rdd).mapPartitionsWithIndex { case (id, p) => p.map(x => (id, x)) }
    rdd.unpersist()
    val joinedRDD = partitionedRDD.join(partitionedRDD).filter(x => x._2._1.data.hashCode < x._2._2.data.hashCode && // for (a, b) and (b, a), calculate only once
      isCompanion(x._2._1, x._2._2, sThreshold, tThreshold))
    partitionedRDD.unpersist()
    val resRDD = joinedRDD
      .map { case (_, (i, j)) =>
        (i.data, i.entries.head.spatial.x, i.entries.head.spatial.y, i.entries.head.temporal.start,
          j.data, j.entries.head.spatial.x, j.entries.head.spatial.y, j.entries.head.temporal.start,
          i.entries.head.temporal.start - j.entries.head.temporal.start,
          i.entries.head.spatial.greatCircle(j.entries.head.spatial))
      }
    joinedRDD.unpersist()
    resRDD
  }

  def extractWith3DSTR(rdd: RDD[Event[Point, None.type, String]]):
  RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val partitioner = new STR3DPartitioner(tPartition * sPartition,
      threshold = sThreshold / 111000 / 2, tThreshold = 0, samplingRate = Some(0.2))
    val partitionedRDD = partitioner.partition(rdd).mapPartitionsWithIndex { case (id, p) => p.map(x => (id, x)) }
    //    val s = scala.math.pow(tPartition * sPartition, 1 / 3.0).toInt
    //    val n = s
    //    val t = (tPartition * sPartition / s / n.toDouble).toInt
    //    val partitionedRDD1 = partitioner.partition(rdd).mapPartitionsWithIndex { case (idx, x) =>
    //      val arr = x.toArray.sortBy(_.temporalCenter)
    //      if (arr.length > 0) {
    //        val l = scala.math.ceil(arr.length / t).toInt +1
    //        val groups = arr.sliding(l, l).toArray.map(x => (x.head.temporalCenter - tThreshold / 2, x.last.temporalCenter + tThreshold / 2)).zipWithIndex
    //        val grouped = arr.flatMap(x => groups.filter(g => g._1._1 <= x.temporalCenter && g._1._2 >= x.temporalCenter).map(y => (y._2 + idx* 10000, x)))
    //        grouped.toIterator
    //      }
    //      else Iterator()
    //    }
    //    val keys = partitionedRDD1.map(_._1).distinct.collect().zipWithIndex.toMap
    //    val partitionedRDD = partitionedRDD1.map(x => (keys(x._1), x._2)).partitionBy(new KeyPartitioner(tPartition * sPartition))
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

  def extractNative(rdd: RDD[Event[Point, None.type, String]]): RDD[(String, Double, Double, Long, String, Double, Double, Long, Long, Double)] = {
    val joinedRDD = rdd.cartesian(rdd).map { case (i, j) =>
      (i.data, i.spatialCenter.x, i.spatialCenter.y, i.temporalCenter,
        j.data, j.spatialCenter.x, j.spatialCenter.y, j.temporalCenter, i.temporalCenter - j.temporalCenter,
        i.spatialCenter.greatCircle(j.spatialCenter))
    }.filter(x => x._1.hashCode < x._5.hashCode && math.abs(x._9) <= tThreshold && x._10 <= sThreshold)
    joinedRDD
  }

  def isCompanion(a: Event[Point, None.type, String], b: Event[Point, None.type, String],
                  sThreshold: Double, tThreshold: Double): Boolean = {
    if (math.abs(a.temporalCenter - b.temporalCenter) <= tThreshold &&
      a.spatialCenter.greatCircle(b.spatialCenter) <= sThreshold) true
    else false
  }
}

object EventCompanionExtractor {
  def apply(sThreshold: Double, tThreshold: Int, parallelism: Int): EventCompanionExtractor = {
    val tPartition = math.pow(parallelism, 1 / 2.0).toInt
    val sPartition = parallelism / tPartition
    new EventCompanionExtractor(sThreshold, tThreshold, sPartition, tPartition)
  }
}
