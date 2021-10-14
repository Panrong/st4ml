package operatorsNew.selector.partitioner

import instances.{Duration, Instance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class TemporalPartitioner(override val numPartitions: Int,
                          override var samplingRate: Option[Double] = None,
                          ref: String = "start") extends STPartitioner {

  var slots = new Array[Duration](0)
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def partition[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => (x.duration.start, x))
      case "center" => dataRDD.map(x => (x.temporalCenter, x))
      case "end" => dataRDD.map(x => (x.duration.end, x))
    }
    import spark.implicits._
    val tDf = rdd.sample(withReplacement = false, sr).map(_._1).toDF
    val wholeTRange = (rdd.map(_._1).min, rdd.map(_._1).max)
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val tRanges = tDf.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong, x(1).toLong)).zipWithIndex
    val tRangesExtended = (Duration(wholeTRange._1, tRanges(0)._1.end), 0) +:
      tRanges.drop(1).dropRight(1) :+
      (Duration(tRanges.last._1.start, wholeTRange._2), tRanges.last._2)
    slots = tRangesExtended.map(_._1)
    val rddWIdx = rdd.map(instance => {
      val overlap = tRangesExtended.filter(x => x._1.intersects(instance._1)).head._2
      (overlap, instance._2)
    })
    val partitioner = new KeyPartitioner(numPartitions)
    rddWIdx.partitionBy(partitioner)
      .map(_._2)
  }

  def partitionWDup[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => (x.duration.start, x))
      case "center" => dataRDD.map(x => (x.temporalCenter, x))
      case "end" => dataRDD.map(x => (x.duration.end, x))
    }
    import spark.implicits._
    val tDf = rdd.sample(withReplacement = false, sr).map(_._1).toDF
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val wholeTRange = (rdd.map(_._1).min, rdd.map(_._1).max)
    val tRanges = tDf.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong, x(1).toLong)).zipWithIndex
    val tRangesExtended = (Duration(wholeTRange._1, tRanges(0)._1.end), 0) +:
      tRanges.drop(1).dropRight(1) :+
      (Duration(tRanges.last._1.start, wholeTRange._2), tRanges.last._2)
    slots = tRangesExtended.map(_._1)
    rdd.flatMap(instance => {
      val overlap = tRangesExtended.filter(x => x._1.intersects(instance._1)).map(_._2)
      overlap.map(x => (x, instance))
    }).partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2._2)
  }

  def partitionWithOverlap[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T],
                                                              overlap: Long): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => (x.duration.start, x))
      case "center" => dataRDD.map(x => (x.temporalCenter, x))
      case "end" => dataRDD.map(x => (x.duration.end, x))
    }
    import spark.implicits._
    val tDf = rdd.sample(withReplacement = false, sr).map(_._1).toDF
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val tRanges = tDf.stat.approxQuantile("_c0", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong - overlap, x(1).toLong + overlap)).zipWithIndex
    val wholeTRange = (rdd.map(_._1).min, rdd.map(_._1).max)
    val tRangesExtended = (Duration(wholeTRange._1, tRanges(0)._1.end), 0) +:
      tRanges.drop(1).dropRight(1) :+
      (Duration(tRanges.last._1.start, wholeTRange._2), tRanges.last._2)
    slots = tRangesExtended.map(_._1)
    val rddWIdx = rdd.map(instance => {
      val overlap = tRangesExtended.filter(x => x._1.intersects(instance._1)).head._2
      (overlap, instance._2)
    }).partitionBy(new KeyPartitioner(numPartitions))
    rddWIdx.map(_._2)
  }

  def partitionWithFixedLength[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T],
                                                                  slot: Array[Duration]): RDD[T] = {
    slots = slot
    val rdd = ref match {
      case "start" => dataRDD.map(x => (x.duration.start, x))
      case "center" => dataRDD.map(x => (x.temporalCenter, x))
      case "end" => dataRDD.map(x => (x.duration.end, x))
    }
    val slotsWIdx = slots.zipWithIndex
    val rddWIdx = rdd.map(instance => {
      val overlap = slotsWIdx.filter(x => x._1.intersects(instance._1)).head._2
      (overlap, instance._2)
    })
    val partitioner = new KeyPartitioner(numPartitions)
    rddWIdx.partitionBy(partitioner)
      .map(_._2)
  }
}
