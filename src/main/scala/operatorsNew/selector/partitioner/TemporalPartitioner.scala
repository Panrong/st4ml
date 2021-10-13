package operatorsNew.selector.partitioner

import instances.{Duration, Geometry, Instance}
import operatorsNew.selector.SelectionUtils.Ss
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TemporalPartitioner(override val numPartitions: Int,
                          override var samplingRate: Option[Double] = None,
                          ref: String = "start") extends STPartitioner with Ss {

  def partition[T <: Instance[_ <: Geometry, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => x.mapTemporal(x => Duration(x.start)).asInstanceOf[T])
      case "center" => dataRDD.map(x => x.mapTemporal(x => Duration(x.center)).asInstanceOf[T])
      case "end" => dataRDD.map(x => x.mapTemporal(x => Duration(x.end)).asInstanceOf[T])
    }
    import spark.implicits._
    val wholeTRange = (rdd.map(_.temporalCenter).min, rdd.map(_.temporalCenter).max)
    val tDf = rdd.sample(withReplacement = false, sr).map(_.temporalCenter).toDF
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val tRanges = tDf.stat.approxQuantile("value", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong, x(1).toLong)).zipWithIndex
    val tRangesExtended = (Duration(wholeTRange._1, tRanges(0)._1.end), 0) +:
      tRanges.drop(1).dropRight(1) :+
      (Duration(tRanges.last._1.start, wholeTRange._2), tRanges.last._2)
    val rddWIdx = rdd.map(instance => {
      val overlap = tRangesExtended.filter(x => instance.intersects(x._1)).head._2
      (overlap, instance)
    })
    val partitioner = new KeyPartitioner(numPartitions)
    rddWIdx.partitionBy(partitioner)
      .map(_._2)
  }

  def partitionWDup[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => x.mapTemporal(x => Duration(x.start)).asInstanceOf[T])
      case "center" => dataRDD.map(x => x.mapTemporal(x => Duration(x.center)).asInstanceOf[T])
      case "end" => dataRDD.map(x => x.mapTemporal(x => Duration(x.end)).asInstanceOf[T])
    }
    import spark.implicits._
    val tDf = rdd.sample(withReplacement = false, sr).map(_.temporalCenter).toDF
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val tRanges = tDf.stat.approxQuantile("_c0", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong, x(1).toLong)).zipWithIndex
    rdd.flatMap(instance => {
      val overlap = tRanges.filter(x => instance.intersects(x._1)).map(_._2)
      overlap.map(x => (x, instance))
    }).partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2)
  }

  def partitionWithOverlap[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T],
                                                              overlap: Long): RDD[T] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rdd = ref match {
      case "start" => dataRDD.map(x => x.mapTemporal(x => Duration(x.start)).asInstanceOf[T])
      case "center" => dataRDD.map(x => x.mapTemporal(x => Duration(x.center)).asInstanceOf[T])
      case "end" => dataRDD.map(x => x.mapTemporal(x => Duration(x.end)).asInstanceOf[T])
    }
    import spark.implicits._
    val tDf = rdd.sample(withReplacement = false, sr).map(_.temporalCenter).toDF
    val splitPoints = (0 to numPartitions).map(_ / numPartitions.toDouble).toArray
    val tRanges = tDf.stat.approxQuantile("_c0", splitPoints, 0.01)
      .sliding(2).toArray.map(x => Duration(x(0).toLong - overlap, x(1).toLong + overlap)).zipWithIndex
    rdd.map(instance => {
      val overlap = tRanges.filter(x => instance.intersects(x._1)).head._2
      (overlap, instance)
    }).partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2)
  }
}
