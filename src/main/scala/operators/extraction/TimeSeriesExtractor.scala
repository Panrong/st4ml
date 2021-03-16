package operators.extraction

import geometry.{Shape, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TimeSeriesExtractor {
  // if the time slot intersects with the query time window, all samples are counted
  // --> may include more samples
  def extractByTimeCoarse[T: ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[T] = {
    rdd.filter(ts => ts.startTime <= timeRange._2 && ts.endTime >= timeRange._1)
      .flatMap(ts => ts.toMap)
      .filter {
        case ((tStart, tEnd), _) => tStart <= timeRange._2 && tEnd >= timeRange._1
      }
      .flatMap(_._2)
  }

  // exact query, need the elements of the time series extending Shape
  def extractByTime[T <: Shape : ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[T] = {
    extractByTimeCoarse(timeRange)(rdd)
      .filter(x => x.timeStamp._1 >= timeRange._1 && x.timeStamp._2 <= timeRange._2)
  }

  // kind of like a batch version of extractByTime but only count the number
  def countTimeSlotSamples[T: ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[((Long, Long), Int)] = {
    rdd.filter(ts => ts.startTime <= timeRange._2 && ts.endTime >= timeRange._1)
      .flatMap(ts => ts.toMap)
      .filter {
        case ((tStart, tEnd), _) => tStart <= timeRange._2 && tEnd >= timeRange._1
      }
      .mapValues(_.length)
  }

  def extractByTimeSpatialCoarse[T: ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[T] = {
    rdd.flatMap(ts => ts.toMap)
      .filter {
        case ((tStart, tEnd), _) => tStart <= timeRange._2 && tEnd >= timeRange._1
      }.mapPartitions(iter => iter.map(_._2)).flatMap(x => x)
  }

  def extractByTimeSpatial[T <: Shape : ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[T] = {
    extractByTimeSpatialCoarse(timeRange)(rdd)
      .filter(x => x.timeStamp._1 >= timeRange._1 && x.timeStamp._2 <= timeRange._2)
  }

  def countTimeSlotSamplesSpatial[T: ClassTag](timeRange: (Long, Long))(rdd: RDD[TimeSeries[T]]): RDD[Array[((Long, Long), Int)]] = {
    rdd.mapPartitions(iter => iter.toArray
      .map(ts => ts.toMap
        .filter {
          case ((tStart, tEnd), _) => tStart <= timeRange._2 && tEnd >= timeRange._1
        }.map {
        case (k, v) => (k, v.length)
      }.toArray).toIterator)

  }
}

