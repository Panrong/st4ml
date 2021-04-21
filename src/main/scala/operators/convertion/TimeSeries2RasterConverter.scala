package operators.convertion

import geometry.{Raster, Shape, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TimeSeries2RasterConverter(timeInterval: Int) extends Converter {
  def convert[T <: Shape : ClassTag](rdd: RDD[(Int, TimeSeries[T])]): RDD[Raster[T]] = {
    rdd.map(_._2).mapPartitions(iter => {
      val ts = iter.toArray.head
      val subTimeSeries = ts.splitByInterval(timeInterval)
      subTimeSeries.zipWithIndex.map {
        case (x, id) =>
          Raster(id = ts.id + "-" + id.toString, Array(x))
      }.toIterator
    })
  }
}
