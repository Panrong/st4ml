package operators.convertion

import geometry.{Raster, Shape, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TimeSeries2RasterConverter[T <: Shape : ClassTag](timeInterval: Int) extends Converter {
  override type I = TimeSeries[T]
  override type O = Raster[T]

  override def convert(rdd: RDD[(Int, TimeSeries[T])]): RDD[Raster[T]] = {
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
