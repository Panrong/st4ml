package operatorsNew.extractor

import instances.TimeSeries
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TsFlowExtractor[T <: TimeSeries[Array[_], D] : ClassTag, D: ClassTag] extends Extractor[T] {

  def extract(rdd: RDD[T]): RDD[TimeSeries[Int, D]] = {
    rdd.map(ts => ts.mapValue(_.length))
  }
}
