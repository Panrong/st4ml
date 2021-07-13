package operatorsNew.selector
import instances.Instance
import org.apache.spark.rdd.RDD

abstract class Selector[R <: Instance[_,_,_]] extends Serializable {
  def query(dataRDD: RDD[R]): RDD[R]
}
