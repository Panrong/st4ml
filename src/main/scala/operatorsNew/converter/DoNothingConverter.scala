package operatorsNew.converter
import instances.Instance
import org.apache.spark.rdd.RDD

class DoNothingConverter[T <: Instance[_,_,_]] extends Converter {
  override type I = T
  override type O = T
  override def convert(input: RDD[I]): RDD[O] = input
}
