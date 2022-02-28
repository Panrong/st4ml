package st4ml.operators.converter

import st4ml.instances.Instance
import org.apache.spark.rdd.RDD

class DoNothingConverter[T <: Instance[_,_,_]]  {
   type I = T
   type O = T

   def convert(input: RDD[I]): RDD[O] = input
}
