package operatorsNew.extractor

import instances.Instance
import org.apache.spark.rdd.RDD

abstract class Extractor[I <: Instance[_, _, _]] {
  def extract(input: RDD[I]): _
}
