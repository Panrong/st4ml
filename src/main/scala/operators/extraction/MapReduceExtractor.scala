package operators.extraction

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MapReduceExtractor[I: ClassTag, N: ClassTag, M: ClassTag](mapFunc: I => (N, M),
                                                                reduceFunc: (M, M) => M,
                                                                collectFunc: Array[(N, M)] => Array[(N, M)]) extends BaseExtractor {

  def extract(rdd: RDD[I]): Array[(N, M)] = {
    collectFunc(rdd.map(mapFunc).reduceByKey(reduceFunc).collect)
  }
}
