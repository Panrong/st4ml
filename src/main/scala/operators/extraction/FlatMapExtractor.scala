package operators.extraction

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class FlatMapExtractor[I: ClassTag, N: ClassTag, M: ClassTag](flatMapFunc: I => Vector[(N, M)],
                                                              reduceFunc: (M, M) => M,
                                                              collectFunc: Array[(N, M)] => Array[(N, M)],
                                                               ) extends Serializable {

  def extract(rdd: RDD[I]): Array[(N, M)] = {
    val resRDD = rdd.flatMap(flatMapFunc).reduceByKey(reduceFunc)
    collectFunc(resRDD.collect)
  }
}


