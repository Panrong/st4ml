package operators.extraction

import geometry.Point
import org.apache.spark.rdd.RDD


class FreqPointExtractor(topK: Int) {
  def mapFunc(x: Point): (String, Int) = (x.id, 1)

  def reduceFunc(x: Int, y: Int): Int = x + y

  def collectFunc(x: Array[(String, Int)]): Array[(String, Int)] = x.sortBy(_._2)(Ordering.Int.reverse).take(topK)

  val extractor = new MapReduceExtractor[Point, String, Int](mapFunc, reduceFunc, collectFunc)

  def extract(rdd: RDD[Point]): Array[(String, Int)] = extractor.extract(rdd)
}

