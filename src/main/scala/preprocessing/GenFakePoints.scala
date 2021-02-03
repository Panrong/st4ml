package preprocessing

import geometry.Point
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

object GenFakePoints {
  def apply(n: Int): RDD[Point] = {
    val sc = SparkContext.getOrCreate()
    val r = new Random(5)
    var pointArray = new Array[Point](0)
    for(_ <- 0 until n) {
      val lat = 25 + 10 * r.nextDouble
      val lon = 115 + 10 * r.nextDouble
      val t = (10000 * r.nextDouble).toLong
      val id = r.nextInt(1000)
      pointArray = pointArray :+ Point(Array(lon, lat), id).setTimeStamp(t)
    }
    sc.parallelize(pointArray)
  }
}
