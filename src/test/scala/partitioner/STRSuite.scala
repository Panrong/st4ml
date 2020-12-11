package partitioner

import geometry.{Point, Rectangle}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

class STRSuite extends AnyFunSuite {

  val conf = new SparkConf()
  conf.setMaster("local").setAppName("partitionerTest")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  var entries = new Array[Point](1000)
  var cnt = 0
  for (i <- -10 to 10) {
    for (j <- -10 to 10) {
      if (Math.abs(i) + Math.abs(j) <= 10) {
        entries(cnt) = Point(Array(i, j))
        cnt = cnt + 1
      }
    }
  }
  val rdd = sc.parallelize(entries)
  val (pRDD, idBoundMap) = STRPartitioner(rdd, 9, 0.5)
  test("STR: partitioner, simple") {
    assert(pRDD.getNumPartitions == 9)
  }
  test("STR: coverage, simple") {
    val wholeRange = Rectangle(Array(
      rdd.map(_.coordinates(0)).min,
      rdd.map(_.coordinates(1)).min,
      rdd.map(_.coordinates(0)).min,
      rdd.map(_.coordinates(1)).min))

    for (r <- idBoundMap.values) assert(r.inside(wholeRange))

    for (i <- idBoundMap.values) {
      for (j <- idBoundMap.values) {
        if (i.coordinates.toList != j.coordinates.toList) {
          assert(i.overlappingArea(j) == 0)
        }
      }
    }
    var totalArea: Double = 0
    for (r <- idBoundMap.values) totalArea += r.area
    assert(totalArea == wholeRange)
  }
}
