package index

import org.scalatest.funsuite.AnyFunSuite

import geometry.{Point, Rectangle}

class RTreeSuite extends AnyFunSuite {

  var entries = new Array[(Point, Int)](221)
  var cnt = 0
  for (i <- -10 to 10) {
    for(j <- -10 to 10) {
      if(Math.abs(i) + Math.abs(j) <= 10) {
        entries(cnt) = (Point(Array(i, j)), i + j)
        cnt = cnt + 1
      }
    }
  }
  val rtree = RTree.apply(entries, 5)

  test("RTree: range, simple"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = Rectangle(A.coordinates ++ B.coordinates)
    val range = rtree.range(mbr)

    range.foreach(println)

    for(x <- range){
      assert(mbr.intersect(x._1))
    }

    var count = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(range.length == count)
  }

  test("RTree: range, complex"){
    val A = Point(Array(0.0, 0.0))
    val B = Point(Array(9.0, 9.0))
    val mbr = Rectangle(A.coordinates ++ B.coordinates)

    val ans = rtree.range(mbr, 10, 1.0)
    assert(ans.isDefined)
    val range = ans.get
    for(x <-range){
      assert(mbr.intersect(x._1))
    }
    var count = 0
    for (i <- -10 to 10){
      for(j <- -10 to 10){
        if(Math.abs(i) + Math.abs(j) <= 10) {
          if(i >= 0 && j >= 0 && i <= 9 && j <= 9)
            count = count + 1
        }
      }
    }
    assert(range.length == count)

    val no_ans = rtree.range(mbr, 1, 0.0)
    assert(no_ans.isEmpty)
  }

  test("RTree: kNN"){
    val center = Point(Array(10.0, 9.0))
    val k = 4
    val range = rtree.kNN(center, k)

    def minDist(p: Point): Double = range.map(x => x._1.minDist(p)).min

    assert(range.length == 4)
    assert(minDist(Point(Array(7.0, 3.0))) < 1e-8)
    assert(minDist(Point(Array(6.0, 4.0))) < 1e-8)
    assert(minDist(Point(Array(5.0, 5.0))) < 1e-8)
    assert(minDist(Point(Array(4.0, 6.0))) < 1e-8)
  }

}
