package preprocessing

import geometry.{Point, Rectangle}

import scala.io.Source

object readQueryFile {
  def apply(f: String): Array[Rectangle] = {
    var queries = new Array[Rectangle](0)
    for ((line, i) <- (Source.fromFile(f).getLines).zipWithIndex) {
      val r = line.split(" ")
      queries = queries :+ Rectangle(Point(r(0).toDouble, r(1).toDouble), Point(r(2).toDouble, r(3).toDouble), i)
    }
    queries
  }
}
