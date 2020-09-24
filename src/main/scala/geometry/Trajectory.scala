package main.scala.geometry

import Distances.greatCircleDistance

case class Trajectory(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point]) extends Serializable {
  def mbr:Rectangle = {
    var lat = new Array[Double](0)
    var lon = new Array[Double](0)
    for(p <- points){
      lat = lat :+ p.lat
      lon  =lon :+ p.lon
    }
    Rectangle(Point(lon.min, lat.min), Point(lon.max, lat.max))
  }
  def intersect(r: Rectangle): Boolean = {
    for (p <- this.points) {
      if (p.inside(r)) {
        return true
      }
    }
    false
  }
  def calSpeed(): Array[Double] = {
    // return speed for each gps points interval
    var s = new Array[Double](0)
    for (p <- 0 to points.length - 2) {
      s = s :+ greatCircleDistance(points(p + 1), points(p)) / (points(p + 1).t - points(p).t)
    }
    s
  }

  def genLineSeg(): Array[Line] = {
    var l = new Array[Line](0)
    for (p <- 0 to points.length - 2) {
      l = l :+ Line(points(p), points(p + 1), points(p).id) // the line id is the same as the start point's id
    }
    l
  }

  def calAvgSpeed(range: Rectangle): Double = {
    val lineSegs = genLineSeg()
    var dist:Double = 0
    var time:Double = 0
    for (l <- lineSegs) {
      if (l.intersects(range)) {
        dist += l.length
        time += l.d.t - l.o.t
      }
    }
    dist/time
  }
}
case class mmTrajectory(tripID: String, taxiID: String, startTime: Long = 0, points: Array[String]) extends Serializable {

}
