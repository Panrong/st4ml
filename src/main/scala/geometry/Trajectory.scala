package geometry

import geometry.Distances.greatCircleDistance

import scala.collection.mutable


case class Trajectory(tripID: String,
                      startTime: Long,
                      points: Array[Point],
                      attributes: Map[String, String] = Map()) extends Shape {

  override var timeStamp: (Long, Long) =
    (points.map(_.timeStamp._1).min, points.map(_.timeStamp._2).max) // to avoid invalid timeStamps


  val mbr: Rectangle = Rectangle(Array(
    points.map(p => p.coordinates(0)).min,
    points.map(p => p.coordinates(1)).min,
    points.map(p => p.coordinates(0)).max,
    points.map(p => p.coordinates(1)).max
  ), ID = tripID, timeStamp = timeStamp)

  val endTime: (Long, Long) = points.last.timeStamp

  override var id: String = tripID

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
    var dist: Double = 0
    var time: Double = 0
    for (l <- lineSegs) {
      if (l.intersect(range)) {
        dist += l.length
        time += l.d.t - l.o.t
      }
    }
    dist / time
  }


  def lines: Array[Line] = points.sliding(2).map(x => Line(x(0), x(1))).toArray

  def strictIntersect(r: Rectangle): Boolean = {
    for (i <- lines) {
      if (i.intersect(r)) return true
    }
    false
  }

  override def coordinates: Array[Double] = mbr.coordinates

  override def intersect(other: Shape): Boolean = this.mbr.intersect(other)

  override def center(): Point = mbr.center()

  override def geoDistance(other: Shape): Double = this.mbr.geoDistance(other)

  override def minDist(other: Shape): Double = this.mbr.minDist(other)

  override def inside(rectangle: Rectangle): Boolean = this.mbr.inside(rectangle)

  def setID(i: String): Trajectory = this.copy(tripID = i)

  def windowBy(rectangle: Rectangle): Option[Array[Trajectory]] = {
    val lineSegments = lines.filter(_.intersect(rectangle)).map(_.windowBy(rectangle))
    if (lineSegments.length == 0) None
    else {
      val subtrajPoints = lineSegments.map(x => Array(x.o, x.d)).foldLeft(Array[Array[Point]]()) {
        case (li, e) => if (li.isEmpty || (li.last.last.coordinates.deep != e.head.coordinates.deep && li.last.length >= 2)) li :+ e
        else li.dropRight(1) :+ (li.last ++ e)
      }
      //      println(">>>>>>")
      //      println(this.points.map(x => (x.x, x.y)).deep)
      //      println(rectangle)
      //      println(lineSegments.map(x => (x.o.x, x.o.y, x.d.x, x.d.y)).deep)
      //      println(subtrajPoints.map(i => i.map(j => (j.x, j.y))).deep)
      //      println("<<<<<<")
      Some(subtrajPoints.zipWithIndex.map {
        case (points, id) => Trajectory(this.tripID + "_" + id.toString, points.head.timeStamp._1, points)
      })
    }
  }

  override def toString: String = s"Trajectory(id: $id, start time: $startTime, points: ${points.mkString(",")})"
}
