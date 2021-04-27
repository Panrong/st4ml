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
    points.sliding(2).map(x => x(1).geoDistance(x(0)) / (x(1).timeStamp._1 - x(0).timeStamp._1)).toArray
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

  def strictIntersect(r: Rectangle, t: (Long, Long)): Boolean = {
    for (i <- lines) {
      if (i.intersect(r) && temporalOverlap(i.timeStamp, t)) return true
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

  // get segments inside a spatial window
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

  // get the segment inside a temporal window
  def windowBy(range: (Long, Long)): Option[Array[Trajectory]] = {
    val tStart = range._1
    val tEnd = range._2
    if (this.timeStamp._1 > tEnd || this.timeStamp._2 < tStart) {
      println(this)
      println(s"is not in range $range")
      None
    }
    else {
      val lineSegments = lines.filter(line => temporalOverlap(range, line.timeStamp)).map(_.windowBy(range))
      val subtrajPoints = lineSegments.map(x => Array(x.o, x.d)).foldLeft(Array[Array[Point]]()) {
        case (li, e) => if (li.isEmpty || (li.last.last.coordinates.deep != e.head.coordinates.deep && li.last.length >= 2)) li :+ e
        else li.dropRight(1) :+ (li.last ++ e)
      }
      Some(subtrajPoints.zipWithIndex.map {
        case (points, id) => Trajectory(this.tripID + "_" + id.toString, points.head.timeStamp._1, points)
      })
    }
  }

  def hasFakePlate(speedThreshold: Double): Boolean = calSpeed().exists(_ > speedThreshold)

  def findAbnormalSpeed(speedThreshold: Double):
  Array[((Long, (Double, Double)), (Long, (Double, Double)), Double)] = {
    //( (startTime ,(startLon, startLat)), (endTime, (endLon, endLat)), speed)
    val start = points.map(x => (x.timeStamp._1, (x.lon, x.lat))).dropRight(1)
    val end = points.map(x => (x.timeStamp._2, (x.lon, x.lat))).drop(1)
    (start, end, calSpeed()).zipped.toArray.filter(x => x._3 > speedThreshold && x._1 != x._2)
  }

  /** check if the points are temporally ordered */
  def tOrderCheck(): Boolean = {
    points.sliding(2, 1).forall {
      case Array(p1, p2) => p1.timeStamp._1 < p2.timeStamp._2
    }
  }

  def reorderTemporally(): Trajectory = {
    if (!tOrderCheck()) {
      scala.util.Sorting.quickSort(points)(Ordering.by[Point, Long](_.timeStamp._1))
      Trajectory(tripID, points.head.timeStamp._1, points, attributes)
    } else this
  }


  override def toString: String = s"Trajectory(id: $id, timestamps: $timeStamp, points: ${points.mkString(",")})"
}
