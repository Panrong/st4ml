package geometry

import geometry.road.RoadGrid

case class mmTrajectory(tripID: String, roads: Array[(String, Long)]) extends Shape with Serializable {
  override def coordinates: Array[Double] = Array.empty

  override def intersect(other: Shape): Boolean = {
    other match {
      case mmTraj: mmTrajectory => this.mbr.intersect(mmTraj.mbr)
      case rectangle: Rectangle => this.mbr.intersect(rectangle)
      case point: Point => this.mbr.intersect(point)
    }
  }

  var mbr: Rectangle = Rectangle(Array(0, 0, 0, 0))

  override def center(): Point = mbr.center()

  override def geoDistance(other: Shape): Double = {
    other match {
      case mmTraj: mmTrajectory => this.mbr.geoDistance(mmTraj.mbr)
      case rectangle: Rectangle => this.mbr.geoDistance(rectangle)
      case point: Point => this.mbr.geoDistance(point)
    }
  }

  override def minDist(other: Shape): Double = {
    other match {
      case mmTraj: mmTrajectory => this.mbr.minDist(mmTraj.mbr)
      case rectangle: Rectangle => this.mbr.minDist(rectangle)
      case point: Point => this.mbr.minDist(point)
    }
  }

  override def inside(rectangle: Rectangle): Boolean = {
    this.mbr.inside(rectangle)
  }

  override var id: String = tripID

  override var timeStamp: (Long, Long) = (roads.head._2, roads.last._2)

  def addMBR(roadMap: Map[String, List[Point]]): mmTrajectory = {
    val points = roads.flatMap(x => roadMap(x._1))
    val xMin = points.map(p => p.x).min
    val xMax = points.map(p => p.x).max
    val yMin = points.map(p => p.y).min
    val yMax = points.map(p => p.y).max
    this.mbr = Rectangle(Array(xMin, yMin, xMax, yMax)).setTimeStamp(timeStamp)
    this
  }

  var speed: Array[(String, Double)] = Array.empty

  def getRoadSpeed(rg: RoadGrid): mmTrajectory = {
    try {
      val middleSpeed = roads.sliding(3).map(x =>
        (rg.id2edge(x(0)._1).length * 0.5 +
          rg.id2edge(x(1)._1).length +
          rg.id2edge(x(2)._1).length * 0.5)
          / (x(2)._2 - x(0)._2) * 3.6
      ).toArray
      val speed = middleSpeed.head +: middleSpeed :+ middleSpeed.last
      this.speed = roads.map(_._1) zip speed
    } catch {
      case _: Throwable => println(tripID)
    }
    this
  }
}
