package geometry

case class mmTrajectory(tripID: String, roads: Array[(String, Long)]) extends Shape with Serializable {
  override def coordinates: Array[Double] = Array.empty

  override def intersect(other: Shape): Boolean = {
    other match {
      case mmTraj: mmTrajectory => this.mbr.intersect(mmTraj.mbr)
      case rectangle: Rectangle => this.mbr.intersect(rectangle)
      case point: Point => this.mbr.intersect(point)
    }
  }

  var mbr: Rectangle = Rectangle(Array(0,0,0,0))

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
  override var id: Long = tripID.toLong

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
}
