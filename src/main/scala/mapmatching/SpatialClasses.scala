/** deprecated */
//package main.scala.mapmatching.SpatialClasses
//
//import scala.math._
//
//abstract class Shape() {
//  def inside(rectangle: Rectangle): Boolean
//
//  def intersect(rectangle: Rectangle): Boolean
//
//  def mbr(): Array[Double]
//
//  def dist(point: Point): Double
//
//  def center(): Point
//
//  var id: Long
//
//  var attr: Map[String, Array[String]] = Map()
//}
//
//case class Point(long: Double, lat: Double, t: Long = 0, ID:Long = 0) extends Shape with Serializable {
//  val x: Double = long
//  val y: Double = lat
//  override var id:Long = ID
//  def setID(i:Long):Point = {
//    id = i
//    this
//  }
//  override def intersect(rectangle: Rectangle): Boolean = {
//    inside(rectangle)
//  }
//
//  override def inside(rectangle: Rectangle): Boolean = {
//    if (x >= rectangle.x_min && x <= rectangle.x_max && y >= rectangle.y_min && y <= rectangle.y_max) true
//    else false
//  }
//
//  override def mbr(): Array[Double] = Array(x, y, x, y)
//
//  override def dist(point: Point): Double = math.sqrt(math.pow(x - point.x, 2) + math.pow(y - point.y, 2))
//
//  override def center(): Point = this
//}
///*
//case class Rectangle(bottomLeft: Point, topRight: Point, ID:Long = 0) extends Shape with Serializable {
//  override def center(): Point = Point((bottomLeft.lat + topRight.lat) / 2, (bottomLeft.long + topRight.long) / 2)
//  override var id:Long = ID
//  def setID(i:Long) :Rectangle= {
//    id = i
//    this
//  }
//  def dilate(d: Double): Rectangle = {
//    // dilate the rectangle with d meter
//    //approx 1 latitude degree = 111km
//    // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
//    val newLatMin = bottomLeft.lat - d / 111000
//    val newLatMax = topRight.lat + d / 111000
//    val newLongMin = bottomLeft.long - (111413 * cos(bottomLeft.lat.toRadians) - 94 * cos(3 * bottomLeft.lat.toRadians))
//    val newLongMax = topRight.long - (111.413 * cos(topRight.lat.toRadians) - 0.094 * cos(3 * topRight.lat.toRadians))
//    Rectangle(Point(newLatMin, newLongMin), Point(newLatMax, newLongMax))
//  }
//
//  val x_min = bottomLeft.x
//  val x_max = topRight.x
//  val y_min = bottomLeft.y
//  val y_max = topRight.y
//  val area: Double = (topRight.x - bottomLeft.x) * (topRight.y - bottomLeft.y)
//
//  def overlap(r: Rectangle): Double = {
//    val overlapX = max((x_max - x_min) + (r.x_max - r.x_min) - (max(x_max, r.x_max) - min(x_min, r.x_min)), 0)
//    val overlapY = max((y_max - y_min) + (r.y_max - r.y_min) - (max(y_max, r.y_max) - min(y_min, r.y_min)), 0)
//    overlapX * overlapY
//  }
//
//  def includeEntry(shape: Shape): Rectangle = {
//    // return a new Rectangle
//    if (shape.isInstanceOf[Point]) {
//      val point = shape.asInstanceOf[Point]
//      val new_x_min = min(point.x, this.x_min)
//      val new_x_max = max(point.x, this.x_max)
//      val new_y_min = min(point.y, this.y_min)
//      val new_y_max = max(point.y, this.y_max)
//      //println(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else if (shape.isInstanceOf[Rectangle]) {
//      val rectangle = shape.asInstanceOf[Rectangle]
//      val new_x_min = min(rectangle.x_min, this.x_min)
//      val new_x_max = max(rectangle.x_max, this.x_max)
//      val new_y_min = min(rectangle.y_min, this.y_min)
//      val new_y_max = max(rectangle.y_max, this.y_max)
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else throw new IllegalArgumentException
//  }
//
//  def enlargement(shape: Shape): Double = {
//    //calculate the enlargement needed to include one entry
//    val newRectangle = this.includeEntry(shape)
//    newRectangle.area - this.area
//  }
//
//  def margin(): Double = {
//    (x_max - x_min + y_max - y_min) * 2
//  }
//
//  override def inside(rectangle: Rectangle): Boolean = {
//    if (x_min >= rectangle.x_min && y_min >= rectangle.y_min && x_max <= rectangle.x_max && y_max <= rectangle.y_max) true
//    else false
//  }
//
//  override def intersect(r: Rectangle): Boolean = {
//    if (abs((x_min + x_max) / 2 - (r.x_min + r.x_max) / 2) < ((x_max + r.x_max - x_min - r.x_min) / 2)
//      && abs((y_min + y_max) / 2 - (r.y_min + r.y_max) / 2) < ((y_max + r.y_max - y_min - r.y_min) / 2)) true
//    else false
//  }
//
//  override def mbr(): Array[Double] = Array(x_min, y_min, x_max, y_max)
//
//  override def dist(point: Point): Double = math.sqrt(math.pow(center.x - point.x, 2) + math.pow(center.y - point.y, 2))
//
//  def addPointAttr(points: Array[Point]): Rectangle = {
//    var s = new Array[String](0)
//    for (p <- points) s = s :+ p.long.toString + "," + p.lat.toString
//    this.attr += ("points" -> s)
//    this
//  }
//
//  var trajectory: Trajectory = Trajectory(0,0,0,Array(Point(0,0)))
//
//  def addTrajAttr(traj: Trajectory): Rectangle = {
//    this.trajectory = traj
//    this
//  }
//  val vertices = Array(Point(x_min, y_min), Point(x_min, y_max), Point(x_max, y_max), Point(x_max, y_min))
//  val edges = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
//  val diagonals = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))
//}
// */
//case class Rectangle(bottomLeft: Point, topRight: Point, ID: Long = 0) extends Shape with Serializable {
//  override def center(): Point = Point((bottomLeft.long + topRight.long) / 2, (bottomLeft.lat + topRight.lat) / 2)
//  override var id: Long = ID
//  def setID(i: Long): Rectangle = {
//    id = i
//    this
//  }
//
//  def dilate(d: Double): Rectangle = {
//    // dilate the rectangle with d meter
//    //approx 1 latitude degree = 111km
//    // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
//    val newLatMin = bottomLeft.lat - d / 111000
//    val newLatMax = topRight.lat + d / 111000
//    val newLongMin = bottomLeft.long - (111413 * cos(bottomLeft.lat.toRadians) - 94 * cos(3 * bottomLeft.lat.toRadians))
//    val newLongMax = topRight.long - (111.413 * cos(topRight.lat.toRadians) - 0.094 * cos(3 * topRight.lat.toRadians))
//    Rectangle(Point(newLongMin, newLatMin), Point(newLongMax, newLatMax))
//  }
//
//  val x_min = bottomLeft.x
//  val x_max = topRight.x
//  val y_min = bottomLeft.y
//  val y_max = topRight.y
//  val area: Double = (topRight.x - bottomLeft.x) * (topRight.y - bottomLeft.y)
//
//  def overlap(r: Rectangle): Double = {
//    val overlapX = max((x_max - x_min) + (r.x_max - r.x_min) - (max(x_max, r.x_max) - min(x_min, r.x_min)), 0)
//    val overlapY = max((y_max - y_min) + (r.y_max - r.y_min) - (max(y_max, r.y_max) - min(y_min, r.y_min)), 0)
//    overlapX * overlapY
//  }
//
//  def includeEntry(shape: Shape): Rectangle = {
//    // return a new Rectangle
//    if (shape.isInstanceOf[Point]) {
//      val point = shape.asInstanceOf[Point]
//      val new_x_min = min(point.x, this.x_min)
//      val new_x_max = max(point.x, this.x_max)
//      val new_y_min = min(point.y, this.y_min)
//      val new_y_max = max(point.y, this.y_max)
//      //println(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else if (shape.isInstanceOf[Rectangle]) {
//      val rectangle = shape.asInstanceOf[Rectangle]
//      val new_x_min = min(rectangle.x_min, this.x_min)
//      val new_x_max = max(rectangle.x_max, this.x_max)
//      val new_y_min = min(rectangle.y_min, this.y_min)
//      val new_y_max = max(rectangle.y_max, this.y_max)
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else throw new IllegalArgumentException
//  }
//
//  def enlargement(shape: Shape): Double = {
//    //calculate the enlargement needed to include one entry
//    val newRectangle = this.includeEntry(shape)
//    newRectangle.area - this.area
//  }
//
//  def margin(): Double = {
//    (x_max - x_min + y_max - y_min) * 2
//  }
//
//  override def inside(rectangle: Rectangle): Boolean = {
//    if (x_min >= rectangle.x_min && y_min >= rectangle.y_min && x_max <= rectangle.x_max && y_max <= rectangle.y_max) true
//    else false
//  }
//
//  override def intersect(r: Rectangle): Boolean = {
//    if (abs((x_min + x_max) / 2 - (r.x_min + r.x_max) / 2) < ((x_max + r.x_max - x_min - r.x_min) / 2)
//      && abs((y_min + y_max) / 2 - (r.y_min + r.y_max) / 2) < ((y_max + r.y_max - y_min - r.y_min) / 2)) true
//    else false
//  }
//
//  override def mbr(): Array[Double] = Array(x_min, y_min, x_max, y_max)
//
//  override def dist(point: Point): Double = math.sqrt(math.pow(center.x - point.x, 2) + math.pow(center.y - point.y, 2))
//
//  def addPointAttr(points: Array[Point]): Rectangle = {
//    var s = new Array[String](0)
//    for (p <- points) s = s :+ p.long.toString + "," + p.lat.toString
//    this.attr += ("points" -> s)
//    this
//  }
//  var trajectory: Trajectory = Trajectory(0,0,0,Array(Point(0,0)))
//  def addTrajAttr(traj: Trajectory): Rectangle = {
//    this.trajectory = traj
//    this
//  }
//
//  val vertices = Array(Point(x_min, y_min), Point(x_min, y_max), Point(x_max, y_max), Point(x_max, y_min))
//  val edges = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
//  val diagonals = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))
//}
//case class Line(start: Point, end: Point, id: Long = 0) extends Serializable {
//  val length = greatCircleDist(start, end)
//
//  def dist2Point(p: Point): Double = {
//    val a = length
//    val b = greatCircleDist(p, start)
//    val c = greatCircleDist(p, end)
//    val alpha = acos((b * b + c * c - a * a) / (2 * b * c))
//    val beta = acos((-b * b + c * c + a * a) / (2 * a * c))
//    val gamma = acos((b * b - c * c + a * a) / (2 * b * a))
//    if (beta < Pi / 2 && gamma < Pi / 2) sqrt((a + b + c) * (a + b - c) * (a + c - b) * (b + c - a)) / (2 * a)
//    else if (beta >= Pi / 2) b
//    else c
//  }
//
//  def projectByPoint(p: Point): Array[Double] = {
//    val a = length
//    val b = greatCircleDist(p, start)
//    val c = greatCircleDist(p, end)
//    val alpha = acos((b * b + c * c - a * a) / (2 * b * c))
//    val beta = acos((-b * b + c * c + a * a) / (2 * a * c))
//    val gamma = acos((b * b - c * c + a * a) / (2 * b * a))
//    if (beta < Pi / 2 && gamma < Pi / 2) Array(c * sin(beta), a - c * sin(beta))
//    else if (beta >= Pi / 2) Array(0, a)
//    else Array(a, 0)
//  }
//
//  def mbr(): Rectangle = {
//    val lat_min = min(start.lat, end.lat)
//    val long_min = min(start.long, end.long)
//    val lat_max = max(start.lat, end.lat)
//    val long_max = max(start.long, end.long)
//    Rectangle(Point(lat_min, long_min), Point(lat_max, long_max))
//  }
//
//  def crosses(l: Line): Boolean = {
//    def crossProduct(A: Point, B: Point, C: Point): Double = (B.x - A.x) * (C.y - A.y) - (B.y - A.y) * (C.x - A.x)
//    val A = start
//    val B = end
//    val C = l.start
//    val D = l.end
//    if (min(A.x, B.x) <= max(C.x, D.x) &&
//      min(C.x, D.x) <= max(A.x, B.x) &&
//      min(A.y, B.y) <= max(C.y, D.y) &&
//      min(C.y, D.y) <= max(A.y, B.y) &&
//      crossProduct(A, B, C) * crossProduct(A, B, D) < 0 &&
//      crossProduct(C, D, A) * crossProduct(C, D, B) < 0)  true
//    else  false
//  }
//
//  def intersects(r: Rectangle): Boolean = {
//    if (this.start.intersect(r) || this.end.intersect(r)) return true
//    else {
//      if (!this.mbr().intersect(r)) return false
//      else {
//        for (e <- r.diagonals) if (e.crosses(this)) return true
//        return false
//      }
//    }
//  }
//}
//
//case class Trajectory(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point]) extends Serializable {
//  def mbr:Rectangle = {
//    var lat = new Array[Double](0)
//    var long = new Array[Double](0)
//    for(p <- points){
//      lat = lat :+ p.lat
//      long  =long :+ p.long
//    }
//    Rectangle(Point(long.min, lat.min), Point(long.max, lat.max))
//  }
//  def intersect(r: Rectangle): Boolean = {
//    for (p <- this.points) {
//      if (p.inside(r)) {
//        return true
//      }
//    }
//    false
//  }
//  def calSpeed(): Array[Double] = {
//    // return speed for each gps points interval
//    var s = new Array[Double](0)
//    for (p <- 0 to points.length - 2) {
//      s = s :+ greatCircleDist(points(p + 1), points(p)) / (points(p + 1).t - points(p).t)
//    }
//    s
//  }
//
//  def genLineSeg(): Array[Line] = {
//    var l = new Array[Line](0)
//    for (p <- 0 to points.length - 2) {
//      l = l :+ Line(points(p), points(p + 1), points(p).id) // the line id is the same as the start point's id
//    }
//    l
//  }
//
//  def calAvgSpeed(range: Rectangle): Double = {
//    val lineSegs = genLineSeg()
//    var dist:Double = 0
//    var time:Double = 0
//    for (l <- lineSegs) {
//      if (l.intersects(range)) {
//        dist += l.length
//        time += l.end.t - l.start.t
//      }
//    }
//    dist/time
//  }
//}
//case class mmTrajectory(tripID: String, taxiID: String, startTime: Long = 0, points: Array[String]) extends Serializable {
//
//}
//object greatCircleDist {
//  def apply(s: Point, f: Point): Double = {
//    val r = 6371009 // earth radius in meter
//    val phiS = s.lat.toRadians
//    val lambdaS = s.long.toRadians
//    val phiF = f.lat.toRadians
//    val lambdaF = f.long.toRadians
//    val deltaSigma = acos(sin(phiS) * sin(phiF) + cos(phiS) * cos(phiF) * cos(abs(lambdaF - lambdaS)))
//    //val deltaSigma = 2*asin(sqrt(pow(sin(abs(phiF-phiS)/2),2)+cos(phiF)*cos(phiS)*pow(sin(abs(lambdaF-lambdaS)/2),2)))
//    r * deltaSigma
//  }
//}
//
