package main.scala.mapmatching

import main.scala.RTree.{Node, RTree}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import main.scala.geometry._

import scala.util.control._
import System.nanoTime

import main.scala.geometry.Distances.greatCircleDistance
import org.apache.spark.sql.types._

import Array.concat
import scala.io.Source

object preprocessing {

  val timeCount = true

  def genTrajRDD(filename: String, num: Int): RDD[Trajectory] = {
    val t = nanoTime
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.option("header", "true").csv(filename).limit(num)
    val samplingRate = 15
    val trajRDD = df.rdd.filter(row => row(8).toString.split(',').length >= 4) // each traj should have no less than 2 recorded points
    var resRDD = trajRDD.map(row => {
      val tripID = row(0).toString.toLong
      val taxiID = row(4).toString.toLong
      val startTime = row(5).toString.toLong
      val pointsString = row(8).toString
      var pointsCoord = new Array[Double](0)
      for (i <- pointsString.split(',')) pointsCoord = pointsCoord :+ i.replaceAll("[\\[\\]]", "").toDouble
      var points = new Array[Point](0)
      for (i <- 0 to pointsCoord.length - 2 by 2) {
        points = points :+ Point(pointsCoord(i), pointsCoord(i + 1), startTime + samplingRate * i / 2)
      }
      Trajectory(tripID, taxiID, startTime, points)
    })
    println("==== Read CSV Done")
    println("--- Total number of lines: " + df.count)
    println("--- Total number of valid entries: " + resRDD.count)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    resRDD
  }

  def splitTraj(trajectory: Trajectory, splitPoints: Array[Int]): Array[Trajectory] = {
    if (splitPoints.length == 1) Array(trajectory)
    else {
      var trajs = new Array[Trajectory](0)
      //println(splitPoints.deep)
      for (i <- 0 to splitPoints.length - 2) {
        val points = trajectory.points.slice(splitPoints(i), splitPoints(i + 1) + 1)
        trajs = trajs :+ Trajectory(trajectory.tripID, trajectory.taxiID, points(0).t, points)
      }
      trajs
    }
  }

  def trajBreak(trajRDD: RDD[Trajectory], speed: Double = 50, timeInterval: Double = 180): RDD[Trajectory] = {
    val t = nanoTime
    println("==== Split trajectories with speed limit " + speed + " m/s and time interval limit " + timeInterval + " s")
    // speed and time interval check
    val newTrajRDD = trajRDD.flatMap(traj => {
      var splitPoints = new Array[Int](0)
      for (i <- 0 to traj.points.length - 2 by 2) {
        val t = traj.points(i + 1).t - traj.points(i).t
        val l = greatCircleDistance(traj.points(i + 1), traj.points(i))
        if (l / t > speed || t > timeInterval) splitPoints = splitPoints :+ i
      }
      splitTraj(traj, 0 +: splitPoints)
    }).filter(traj => traj.points.length > 1)
    println("==== Split Trajectories Done")
    println("--- Now total number of entries: " + newTrajRDD.count)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    newTrajRDD
  }

  def removeRedundancy(trajRDD: RDD[Trajectory], sigmaZ: Double = 4.07): RDD[Trajectory] = {
    val t = nanoTime
    val resRDD = trajRDD.map(traj => {
      var newPoints = Array(traj.points(0))
      for (p <- 1 until traj.points.length) {
        if (greatCircleDistance(traj.points(p), newPoints.last) >= 2 * sigmaZ) newPoints = newPoints :+ traj.points(p)
      }
      Trajectory(traj.tripID, traj.taxiID, traj.startTime, newPoints)
    })
    println("==== Remove Redundancy Done")
    println("--- Now total number of entries: " + resRDD.count)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    resRDD
  }

  def checkMapCoverage(trajRDD: RDD[Trajectory], mapRange: List[Double]): RDD[Trajectory] = {
    val t = nanoTime
    val resRDD = trajRDD.filter(traj => {
      var check = true
      val loop = new Breaks
      loop.breakable {
        for (point <- traj.points) {
          if (point.lat < mapRange.head || point.lat > mapRange(2) || point.lon < mapRange(1) || point.lon > mapRange(3)) {
            check = false
            loop.break
          }
        }
      }
      check
    })
    println("==== Check Map Coverage Range Done")
    println("--- Now total number of entries: " + resRDD.count + " in the map range of " + mapRange)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    resRDD
  }

  def apply(filename: String, mapRange: List[Double], clean: Boolean = true, num:Int = 2147483647): RDD[Trajectory] = {
    if (clean) checkMapCoverage(removeRedundancy(trajBreak(genTrajRDD(filename, num))), mapRange)
    else genTrajRDD(filename, num)
  }

  def readMMTrajFile(filename: String): RDD[mmTrajectory] = {
    val customSchema = StructType(Array(
      StructField("taxiID", LongType, nullable = true),
      StructField("tripID", LongType, nullable = true),
      StructField("GPSPoints", StringType, nullable = true),
      StructField("VertexID", StringType, nullable = true),
      StructField("Candidates", StringType, nullable = true),
      StructField("pointRoadPair", StringType, nullable = true))
    )
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.option("header", "true").schema(customSchema).csv(filename)
    //val trajRDD = df.rdd.filter(row => row(3).toString.split(':').length > 2) // remove invalid entries
    val trajRDD = df.rdd.filter(row => row(3)!="(-1:-1)" && row(3)!="-1") // remove invalid entries

    val resRDD = trajRDD.map(row => {
      val tripID = row(1).toString
      val taxiID = row(0).toString
      val vertexString = row(3).toString
      var vertices = new Array[String](0)
      for (i <- vertexString.split(',')) vertices = concat(vertices, i.replaceAll("[(),]", "").split(" "))
      vertices = vertices.map(x => x.dropRight(2))
      mmTrajectory(tripID, taxiID, points = vertices)
    })
    resRDD
  }
  def readMMWithSpeed(filename: String): RDD[mmTrajectoryS] = {
    val customSchema = StructType(Array(
      StructField("taxiID", LongType, nullable = true),
      StructField("tripID", LongType, nullable = true),
      StructField("GPSPoints", StringType, nullable = true),
      StructField("VertexID", StringType, nullable = true),
      StructField("Candidates", StringType, nullable = true),
      StructField("pointRoadPair", StringType, nullable = true),
      StructField("RoadSpeed", StringType, nullable = true))
    )
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.option("header", "true").schema(customSchema).csv(filename)
    val trajRDD = df.rdd.filter(row => row(3)!="(-1:-1)" && row(3)!="-1") // remove invalid entries
    val resRDD = trajRDD.map(row => {
      val tripID = row(1).toString
      val taxiID = row(0).toString
//      val vertexString = row(3).toString
//      var vertices = new Array[String](0)
      val roadSpeed = row(6).toString.replaceAll("[()]", "").split(" ").map(x=>(x.split(",")(0), x.split(",")(1).toDouble)) // Array(roadID, speed)
      val subTrajectories = roadSpeed.map(x=> subTrajectory(0,0,x._1, x._2))
      mmTrajectoryS(tripID, taxiID, subTrajectories(0).startTime, subTrajectories)
    })
    resRDD
  }
  def readMMWithRoadTime(filename: String): RDD[mmTrajectoryS] = {
    val customSchema = StructType(Array(
      StructField("taxiID", LongType, nullable = true),
      StructField("tripID", LongType, nullable = true),
      StructField("GPSPoints", StringType, nullable = true),
      StructField("VertexID", StringType, nullable = true),
      StructField("Candidates", StringType, nullable = true),
      StructField("pointRoadPair", StringType, nullable = true),
      StructField("RoadTime", StringType, nullable = true))
    )
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.option("header", "true").schema(customSchema).csv(filename)
    val trajRDD = df.rdd.filter(row => row(3)!="(-1:-1)" && row(3)!="-1") // remove invalid entries
    val resRDD = trajRDD.map(row => {
      val tripID = row(1).toString
      val taxiID = row(0).toString
      //      val vertexString = row(3).toString
      //      var vertices = new Array[String](0)
      val roadTime = row(6).toString.replaceAll("[() ]","").split(",").grouped(2).toArray // Array(Array(roadID, time))
      val subTrajectories = roadTime.map(x=> subTrajectory(x(1).toLong, 0, x(0), 0))
      mmTrajectoryS(tripID, taxiID, subTrajectories(0).startTime, subTrajectories)
    })
    resRDD
  }

  def readQueryFile(f: String): Array[Rectangle] = {
    var queries = new Array[Rectangle](0)
    for (line <- Source.fromFile(f).getLines) {
      val r = line.split(" ")
      queries = queries :+ Rectangle(Point(r(0).toDouble,r(1).toDouble), Point(r(2).toDouble, r(3).toDouble))
    }
    queries
  }

  def readRoadIDQueryFile(f:String): Array[String] = {
    Source.fromFile(f).getLines.toArray
  }
}

object preprocessingTest extends App {
  val conf = new SparkConf()
  conf.setAppName("MapMatching_v1").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val filename = "C:\\Users\\kaiqi001\\Map Matching\\src\\Porto_taxi_data_training.csv"
  val trajRDD = preprocessing(filename, List(-10e6, 10e6, -10e6, 10e6))
  println("==== Trajectory examples: ")
  for (i <- trajRDD.take(2)) {
    println("tripID: " + i.tripID + " taxiID: " + i.taxiID + " startTime: " + i.startTime + " points: " + i.points.deep)
  }
  //rtree test

  val rectangles = trajRDD.map(x => x.mbr.assignID(x.tripID)).collect

  //generate rtree
  var rootNode = Node(Rectangle(Point(0, 0), Point(10, 10)), isLeaf = true)
  val capacity = 200
  var rtree = RTree(rootNode, capacity)
  var i = 0
  for (rectangle <- rectangles) {
    RTree.insertEntry(rectangle, rtree)
    i += 1
  }
  // range query
  val res = rtree.genTable()
  val table = res._1
  val entries = rtree.leafEntries
  val queryRange = Rectangle(Point(-8.625, 41.145), Point(-8.615, 41.155))
  val retrieved = RTree.queryWithTable(table.map { case (key, value) => (key, value.mbr) }, entries, capacity, queryRange)
  //printRetrieved(retrieved)
  for (r <- retrieved) println(r.id)
  println(retrieved.length + " trajectories retrieved in the range " + queryRange.x_min, queryRange.y_min, queryRange.x_max, queryRange.y_max)
  sc.stop()

  def printRetrieved(retrieved: Array[Shape]) {
    println("-------------")
    for (i <- retrieved) {
      i match {
        case p: Point =>
          println(p.x, p.y)
        case _ =>
          val r = i.asInstanceOf[Rectangle]
          println(r.x_min, r.y_min, r.x_max, r.y_max)
      }
    }
    println("-------------")
  }

}