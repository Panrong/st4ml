package preprocessing

import geometry.Distances.greatCircleDistance
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.lang.System.nanoTime
import scala.util.control.Breaks

object ReadTrajFile {
  val timeCount = true

  /**
   *
   * @param filename : path to the dataRDD file
   * @param num      : number of trajectories to read
   * @param clean : clean the trajectories
   * @return : RDD[Trajectory]
   */
  def apply(filename: String, num: Int, numPartitions:Int = 8, clean: Boolean = false, mapRange: List[Double] = List(-180, -90, 180, 90)):
  RDD[geometry.Trajectory] = {

    val spark = SparkSession.builder().getOrCreate()
    var t = nanoTime
    val df = spark.read.option("header", "true")
      .option("numPartitions", numPartitions)
      .csv(filename).limit(num)
    val samplingRate = 15
    val trajRDD = df.rdd
    .repartition(numPartitions)
    .filter(row => row(8).toString.split(',').length >= 4) // each trajectory should have no less than 2 recorded points
    val resRDD = trajRDD.map(row => {
      val tripID = row(0).toString
      val taxiID = row(4).toString.toLong
      val startTime = row(5).toString.toLong
      val pointsString = row(8).toString
      var pointsCoord = new Array[Double](0)
      for (i <- pointsString.split(',')) pointsCoord = pointsCoord :+ i.replaceAll("[\\[\\]]", "").toDouble
      var points = new Array[geometry.Point](0)
      for (i <- 0 to pointsCoord.length - 2 by 2) {
        points = points :+ geometry.Point(Array(pointsCoord(i), pointsCoord(i + 1)), startTime + samplingRate * i / 2)
      }
      geometry.Trajectory(tripID, startTime, points, Map("taxiID" -> taxiID.toString))
    })
    println("==== Read CSV Done")
    resRDD.take(1)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    t = nanoTime
    println("--- Total number of lines: " + df.count)
    println("--- Total number of valid entries: " + resRDD.count)
    if (timeCount) println("... Counting time used: " + (nanoTime - t) / 1e9d + "s")
    if (clean) checkMapCoverage(removeRedundancy(trajBreak(resRDD)), mapRange)
    else resRDD
  }

  def splitTraj(trajectory: geometry.Trajectory, splitPoints: Array[Int]): Array[geometry.Trajectory] = {
    if (splitPoints.length == 1) Array(trajectory)
    else {
      var trajs = new Array[geometry.Trajectory](0)
      //println(splitPoints.deep)
      for (i <- 0 to splitPoints.length - 2) {
        val points = trajectory.points.slice(splitPoints(i), splitPoints(i + 1) + 1)
        trajs = trajs :+ geometry.Trajectory(trajectory.tripID, points(0).t, points)
      }
      trajs
    }
  }

  def trajBreak(trajRDD: RDD[geometry.Trajectory], speed: Double = 50, timeInterval: Double = 180): RDD[geometry.Trajectory] = {
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

  def removeRedundancy(trajRDD: RDD[geometry.Trajectory], sigmaZ: Double = 4.07): RDD[geometry.Trajectory] = {
    val t = nanoTime
    val resRDD = trajRDD.map(traj => {
      var newPoints = Array(traj.points(0))
      for (p <- 1 until traj.points.length) {
        if (greatCircleDistance(traj.points(p), newPoints.last) >= 2 * sigmaZ) newPoints = newPoints :+ traj.points(p)
      }
      geometry.Trajectory(traj.tripID, traj.startTime, newPoints)
    })
    println("==== Remove Redundancy Done")
    println("--- Now total number of entries: " + resRDD.count)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    resRDD
  }

  def checkMapCoverage(trajRDD: RDD[geometry.Trajectory], mapRange: List[Double]): RDD[geometry.Trajectory] = {
    val t = nanoTime
    val resRDD = trajRDD.filter(traj => {
      var check = true
      val loop = new Breaks
      loop.breakable {
        for (point <- traj.points) {
          if (point.lon < mapRange.head || point.lon > mapRange(2) || point.lat < mapRange(1) || point.lat > mapRange(3)) {
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
}

