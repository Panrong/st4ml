package preprocessing

import geometry.Distances.greatCircleDistance

import java.lang.System.nanoTime
import geometry.{Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.control.Breaks

object readTrajFile {

  /**
   *
   * @param filename : path to the data file
   * @param num      : number of trajectories to read
   * @clean validate : validate the trajectories
   * @return : Dataset[Trajectory]
   *         +-------------------+--------+----------+--------------------+
   *         |             tripID|  taxiID| startTime|              points|
   *         +-------------------+--------+----------+--------------------+
   *         |1372636858620000589|20000589|1372636858|[[-8.618643, 41.1...|
   *         |1372637303620000596|20000596|1372637303|[[- 8.6 3 9 8 4 7, 41.1...|
   *         |1372636951620000320|20000320|1372636951|[[-8.612964, 41.1...|
   *         |1372636854620000520|20000520|1372636854|[[- 8.5 7 4 6 7 8, 41.1...|
   *         |1372637091620000337|20000337|1372637091|[[-8.645994, 41.1...|
   *         +-------------------+--------+----------+--------------------+
   *
   */
  val timeCount = true

  def apply(filename: String, num: Int, clean: Boolean = false, mapRange: List[Double] = List(0, 0, 0, 0)): Dataset[Trajectory] = {

    val spark = SparkSession.builder().getOrCreate()
    val t = nanoTime
    import spark.implicits._
    val df = spark.read.option("header", "true").csv(filename).limit(num)
    val samplingRate = 15
    val trajRDD = df.rdd.filter(row => row(8).toString.split(',').length >= 4) // each trajectory should have no less than 2 recorded points
    val resRDD = trajRDD.map(row => {
      val tripID = row(0).toString.toLong
      val taxiID = row(4).toString.toLong
      val startTime = row(5).toString.toLong
      val pointsString = row(8).toString
      var pointsCoord = new Array[Double](0)
      for (i <- pointsString.split(',')) pointsCoord = pointsCoord :+ i.replaceAll("[\\[\\]]", "").toDouble
      var points = new Array[Point](0)
      for (i <- 0 to pointsCoord.length - 2 by 2) {
        points = points :+ Point(Array(pointsCoord(i), pointsCoord(i + 1)), startTime + samplingRate * i / 2)
      }
      Trajectory(tripID, startTime, points)
    })
    println("==== Read CSV Done")
    println("--- Total number of lines: " + df.count)
    println("--- Total number of valid entries: " + resRDD.count)
    if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
    if (clean) checkMapCoverage(removeRedundancy(trajBreak(resRDD)), mapRange).toDS
    else resRDD.toDS
  }

  def splitTraj(trajectory: Trajectory, splitPoints: Array[Int]): Array[Trajectory] = {
    if (splitPoints.length == 1) Array(trajectory)
    else {
      var trajs = new Array[Trajectory](0)
      //println(splitPoints.deep)
      for (i <- 0 to splitPoints.length - 2) {
        val points = trajectory.points.slice(splitPoints(i), splitPoints(i + 1) + 1)
        trajs = trajs :+ Trajectory(trajectory.tripID, points(0).t, points)
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
      Trajectory(traj.tripID, traj.startTime, newPoints)
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

//object readTrajTest extends App {
//  override def main(args: Array[String]): Unit = {
//    /** set up Spark */
//    readTrajFile("/datasets/porto_traj.csv", 1000).show(5)
//  }
//}
