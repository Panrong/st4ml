package preprocessing
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import SpatialClasses._

object preprocessing {

  def genTrajRDD(filename: String): RDD[Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.option("header", "true").csv(filename)
    val samplingRate = 15
    val trajRDD = df.rdd.filter((row => row(8).toString.split(',').length >= 4)) // each traj should have no less than 2 recorded points
    val resRDD = trajRDD.map(row => {
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
    resRDD
  }

  def splitTraj(trajectory: Trajectory, splitPoints: Array[Int]): Array[Trajectory] = {
    if (splitPoints.length == 1) Array(trajectory)
    else {
      var trajs = new Array[Trajectory](0)
      //println(splitPoints.deep)
      for (i <- 0 to splitPoints.length - 2) {
        val points = trajectory.points.take(splitPoints(i + 1) + 1).drop(splitPoints(i))
        trajs = trajs :+ Trajectory(trajectory.tripID, trajectory.taxiID, points(0).t, points)
      }
      trajs
    }
  }

  def trajBreak(trajRDD: RDD[Trajectory], speed: Double = 50, timeInterval: Double = 180): RDD[Trajectory] = {
    println("==== Split trajectories with speed limit " + speed + " m/s and time interval limit " + timeInterval + " s")
    // speed and time interval check
    val newTrajRDD = trajRDD.flatMap(traj => {
      var splitPoints = new Array[Int](0)
      for (i <- 0 to traj.points.length - 2 by 2) {
        val t = traj.points(i + 1).t - traj.points(i).t
        val l = greatCircleDist(traj.points(i + 1), traj.points(i))
        if (l / t > speed || t > timeInterval) splitPoints = splitPoints :+ i
      }
      splitTraj(traj, 0 +: splitPoints)
    }).filter(traj => traj.points.length > 1)
    println("==== Split Trajectories Done")
    println("--- Now total number of entries: " + newTrajRDD.count)
    newTrajRDD
  }

  def removeRedundancy(trajRDD: RDD[Trajectory], sigmaZ:Double = 4.07): RDD[Trajectory] = {
    val resRDD = trajRDD.map(traj => {
      var newPoints = Array(traj.points(0))
      for (p<- 1 to traj.points.length -1){
        if(greatCircleDist(traj.points(p), newPoints.last) >= 2*sigmaZ) newPoints = newPoints :+ traj.points(p)
      }
      Trajectory(traj.tripID, traj.taxiID, traj.startTime, newPoints)
    })
    println("==== Remove Redundancy Done")
    println("--- Now total number of entries: " + resRDD.count)
    resRDD
  }

  def apply(filename: String): RDD[Trajectory] = {
    removeRedundancy(trajBreak(genTrajRDD(filename)))
  }
}

object preprocessingTest extends App {
  val conf = new SparkConf()
  conf.setAppName("MapMatching_v1").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val filename = "C:\\Users\\kaiqi001\\Map Matching\\src\\Porto_taxi_data_training.csv"
  val trajRDD = preprocessing(filename)
  println("==== Trajectory examples: ")
  for (i <- trajRDD.take(2)) {
    println("tripID: " + i.tripID + " taxiID: " + i.taxiID + " startTime: " + i.startTime + " points: " + i.points.deep)
  }
}