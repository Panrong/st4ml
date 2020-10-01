import java.lang.System.nanoTime

import main.scala.geometry.{Point, Trajectory}
import main.scala.graph.RoadGrid
import main.scala.mapmatching.preprocessing
import org.apache.spark.{SparkConf, SparkContext}

object runSpeedQuery extends App {
  override def main(args: Array[String]): Unit = {
    val master = args(0)
    val mmTrajFile = args(1)
    val numPartition = args(2).toInt
    val rTreeCapacity = args(3).toInt
    val query = args(4)
    val roadGraphFile = args(5)
    val gridSize = args(6).toDouble

    val conf = new SparkConf()
    conf.setAppName("SpeedQuery_v1").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var t = nanoTime
    val rg = RoadGrid(roadGraphFile, gridSize)
    val trajRDD = preprocessing.readMMTrajFile(mmTrajFile).map(x => {
      var points = new Array[Point](0)
      var i = 0
      for (p <- x.points) {
        val v = rg.id2vertex(p)
        points = points :+ Point(v.point.lon, v.point.lat, t = x.startTime + i * 15)
        i += 1
      }
      var tripID: Long = 0
      try {
        tripID = x.tripID.toLong
      } catch {
        case ex: java.lang.NumberFormatException => {
          tripID = 1
        }
      }
      var taxiID: Long = 0
      try {
        taxiID = x.taxiID.toLong
      } catch {
        case ex: java.lang.NumberFormatException => {
          taxiID = 1
        }
      }
      Trajectory(tripID, taxiID, x.startTime, points)
    }).repartition(numPartition)
    println(trajRDD.count)
    println("... Repartition time: " + (nanoTime - t) / 1e9d + "s")
    t = nanoTime

    val trajA = trajRDD.map(x => (x.tripID, x.points.dropRight(1)))
    val trajB = trajRDD.map(x => (x.tripID, x.points.drop(1)))
    val speedRDD = trajA.join(trajB).mapValues(x => {
      val a = x._1
      val b = x._2
      (a, b).zipped.map((x, y) => (x.calSpeed(y), x.t, y.t))
    }) // k: tripID, v: Array[speed, start time, end time]
    speedRDD.take(2).foreach(x => println(x._1, x._2.deep))
  }
}