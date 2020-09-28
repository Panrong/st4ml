package main.scala.od

import org.apache.spark.rdd.RDD
import main.scala.geometry.mmTrajectory
import main.scala.geometry.Distances.greatCircleDistance
import main.scala.graph.RoadGrid

object odQuery {
  /** helper functions */
  def genODRDD(mmTrajectoryRDD: RDD[mmTrajectory]): RDD[(String, Array[String])] = {
    val odRDD = mmTrajectoryRDD.map(x => (s"${x.points(0)}->${x.points.last}", x.tripID)) // k: "oVertex -> dVertex", v: tripID
    odRDD.groupByKey().map(x => (x._1, x._2.toArray)) // k: Array(oVertex, dVertex), v: Array(tripID)
  }

  def getNeighbors(vertex: String, rg: RoadGrid, th: Double): Array[String] = {
    val p = rg.id2vertex(vertex).point
    var r = 1
    var maxDist: Double = 0
    var neighbors = new Array[(String, Double)](0)
    while (maxDist < th && r <= 5) {
      neighbors = rg.getNearestVertex(p, 100, r)
      maxDist = neighbors.map(x => x._2).max
      r += 1
    }
    neighbors.filter(x => {
      x._2 <= th
    }).map(x => x._1)
  } //get neighbor vertices based on threshold

  /** query functions */
  def strictQuery(oVertex: String, dVertex: String, odRDD: RDD[(String, Array[String])]): Array[String] = { // single query
    val queryString = s"$oVertex->$dVertex"
    odRDD.filter(x => x._1 == queryString).flatMap(x => x._2).collect
  }

  def strictQuery(queryRDD: RDD[String], odRDD: RDD[(String, Array[String])]): Array[(String,Array[String])] = { // multiple queries
    queryRDD.map(x => (x, 1)).join(odRDD).map(x => (x._1,x._2._2)).collect
  }

  def thresholdQuery(oVertex: String, dVertex: String, odRDD: RDD[(String, Array[String])], th: Double, rg: RoadGrid): Array[String] = { // single query

    val oNeighbors = getNeighbors(oVertex, rg, th)
    val dNeighbors = getNeighbors(dVertex, rg, th)

    val res = for (o <- oNeighbors; d <- dNeighbors) yield (strictQuery(o, d, odRDD))
    res.flatten
  }

  def thresholdQuery(queryRDD: RDD[String], odRDD: RDD[(String, Array[String])], th: Double, rg: RoadGrid): Array[Array[String]] = { // multiple queries
    queryRDD.flatMap(x => {
      val oNeighbors = getNeighbors(x.split("->")(0), rg, th)
      val dNeighbors = getNeighbors(x.split("->")(1), rg, th)
      val res = for (o <- oNeighbors; d <- dNeighbors) yield (s"$o->$d", x)
      res
    }).join(odRDD).map(x=> x._2._2).collect
  }
}
