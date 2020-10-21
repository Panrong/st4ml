import main.scala.graph.RoadGrid
import main.scala.mapmatching.preprocessing
import org.apache.spark.{SparkConf, SparkContext}
import System.nanoTime

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object runRangeSpeedQuery extends App {
  override def main(args: Array[String]): Unit = {
    var t = nanoTime()
    val master = args(0)
    val mmTrajFile = args(1)
    val numPartition = args(2).toInt
    val query = args(3)
    val roadGraphFile = args(4)
    val speedRange = args(5).split(",").map(x => x.toDouble)
    val minSpeed = speedRange(0)
    val maxSpeed = speedRange(1)
    val resDir = args(6)

    val conf = new SparkConf()
    conf.setAppName("SpeedQuery_v2").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    println("... Setting up Spark time: " + (nanoTime - t) / 1e9d + "s")
    t = nanoTime()
    val queries = preprocessing.readQueryFile(query)
    val queryRDD = sc.parallelize(queries, numPartition)
    val roadMapRDD = sc.parallelize(
      RoadGrid(roadGraphFile).edges.map(x => (x.ls.mbr, x.id)), numPartition) // (mbr, roadID)
    val queriedRoadSegs = queryRDD.cartesian(roadMapRDD)
      .filter { case (qRange, (mbr, _)) => mbr.intersect(qRange) }
      .map { case (qRange, (_, id)) => (id, qRange) }
      .groupByKey()
      .map { case (k, v) => (k, v.toArray) } //(roadID, queryBoxes)

    val rg = RoadGrid(roadGraphFile)
    val speedRDD = preprocessing.readMMWithRoadTime(mmTrajFile)
      .map(x => (x.tripID, x.subTrajectories.map(x => (x.roadEdgeID, x.startTime))))
      .mapValues(x => {
        (x.head +: x :+ x.last).sliding(3).toArray.map(x => {
          val d = rg.id2edge(x(0)._1).length / 2 + rg.id2edge(x(1)._1).length + rg.id2edge(x(2)._1).length / 2
          (x(1)._1, d / (x(2)._2 - x(0)._2))
        }) // tripID, Array((roadID, speed))
      })
      .flatMapValues(x => x)
      .map(x => (x._2._1, (x._2._2, x._1)))

    //    val speedRDD = preprocessing.readMMWithSpeed(mmTrajFile)
    //      .map(x => (x.tripID, x.subTrajectories))
    //      .flatMapValues(x => x)
    //      .mapValues(x => (x.roadEdgeID, x.speed))
    //      .map { case (x, y) => (y._1, (y._2, x)) }
    //      .repartition(numPartition) // (roadID, (speed, tripID))

    val res = queriedRoadSegs.join(speedRDD)
      .map { case (_, (queries, (speed, tripID))) => ((speed, tripID), queries) }
      .flatMapValues(x => x)
      .map { case (k, v) => (v, k) }
      .filter { case (_, v) => v._1 >= minSpeed && v._1 <= maxSpeed }
      .groupByKey()
      .map { case (k, v) => (k, v.toArray) }

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = res.map(x => {
      val queryRange = x._1
      var info = ""
      x._2.foreach(x => info += s"${x._2}:${x._1.formatted("%.2f")},")
      Row("(" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")",
        x._2.length.toString, info.dropRight(1))
    })
      .map({
        case Row(val1: String, val2: String, val3: String) => (val1, val2, val3)
      }).toDF("queryRange", "Num", "TrajID:Speed")
    df.write.option("header", value = true).option("encoding", "UTF-8").csv(resDir)

    for (i <- res.collect) {
      val queryRange = i._1
      val q = "(" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")"
      println(s"Query Range: $q : ${i._2.length} sub-trajectories has speed in the range ($minSpeed, $maxSpeed)")
    }
    println(s"==== Speed query for ${queries.length} ranges takes ${(nanoTime() - t) / 1e9d} s.")
    sc.stop()

  }
}

object runRoadIDSpeedQuery extends App {
  override def main(args: Array[String]): Unit = {
    val t = nanoTime()
    val master = args(0)
    val mmTrajFile = args(1)
    val numPartition = args(2).toInt
    val query = args(3)
    val roadGraphFile = args(4)
    val speedRange = args(5).split(",").map(x => x.toDouble)
    val minSpeed = speedRange(0)
    val maxSpeed = speedRange(1)
    val resDir = args(6)

    val conf = new SparkConf()
    conf.setAppName("SpeedQuery_v2").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rg = RoadGrid(roadGraphFile)

    val queries = preprocessing.readRoadIDQueryFile(query)
    val queryRDD = sc.parallelize(queries, numPartition)
      .map((_, 1))
    //
    //    val speedRDD = preprocessing.readMMWithSpeed(mmTrajFile)
    //      .map(x => (x.tripID, x.subTrajectories))
    //      .flatMapValues(x => x)
    //      .mapValues(x => (x.roadEdgeID, x.speed))
    //      .map { case (x, y) => (y._1, (y._2, x)) }
    //      // .repartition(numPartition) // (roadID, (speed, tripID))
    //      .groupByKey()

    val speedRDD = preprocessing.readMMWithRoadTime(mmTrajFile)
      .map(x => (x.tripID, x.subTrajectories.map(x => (x.roadEdgeID, x.startTime))))
      .mapValues(x => {
        (x.head +: x :+ x.last).sliding(3).toArray.map(x => {
          val d = rg.id2edge(x(0)._1).length / 2 + rg.id2edge(x(1)._1).length + rg.id2edge(x(2)._1).length / 2
          (x(1)._1, d / (x(2)._2 - x(0)._2))
        }) // tripID, Array((roadID, speed))
      })
      .flatMapValues(x => x)
      .map(x => (x._2._1, (x._2._2, x._1)))
      .groupByKey()
      .map { case (k, v) => (k, v.toArray) }

    val res = queryRDD.join(speedRDD)
      .map { case (k, v) => (k, v._2.filter(x => x._1 >= minSpeed && x._1 <= maxSpeed)) } // roadID, Array(speed, trajID)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = res.map(x => {
      var info = ""
      x._2.foreach(x => info += s"${x._2}:${x._1.formatted("%.2f")},")
      Row(x._1, x._2.length.toString, info.dropRight(1))
    })
      .map({
        case Row(val1: String, val2: String, val3: String) => (val1, val2, val3)
      }).toDF("queryID", "Num", "TrajID:Speed")
    df.write.option("header", value = true).option("encoding", "UTF-8").csv(resDir)


    for (i <- res.collect) {
      val queryRange = i._1
      println(s"Query road ID: $queryRange : ${i._2.size} sub-trajectories with speed  in the range ($minSpeed, $maxSpeed)")
    }
    println(s"==== Speed query for ${queries.length} ranges takes ${(nanoTime() - t) / 1e9d} s.")
    sc.stop()
  }
}