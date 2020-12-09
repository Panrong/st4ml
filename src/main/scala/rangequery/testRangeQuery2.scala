//package rangequery
//
//import graph.RoadGrid
//import mapmatching.preprocessing
//import org.apache.spark.{SparkConf, SparkContext}
//import System.nanoTime
//
//object testRangeQuery2 extends App {
//  override def main(args: Array[String]): Unit = {
//    val master = args(0)
//    val mmTrajFile = args(1)
//    val numPartition = args(2).toInt
//    val query = args(3)
//    val roadGraphFile = args(4)
//
//    val conf = new SparkConf()
//    conf.setAppName("RangeQuery_v2").setMaster(master)
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//    var t = nanoTime()
//    val queries = preprocessing.readQueryFile(query)
//    val numQueries = queries.length
//    val queryRDD = sc.parallelize(queries, numPartition)
//    val roadMapRDD = sc.parallelize(
//      RoadGrid(roadGraphFile).edges.map(x => (x.ls.mbr, x.id)), numPartition) // (mbr, roadID)
//    val queriedRoadSegs = queryRDD.cartesian(roadMapRDD)
//      .filter { case (qRange, (mbr, _)) => mbr.intersect(qRange) }
//      .map { case (qRange, (_, id)) => (id, qRange) }
//      .groupByKey()
//      .map { case (k, v) => (k, v.toArray) } //(roadID, queryBoxes)
//    queriedRoadSegs.cache()
//    println(s"--- Generating query RDD takes ${((nanoTime()-t)/10e9).formatted("%.3f")} s")
//    t = nanoTime()
//    val subTrajRDD = preprocessing.readMMTrajFile(mmTrajFile)
//      .map(x => (x.tripID, x.points.sliding(2).toArray.map(x=>s"${x(0)}-${x(1)}")))
//      .flatMapValues(x => x)
//      .map { case (x, y) => (y, x) }
//      .repartition(numPartition) // (roadID, (speed, tripID))
//    subTrajRDD.cache()
//    println(s"--- Generating subtrajectory RDD takes  ${((nanoTime()-t)/10e9).formatted("%.3f")} s")
//    t = nanoTime()
//    val res = queriedRoadSegs.join(subTrajRDD).map { case (_, (queries, tripID)) => (tripID, queries) }
//      .flatMapValues(x => x)
//      .map { case (k, v) => (v, k) }
//      .groupByKey()
//      .map { case (k, v) => (k, v.toArray.distinct) }
//
//    for (i <- res.collect) {
//      val queryRange = i._1
//      println(s"Query Range: $queryRange : ${i._2.length} sub-trajectories")
//    }
//    println(s"--- $numQueries range queries takes ${((nanoTime()-t)/10e9).formatted("%.3f")} s")
//  }
//}