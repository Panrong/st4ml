//import main.scala.rangequery.rangeQuery
//import main.scala.mapmatching.SpatialClasses.{Point, Rectangle, Trajectory}
//import org.apache.spark.{SparkConf, SparkContext}
//import preprocessing.preprocessing
//import System.nanoTime
//
//import main.scala.STRPartitioner.STRPartitioner
//import main.scala.graph.RoadGraph
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//
//object runRangeQuery extends App {
//  override def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.setAppName("RangeQuery_v1").setMaster(args(0))
//    val sc = new SparkContext(conf)
//    val roadGraphFile = args(7)
//    sc.setLogLevel("ERROR")
//    /** repartition */
//    var t = nanoTime
//    val filename = args(1)
//    val rg = RoadGraph(roadGraphFile, 2)
//    val trajRDD = preprocessing.readMMTrajFile(filename).map(x => {
//      var points = new Array[Point](0)
//      for (p <- x.points) {
//        val v = rg.id2vertex(p)
//        points = points :+ Point(v.lon, v.lat)
//      }
//      var tripID: Long = 0
//      try {
//        tripID = x.tripID.toLong
//      } catch {
//        case ex:java.lang.NumberFormatException => {
//          tripID = 1
//        }
//      }
//      var taxiID: Long = 0
//      try {
//        taxiID = x.taxiID.toLong
//      } catch {
//        case ex:java.lang.NumberFormatException => {
//          taxiID = 1
//        }
//      }
//      Trajectory(tripID, taxiID, x.startTime, points)
//    })
//    println(trajRDD.count)
//    println("... Repartition time: " + (nanoTime - t) / 1e9d + "s")
//    t = nanoTime
//    val capacity = args(3).toInt
//    val mbrRDD = trajRDD.map(traj => traj.mbr.assignID(traj.tripID).addPointAttr(traj.points).addTrajAttr(traj))
//    for(i <-mbrRDD.take(2)) println(i)
//    /** generate RTree for each partition */
//    val RTreeRDD = mbrRDD.mapPartitionsWithIndex((index, iter) => {
//      Iterator((index, rangeQuery.genRTree(iter.toArray, capacity)))
//    })
//    RTreeRDD.cache
//    println("... RTree generation time: " + (nanoTime - t) / 1e9d + "s")
//    t = nanoTime
//
//    /** query with rtree */
//
//    for (queryRange <- genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), args(4).toInt)) {
//      val queriedTrajRDD = RTreeRDD.flatMap(x => {
//        rangeQuery.query(x._2._1, x._2._2, queryRange)
//      })
//        /*.filter(x => {
//        rangeQuery.refinement(x, queryRange)
//      })
//
//         */
//      println("=== " + queriedTrajRDD.count + " trajectories in the range (" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")")
//      //val avgSpeed = queriedTrajRDD.map(x => x.asInstanceOf[Rectangle].trajectory.calAvgSpeed(queryRange)).mean
//      //println("--- Average Speed: " + avgSpeed)
//    }
//    println("... RTree query time: " + (nanoTime - t) / 1e9d + "s")
//
//    /** query with filter */
//    t = nanoTime
//    for (queryRange <- genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), args(4).toInt)) {
//      val queriedTrajRDD2 = trajRDD.filter(x => x.intersect(queryRange))
//      println("=== " + queriedTrajRDD2.count + " trajectories in the range (" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")")
//    }
//    println("... Filter query time: " + (nanoTime - t) / 1e9d + "s")
//    sc.stop()
//  }
//
//  def genRandomQueryBoxes(r: Rectangle, n: Int): Array[Rectangle] = {
//    var rectangles = new Array[Rectangle](0)
//    val rdm = new scala.util.Random(5)
//    for (i <- 0 to n) {
//      var two = (rdm.nextDouble, rdm.nextDouble)
//      if (two._1 > two._2) two = two.swap
//      val (x_min, x_max) = two
//      two = (rdm.nextDouble, rdm.nextDouble)
//      if (two._1 > two._2) two = two.swap
//      val (y_min, y_max) = two
//      rectangles = rectangles :+ Rectangle(Point(x_min * (r.x_max - r.x_min) + r.x_min, y_min * (r.y_max - r.y_min) + r.y_min), Point(x_max * (r.x_max - r.x_min) + r.x_min, y_max * (r.y_max - r.y_min) + r.y_min))
//    }
//    rectangles
//  }
//}
