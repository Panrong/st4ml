import main.scala.rangequery.rangeQuery
import main.scala.geometry.{Point, Rectangle, Trajectory}
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.mapmatching.preprocessing
import main.scala.graph.RoadGrid
import System.nanoTime

import main.scala.mapmatching.preprocessing.readQueryFile
import org.apache.spark.sql.{Row, SparkSession}

object runRangeQuery extends App {
  override def main(args: Array[String]): Unit = {
    /** input arguments */
    val master = args(0)
    val mmTrajFile = args(1)
    val numPartition = args(2).toInt
    val rTreeCapacity = args(3).toInt
    val query = args(4)
    val roadGraphFile = args(5)
    val gridSize = args(6).toDouble
    val resultdir = args(7)

    val conf = new SparkConf()
    conf.setAppName("RangeQueryRTree_v1").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    /** repartition */
    var t = nanoTime
    val rg = RoadGrid(roadGraphFile, gridSize)
    val trajRDD = preprocessing.readMMTrajFile(mmTrajFile).map(x => {
      var points = new Array[Point](0)
      for (p <- x.points) {
        val v = rg.id2vertex(p)
        points = points :+ Point(v.point.lon, v.point.lat)
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
    val mbrRDD = trajRDD.map(traj => traj.mbr.assignID(traj.tripID).addPointAttr(traj.points).addTrajAttr(traj))
    for (i <- mbrRDD.take(2)) println(i)
    /** generate RTree for each partition */
    val RTreeRDD = mbrRDD.mapPartitionsWithIndex((index, iter) => {
      Iterator((index, rangeQuery.genRTree(iter.toArray, rTreeCapacity)))
    })
    RTreeRDD.cache
    RTreeRDD.foreach(x => println(x))
    println("... RTree generation time: " + (nanoTime - t) / 1e9d + "s")
    t = nanoTime
    var queries = new Array[Rectangle](0)

    /** query with rtree */
    try {
      val numRandomQueries = query.toInt
      queries = genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), numRandomQueries)
    }
    catch {
      case ex: java.lang.NumberFormatException => {
        val queryFile = query
        queries = readQueryFile(queryFile)
      }
    }

    for ((queryRange, i) <- queries.zipWithIndex) {
      val queriedTrajRDD = RTreeRDD.flatMap(x => {
        rangeQuery.query(x._2._1, x._2._2, queryRange)
      })
      /*.filter(x => {
      rangeQuery.refinement(x, queryRange)
    })

       */
      val trajIDs = queriedTrajRDD.map(x => Row("(" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")",
      x.id.toString.dropRight(8), x.id.toString.takeRight(8))) // (query, tripID, taxiID)

      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val df = trajIDs.map({
        case Row(val1: String, val2: String, val3: String) => (val1, val2,val3)
      }).toDF("query range", "tripID", "taxiID")

      

      df.write.option("header", value = true).option("encoding", "UTF-8").csv(args(7) + s"/tmp/$i")
      println("=== " + trajIDs.count + " trajectories in the range (" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")")

      //val avgSpeed = queriedTrajRDD.map(x => x.asInstanceOf[Rectangle].trajectory.calAvgSpeed(queryRange)).mean
      //println("--- Average Speed: " + avgSpeed)
    }
    println("... RTree query time: " + (nanoTime - t) / 1e9d + "s")

    /** query with filter */
    //    t = nanoTime
    //    for (queryRange <- genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), args(4).toInt)) {
    //      val queriedTrajRDD2 = trajRDD.filter(x => x.intersect(queryRange))
    //      println("=== " + queriedTrajRDD2.count + " trajectories in the range (" + queryRange.x_min + ", " + queryRange.y_min + ", " + queryRange.x_max + ", " + queryRange.y_max + ")")
    //    }
    //    println("... Filter query time: " + (nanoTime - t) / 1e9d + "s")
    sc.stop()
  }

  def genRandomQueryBoxes(r: Rectangle, n: Int): Array[Rectangle] = {
    var rectangles = new Array[Rectangle](0)
    val rdm = new scala.util.Random(5)
    for (i <- 0 to n) {
      var two = (rdm.nextDouble, rdm.nextDouble)
      if (two._1 > two._2) two = two.swap
      val (x_min, x_max) = two
      two = (rdm.nextDouble, rdm.nextDouble)
      if (two._1 > two._2) two = two.swap
      val (y_min, y_max) = two
      rectangles = rectangles :+ Rectangle(Point(x_min * (r.x_max - r.x_min) + r.x_min, y_min * (r.y_max - r.y_min) + r.y_min), Point(x_max * (r.x_max - r.x_min) + r.x_min, y_max * (r.y_max - r.y_min) + r.y_min))
    }
    rectangles
  }


}
