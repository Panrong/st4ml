package operatorsNew.selector

import instances.{Duration, Extent, Instance, Trajectory}
import operatorsNew.selector.SelectionUtils.T
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Config

import scala.io.Source

class GridIndexer(grid: Grid3d) extends Serializable {

  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def index[T <: Instance[_, _, _]](rdd: RDD[T]): RDD[(String, T)] = {
    val gridBc = spark.sparkContext.broadcast(grid)
    rdd.flatMap { instance =>
      val grid = gridBc.value
      grid.findIntersections(instance).map(x => (x, instance))
    }
  }
}

object GridPartitionerTest {
  def main(args: Array[String]): Unit = {
    type TRAJ = Trajectory[Option[String], String]

    val spark = SparkSession.builder()
      .appName("GridPartitionerTest")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val range = args(1).split(",").map(_.toDouble)
    val queryFile = args(2)
    val metadata = args(3)
    val grid = new Grid3d(range(0), range(1), range(2), range(3), range(4).toLong, range(5).toLong, range(6), range(7), range(8).toInt)
    //    val grid = new Grid3d(-9, -7, 40, 42, 1370000000, 1400000000, 10, 10, 10)
    import spark.implicits._
    val trajDs = spark.read.parquet(fileName).as[T]
    val trajRDD = trajDs.toRdd
    //    println(trajRDD.map(_.extent.xMin).min, trajRDD.map(_.extent.xMax).max, trajRDD.map(_.extent.yMin).min, trajRDD.map(_.extent.yMax).max)
    //    println(trajRDD.map(_.duration.start).min, trajRDD.map(_.duration.end).max)
    /** step1: index and persist metadata */
    // TODO persist
    val gridIndexer = new GridIndexer(grid)
    val indexedRDD = gridIndexer.index(trajRDD)
    // TODO persist
    /** step2: load with metadata */
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble), Duration(r(4).toLong, r(5).toLong))
    })
    //    val query = (Extent(-8.651553, 41.066949, -7.984440, 41.324905), Duration(1372636853, 1372670053))
    for (query <- ranges) {
      val queried = grid.findPartitions(query)
      println("---")
      println(indexedRDD.filter(x => queried.contains(x._1)).count)
      // TODO actually load data from disk
      println(indexedRDD.filter(x => queried.contains(x._1)).filter(_._2.intersects(query._1, query._2)).count)
      //      println(trajRDD.filter(_.intersects(query._1, query._2)).count) // ground truth
      val selector = Selector[TRAJ](query._1.toPolygon, query._2, 16)
      val rdd1 = selector.selectTraj(fileName, metadata, false)
      println(rdd1.count)
      println("---")
    }
  }
}

