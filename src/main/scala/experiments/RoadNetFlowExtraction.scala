package experiments

import instances.GeometryImplicits.withExtraPointOps
import instances._
import operatorsNew.converter.{MapMatcher, Traj2SpatialMapConverter}
import operatorsNew.selector.DefaultLegacySelector
import operatorsNew.selector.SelectionUtils.T
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object RoadNetFlowExtraction {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RegionalSpeedTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val spatialRange = args(1).split(",").map(_.toDouble)
    val temporalRange = args(2).split(",").map(_.toLong)
    val numPartitions = args(3).toInt

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))

    import spark.implicits._
    val trajRDD = spark.read.parquet(fileName).drop("pId").as[T]
      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
      .filter(_.data!="invalid")
    val selector = new DefaultLegacySelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)
    val selectedRDD = selector.query(trajRDD)
    val mapMatcher = new MapMatcher("datasets/porto.csv")
    val mmTrajRDD = selectedRDD.map(traj => mapMatcher.mapMatch(traj))

    val converter = new Traj2SpatialMapConverter(Array(Extent(Array(0.0,0,1,1)).toPolygon))
    val smRDD = converter.convert(mmTrajRDD, mapMatcher.roadNetwork).map(_.mapValue(x => (x._1, x._2.length)))
    smRDD.count()
    val t = nanoTime

//    smRDD.take(2).foreach(println)


    val t2 = nanoTime

    //    println(speeds.deep)
    println((t2 - t) * 1e-9)

    sc.stop()
  }
}
