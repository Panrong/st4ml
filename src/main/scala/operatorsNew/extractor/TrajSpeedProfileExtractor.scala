package operatorsNew.extractor

import instances.{Duration, Event, Extent, Point, Trajectory}
import operatorsNew.selector.Selector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class TrajSpeedProfileExtractor[V: ClassTag, D: ClassTag] extends Extractor[Trajectory[V, D]] {
  type T = Trajectory[V, D]

  /**
   * extract the interval speeds of trajectories
   *
   * @param rdd : trajectory RDD
   * @return : new trajectory RDD with data field recording interval speeds
   */
  def extract(rdd: RDD[T], metric: String = "greatCircle", convertKmh: Boolean = false): RDD[Trajectory[V, Array[Double]]] = {
    rdd.map(x => {
      val data = if (convertKmh) (x.consecutiveSpatialDistance(metric) zip
        x.entries.map(_.temporal).sliding(2).map(x => x(1).end - x(0).start).toSeq)
        .map(x => x._1 / x._2 * 3.6)
      else (x.consecutiveSpatialDistance(metric) zip
        x.entries.map(_.temporal).sliding(2).map(x => x(1).end - x(0).start).toSeq)
        .map(x => x._1 / x._2)
      new Trajectory(x.entries, data)
    }
    )
  }
}

object TrajSpeedProfileExtractorTest extends App {
  val spark = SparkSession.builder()
    .appName("TrajSpeedProfileExtractorTest")
    .master("local[4]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val spatial = Extent(-8.651553, 41.066949, -7.984440, 41.324905).toPolygon
  val temporal = Duration(1376540750, 1394656756)
  val fileName = "datasets/porto_taxi_traj_0.2_tstr"
  val metadata = "datasets/traj_0.2_metadata.json"
  type TRAJ = Trajectory[Option[String], String]
  val selector = Selector[TRAJ](spatial, temporal, 4)
  val trajRDD = selector.selectTraj(fileName, metadata, false)
  val extractor = new TrajSpeedProfileExtractor[Option[String], String]
  val res = extractor.extract(trajRDD).map(_.data).collect
  res.foreach(x => println(x.deep))
}