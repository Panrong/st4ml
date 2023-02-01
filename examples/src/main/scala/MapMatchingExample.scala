import org.apache.spark.sql.SparkSession
import st4ml.instances.Extent
import st4ml.operators.converter.Traj2TrajConverter
import st4ml.operators.selector.SelectionUtils.readOsm
import st4ml.operators.selector.Selector

object MapMatchingExample {
  def main(args: Array[String]): Unit = {
    /** to generate the road network that covers all trajectories,
     * run
     * >> cd preprocessing && python process_osm.py -r='-8.7,41,-7.5,41.5' -o porto
     * the map file is too large for github
     */
    // example inputs: local[*] ../datasets/porto_toy ../datasets/osm_toy 64
    val master = args(0)
    val trajDir = args(1)
    val mapDir = args(2)
    val parallelism = args(3).toInt

    val spark = SparkSession.builder()
      .appName("MapMatchingExample")
      .master(master)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val map = readOsm(mapDir)
    val sRange = Extent(map.minLon, map.minLat, map.maxLon, map.maxLat).toPolygon

    val selector = new Selector(sRange, parallelism = parallelism)
    val converter = new Traj2TrajConverter(map)

    val selectedRDD = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD.count} trajectories")
    val convertedRDD = converter.convertWithInterpolation(selectedRDD) // this will interpolate the missing roads based on shortest path
    println(s"--- Two example map-matched trajectories:")
    convertedRDD.filter(_.data != "invalid").take(2).foreach(println)
    sc.stop()
  }
}
