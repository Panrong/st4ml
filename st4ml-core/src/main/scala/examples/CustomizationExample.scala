package examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Polygon
import st4ml.instances.{Duration, SpatialMap, Trajectory}
import st4ml.operators.converter.Traj2SpatialMapConverter
import st4ml.operators.extractor.Extractor
import st4ml.operators.selector.SelectionUtils.{ReadRaster, TrajDefault}
import st4ml.operators.selector.Selector

object CustomizationExample {
  def main(args: Array[String]): Unit = {
    // example inputs: local[*] datasets/porto_toy datasets/porto_raster.csv 64
    val master = args(0)
    val trajDir = args(1)
    val rasterDir = args(2)
    val parallelism = args(3).toInt

    val spark = SparkSession.builder()
      .appName("CustomizationExample")
      .master(master)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val sm = ReadRaster(rasterDir)._1.distinct // read the spatial grids from the raster

    val selector = new Selector(tQuery = Duration(1408039037, 1408125437), parallelism = parallelism) // select trajectories according to the timestamps
    val converter = new Traj2SpatialMapConverter(sm)

    /** example for writing a customized converter */
    println("=== Customized Converter")
    val selectedRDD = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD.count} trajectories")
    val preMap: TrajDefault => Trajectory[None.type, String] = traj => traj.mapData(x => x("TRIP_ID")) // discard the rest attributes
    val agg: Array[Trajectory[None.type, String]] => Int = _.length // count
    val convertedRDD = converter.convert(selectedRDD, preMap, agg) // utilizing the two customized functions
    import st4ml.instances.Utils._
    val merge: (Int, Int) => Int = _ + _
    val resultSm = convertedRDD.collectAndMerge(0, merge)
    println(s"Number of trajectories inside each cell: ${resultSm.entries.map(_.value).deep}") // utilizing the collectiveRDD functions

    /** example for writing a customized extractor */
    println("\n=== Customized Extractor")
    val selectedRDD2 = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD2.count} trajectories")
    val convertedRDD2 = converter.convert(selectedRDD2)
    class CountExtractor extends Extractor {
      def extract(rdd: RDD[SpatialMap[Polygon, Array[TrajDefault], None.type]]): Array[Int] = {
        val merge: (Int, Int) => Int = _ + _
        rdd.map(sm => sm.mapValue(_.length)).collectAndMerge(0, merge)
          .entries.map(_.value)
      }
    }
    val extractor = new CountExtractor
    val extractedResult = extractor.extract(convertedRDD2)
    println(s"Number of trajectories inside each cell: ${extractedResult.deep}") // utilizing the customized extractor

    sc.stop
  }
}