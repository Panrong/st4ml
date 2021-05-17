package examples

import geometry.{Point, Rectangle, Trajectory}
import operators.OperatorSet
import operators.convertion.Traj2PointConverter
import operators.extraction.TransferRateExtractor
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.SpatialProcessing.gridPartition
import utils.TimeParsing.parseTemporalRange

object TransferDev {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("TransferDev")
      .getOrCreate()

    /** example input:
     * "118.35,29.183,120.5,30.55" "2020-07-31 23:30:00,2020-08-02 00:30:00" 4 3600 */
    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val tStart = tQuery._1
    val tEnd = tQuery._2
    val ranges = gridPartition(sQuery.coordinates, args(2).toInt)
    val timeInterval = args(3).toInt
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** initialize operators */
    val operator = new OperatorSet(sQuery, tQuery) {
      type I = Trajectory
      type O = Point
      override val converter = new Traj2PointConverter
      override val extractor = new TransferRateExtractor
    }

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    println(s"--- ${rdd1.count} trajectories")

    /** step 2: Conversion */
    val pointRDD = operator.converter.convert(rdd1)
      .filter(p => p.inside(sQuery) && p.timeStamp._1 >= tStart && p.timeStamp._2 <= tEnd)
    println(s" ${pointRDD.count} point after conversion")

    /** step 3: Repartition */

    /** step 4: Extraction */
    val resRDD = operator.extractor.extract(ranges, pointRDD, numPartitions, tStart, timeInterval)
    resRDD.take(5).foreach {
      case (pId, transferTable) =>
        println(s"time slot: ${pId * timeInterval + tStart}, ${(pId + 1) * timeInterval + tStart}")
        for (i <- transferTable) println(i)
    }

    sc.stop()
  }
}
