package experiments

import geometry.{Point, Rectangle, Trajectory}
import operators.OperatorSet
import operators.convertion.Traj2PointConverter
import operators.extraction.PointCompanionExtractor
import operators.repartitioner.TSTRRepartitioner
import operators.selection.DefaultSelector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.{nextDay, parseTemporalRange}

object CompanionExp {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("CompanionDaily")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1)) //should be one day with margin (e.g. 1 hour)
    val queryThreshold = args(2).split(",").map(_.toDouble)
    val sThreshold = queryThreshold.head
    val tThreshold = queryThreshold.last
    val optimize = args(3).toBoolean

    /**
     * example input arguments: "118.35,29.183,120.5,30.55" "2020-07-31 23:30:00,2020-08-02 00:30:00" "1000,600"
     */

    /** initialize operators */
    val operator = new OperatorSet {
      override type I = Trajectory
      override type O = Point
      override val selector = new DefaultSelector[I](sQuery, tQuery)
      override val converter = new Traj2PointConverter(Some(sQuery), Some(tQuery))
      override val extractor = new PointCompanionExtractor
    }

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    //    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"--- Total points: ${rdd2.count}")
    println(s"... Total ids: ${rdd2.map(_.attributes("tripID")).distinct.count}")


    //    /** benchmark: full cartesian */
    //    val companionPairsRDD = rdd2.cartesian(rdd2).filter {
    //      case (x, y) => operator.extractor.isCompanion(tThreshold, sThreshold)(x, y)
    //    }.map(x => (x._1.id, Array(x._2.t, x._2.id)))
    //      .reduceByKey(_ ++ _)

    /** step 3: Extraction */
    val companionPairsRDD = if(optimize) operator.extractor.extractSTR(sThreshold, tThreshold)(rdd2)
    else operator.extractor.extract(sThreshold, tThreshold)(rdd2)
    companionPairsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rdd2.unpersist()
    println("=== Companion Analysis done: ")
    //    println(s"Max num companion: ${companionPairsRDD.map(_._2.size).max}")
    //    println(s"Average num companion: ${companionPairsRDD.map(_._2.size).mean}")
    println("=== 5 examples: ")
    companionPairsRDD.take(5).foreach { case (q, c) => println(s"  ... $q: $c") }

    sc.stop()
  }

}
