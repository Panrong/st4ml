package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.Converter
import operators.extraction.PointCompanionExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.{nextDay, parseTemporalRange}


object CompanionDailyDev {
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

    /**
     * example input arguments: "118.35,29.183,120.5,30.55" "2020-07-31 23:30:00,2020-08-02 00:30:00" "1000,600"
     */

    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelector(numPartitions),
      new Converter,
      new PointCompanionExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    //    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.traj2Point(rdd1).filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && te >= tQuery._1
    })
    rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"--- Total points: ${rdd2.count}")

    /** step 3: Extraction */
    val companionPairsRDD = operator.extractor.optimizedExtract(sThreshold, tThreshold)(rdd2)
    println("=== Companion Analysis done: ")
    println("=== 5 examples: ")
    companionPairsRDD.take(5).foreach { case (q, c) => println(s"  ... $q: $c") }
    /*
        /** save results */
        val schema = StructType(
          Seq(
            StructField("ID", StringType, nullable = false),
            StructField("companion", MapType(LongType, StringType, valueContainsNull = false), nullable = false)
          )
        )
        //    println(companionPairsRDD.map(Row(_)).take(1).deep)
        val resDF = spark.createDataFrame(companionPairsRDD.map(x => Row(x._1, x._2)), schema)
        //    resDF.show
        //    resDF.printSchema()
        resDF.write.json(Config.get("resPath") + nextDay(tQuery._1))
    */
    sc.stop()
  }

}
