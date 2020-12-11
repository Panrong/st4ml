package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc
import preprocessing.{ReadQueryFile, readTrajFile}
import query.{QueryWithDS, QueryWithPartitioner, QueryWithRDD}

import scala.io.Source

object RangeQueryExample extends App {

  override def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f =Source.fromFile("config")
      f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    val spark = SparkSession.builder().master(config("master")).appName(config("appName")).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val samplingRate = args(3).toDouble
    val dataSize = args(4).toInt

    /** generate trajectory MBR DS */
    val trajDS = readTrajFile(trajectoryFile, num = dataSize)
    println("=== traj DS: ")
    trajDS.show(5)
    /** generate query DS */
    val queryDS = ReadQueryFile(queryFile)
    queryDS.show(5)

    /** query with DS */
    val dsRes = QueryWithDS(trajDS, queryDS)
    (dsRes join(queryDS, dsRes("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID"))
      .show


    /** query with RDD */
    val rddRes = QueryWithRDD(trajDS, queryDS, numPartitions)
    (rddRes join(queryDS, rddRes("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID"))
      .show

    /** query with STR partitioner */

    val strRes = QueryWithPartitioner(trajDS, queryDS, numPartitions, samplingRate)
    (strRes join(queryDS, strRes("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID"))
      .show

    /** query with grid partitioner */

    val gridRes = QueryWithPartitioner(trajDS, queryDS, numPartitions, samplingRate,"grid")
    (strRes join(queryDS, strRes("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID"))
      .show

    /** stop Spark session */
    sc.stop()
  }
}
