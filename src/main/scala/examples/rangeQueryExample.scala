package examples

import org.apache.spark.sql.SparkSession
import preprocessing.{readQueryFile, readTrajFile}
import query.{queryWithDS, queryWithRDD, queryWithSTRPartitioner}

import scala.io.Source

object rangeQueryExample extends App {

  override def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    var config: Map[String, String] = Map()
    Source.fromFile("config").getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
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
    val queryDS = readQueryFile(queryFile)
    println("=== query DS: ")
    queryDS.show(5)

    /** query with DS */
    queryWithDS(trajDS, queryDS).show(5)

    /** query with RDD */
    queryWithRDD(trajDS, queryDS, numPartitions).show(5)


    /** query with STR partitioner */

    queryWithSTRPartitioner(trajDS, queryDS, numPartitions, samplingRate).show(5)

    /** stop Spark session */
    sc.stop()
  }
}
