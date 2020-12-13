package examples

import org.apache.spark.sql.SparkSession

import query.QuerySubmitter

import scala.io.Source

object RangeQueryExample extends App {

  override def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
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

    val querySubmitter = QuerySubmitter(trajectoryFile, queryFile, numPartitions, dataSize)

    /** query with DS */
    querySubmitter.queryWithDS().show

    /** query with RDD */
    querySubmitter.queryWithRDD().show

    /** query with STR partitioner */

    querySubmitter.queryWithPartitioner(samplingRate).show

    /** query with grid partitioner */

    querySubmitter.queryWithPartitioner(samplingRate, "grid").show

    /** query with RTree index */

    querySubmitter.queryWithIndex(samplingRate, 10).show

    /** stop Spark session */
    sc.stop()
  }
}
