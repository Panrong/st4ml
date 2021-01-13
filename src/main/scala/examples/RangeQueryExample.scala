//package examples
//
//import geometry.Trajectory
//import org.apache.spark.sql.{Dataset, SparkSession}
//import preprocessing.{resRangeQuery}
//import query.QuerySubmitter
//import STInstance.Query2d
//import scala.io.Source
//
//object RangeQueryExample extends App {
//
//  override def main(args: Array[String]): Unit = {
//
//    /** set up Spark environment */
//    var config: Map[String, String] = Map()
//    val f = Source.fromFile("config")
//    f.getLines
//      .filterNot(_.startsWith("//"))
//      .filterNot(_.startsWith("\n"))
//      .foreach(l => {
//        val p = l.split(" ")
//        config = config + (p(0) -> p(1))
//      })
//    f.close()
//    val spark = SparkSession.builder().master(config("master")).appName(config("appName")).getOrCreate()
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//
//    val trajectoryFile = args(0)
//    val queryFile = args(1)
//    val numPartitions = args(2).toInt
//    val samplingRate = args(3).toDouble
//    val rtreeCapacity = args(4).toInt
//    val dataSize = args(5).toInt
//
//    val querySubmitter = QuerySubmitter(trajectoryFile, queryFile, numPartitions, dataSize)
//
//    val trajDS = querySubmitter.tDS
//    val queryDS = querySubmitter.qDS
//
//
//    /** query with DS */
//    val queryWDS: Dataset[Query2d] => Dataset[Trajectory] => Dataset[resRangeQuery] = querySubmitter.queryWithDS
//    trajDS.transform(queryWDS(queryDS)).show
//
//    /** query with RDD */
//    val queryWRDD: Dataset[Query2d] => Dataset[Trajectory] => Dataset[resRangeQuery] = querySubmitter.queryWithRDD
//    trajDS.transform(queryWRDD(queryDS)).show
//
//    /** query with STR partitioner */
//    val queryWPartitioner: Dataset[Query2d] => (Double, String) => Dataset[Trajectory] => Dataset[resRangeQuery] = querySubmitter.queryWithPartitioner
//    trajDS.transform(queryWPartitioner(queryDS)(samplingRate, "STR")).show
//
//    /** query with grid partitioner */
//    trajDS.transform(queryWPartitioner(queryDS)(samplingRate, "grid")).show
//
//    /** query with RTree selection.indexer */
//    val queryWIndex: Dataset[Query2d] => (Double, Int, String) => Dataset[Trajectory] => Dataset[resRangeQuery] = querySubmitter.queryWithIndex
//    trajDS.transform(queryWIndex(queryDS)(samplingRate, rtreeCapacity, "STR")).show
//
//
//    /** stop Spark session */
//    sc.stop()
//  }
//}
