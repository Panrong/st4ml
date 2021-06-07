package examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.elasticsearch.spark.sparkContextFunctions
import utils.Config

object ReadFromEsDemo {

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es-reading")
      .master(Config.get("master"))

      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val esNode = args(0)
    val esPort = args(1)
    val esIndex = args(2)
    val esQuery = args(3)
    val outputPath = args(4)
    val numPartitions = args(5).toInt

    // reading
    val options = Map("pushdown" -> "true",
      "es.nodes" -> esNode,
      "es.port" -> esPort,
      "es.query" -> esQuery)
    // aggregation query example
//      "es.query" -> """{
//                      |    "query": {
//                      |        "range": {
//                      |            "timestamp" :{
//                      |                "gte": 1596608074,
//                      |                "lte": 1596626531
//                      |            }
//                      |        }
//                      |    },
//                      |    "aggs": {
//                      |        "id_groupby" :{
//                      |            "terms": {"field": "id.keyword"}
//                      |        }
//                      |    }
//                      |}
//                      |""".stripMargin)

    val resDs = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load(esIndex)

    println("Reading result top 5: ")
    resDs.take(5).foreach(println)
    resDs.printSchema()

    print("Total number of records: ")
    println(resDs.count())

    resDs.repartition(numPartitions).write.json(outputPath)

  }

}
