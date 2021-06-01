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

    // reading
    val options = Map("es.nodes" -> esNode,
      "es.port" -> esPort,
      "es.query" -> esQuery)

    val resDs = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load(esIndex)


    println("Reading result top 5: ")
    resDs.take(5).foreach(println)
    resDs.printSchema()
    resDs.write.json(outputPath)
  }

}
