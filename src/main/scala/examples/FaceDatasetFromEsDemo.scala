package examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.elasticsearch.spark.sparkContextFunctions
import utils.Config

object FaceDatasetFromEsDemo {

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es")
      .master(Config.get("master"))
      .config("es.nodes", args(0))
      .config("es.port", args(1))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val esIndex = args(2)
    val esQuery = args(3)
    val outputPath = args(4)


    // reading
    // es.query=<uri or query dsl query>              # defaults to {"query":{"match_all":{}}}
    // es.nodes=<ES host address>                     # defaults to localhost
    // es.port=<ES REST port>                         # defaults to 9200
    //    val esResource = "trajs2"
    //    val esQuery = "?q=*:*"
    val resRdd = sc.esRDD(esIndex, esQuery)
    // (HEJrwnkB9NeNnnHfOvYY,Map(id -> 326e665e-b2e0-35f2-bef7-24c6be2b2fa6, latitude -> 30.2765380859375, longitude -> 120.11165364583333, timestamp -> 1597561598))
    println("Reading result top 5: ")
    resRdd.take(5).foreach(println)

    import spark.implicits._
    val schema = StructType(
      Seq(
        StructField("point", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
      )
    )
    val resDF = spark.createDataFrame(resRdd.map(x => Row(x._2)), schema)
    resDF.write.json(outputPath)


  }

}
