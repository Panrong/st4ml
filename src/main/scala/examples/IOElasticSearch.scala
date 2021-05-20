package examples

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

object IOElasticSearch {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("io-es")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //writing
//    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
//    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    // reading
    // es.query=<uri or query dsl query>              # defaults to {"query":{"match_all":{}}}
    // es.nodes=<ES host address>                     # defaults to localhost
    // es.port=<ES REST port>                         # defaults to 9200
    val esResource = "spark/docs"
    val esQuery = "?q=*:*"
    val res = sc.esRDD(esResource, esQuery)
    println("Reading result")
    res.foreach(println)
  }

}
