package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import utils.Config

import java.lang.System.nanoTime

object LoadingTest extends App {
  var spark = SparkSession.builder()
    .appName("LoadingTest")
    .master(Config.get("master"))
    .getOrCreate()

  var sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val fileName = args(0)
  val m = args(1).toBoolean
  var t = nanoTime
  if (m) {
    val df = spark.read.parquet(fileName)
    println(df.count)
    println((nanoTime - t) * 1e-9)
  }
  else {
    val dirs = Array(0, 1, 2, 3).map(x => fileName + s"/pId=$x")
    val df2 = spark.read.parquet(dirs: _*)
    //    val df2 = spark.read.parquet(fileName).filter(col("pId").isin(Array(0, 1, 2, 3, 4): _*))
    println(df2.count)
    println((nanoTime - t) * 1e-9)
  }
  sc.stop()
}
