package preprocessing

import org.apache.spark.sql.SparkSession
trait SparkSessionWrapper {
  val config = new Config()
  lazy val spark: SparkSession = {
    SparkSession.builder().master(config.master).appName(config.appName).getOrCreate()
  }
  lazy val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
}
