package preprocessing

import scala.io.Source
import org.apache.spark.sql.SparkSession

class SparkSessionWrapper(configFile: String) extends Serializable {
  def parseConfig(configFile: String): Map[String, String] = {
    var config: Map[String, String] = Map()
    Source.fromFile(configFile).getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    config
  }

  val config = parseConfig(configFile)
  val spark: SparkSession = {
    SparkSession.builder().master(config("master")).appName(config("appName")).getOrCreate()
  }
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val timeCount = config.getOrElse("timeCount", "false").toBoolean
}
