package preprocessing

import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ReadPointFile {
  def apply(fileName: String, limitNum: Int = Int.MaxValue): RDD[geometry.Point] = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read
      .option("header", "false")
      .option("multiline", "true")
      .json(fileName)
      .limit(limitNum)
    df.filter(!_.anyNull).rdd
      .filter(r=>NumberUtils.isNumber(r(1).toString) && NumberUtils.isNumber(r(2).toString))
      .map(row =>
        geometry.Point(Array(row.getString(2).toDouble ,row.getString(1).toDouble))
          .setID(row.getString(0).hashCode()))
  }
}

object ReadPointFileTest extends App {
  /** set up Spark environment */

  import scala.io.Source

  override def main(args: Array[String]): Unit = {
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

    val fileName = "datasets/cams.json"
    val res = ReadPointFile(fileName)
    println(res.take(5).deep)
    sc.stop()
  }
}
