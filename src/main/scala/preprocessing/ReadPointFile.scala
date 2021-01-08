package preprocessing

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
      .map(row =>
        geometry.Point(Array(try row.getString(2).toDouble catch {
          case _: Throwable => 0.0
        }, try row.getString(1).toDouble catch {
          case _: Throwable => 0.0
        }
        ))
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
