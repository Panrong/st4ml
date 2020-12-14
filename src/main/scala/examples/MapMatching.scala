package examples

import org.apache.spark.sql.SparkSession

import java.io.File
import scala.io.Source
import scala.math.min
import scala.reflect.io.Directory
import mapmatching.MapMatchingSubmitter

object MapMatching extends App {
  override def main(args: Array[String]): Unit = {
    /** set up Spark environment */
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

    val trajFile = args(0)
    val mapFile = args(1)
    val resDir = args(2)
    val numTraj = args(3).toInt
    val batchSize = min(args(4).toInt, args(3).toInt)

    val directory = new Directory(new File(resDir))
    directory.deleteRecursively()

    MapMatchingSubmitter(trajFile, mapFile, numTraj, batchSize, resDir)
      .run

  }
}
