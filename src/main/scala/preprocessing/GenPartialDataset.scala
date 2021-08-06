package preprocessing

import org.apache.spark.sql.SparkSession
import utils.Config

object GenPartialDataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenPartialDataset")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val sourceFile = args(0)
    val ratio = args(1).toDouble

    val ds = spark.read.parquet(sourceFile)
    val sampledDs = ds.sample(ratio)
    sampledDs.show(5)
    sampledDs.write.parquet(sourceFile + "_" + ratio.toString)

  }
}
