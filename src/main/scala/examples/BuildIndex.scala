package examples

import org.apache.spark.sql.SparkSession

import geometry.Point
import index.RTree

object BuildIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("BuildIndex")
      .getOrCreate()

    buildRTreeByPoints(spark)


    spark.stop()
  }

  private def buildRTreeByPoints(spark: SparkSession): Unit = {
    import spark.implicits._

    val toyPointsDF = Seq(
      Point(Array(1.0, 1.0)),
      Point(Array(2.0, 2.0)),
      Point(Array(3.0, 3.0)),
      Point(Array(4.0, 4.0)),
      Point(Array(5.0, 5.0)),
      Point(Array(6.0, 6.0))).toDF()

    toyPointsDF.show()
    toyPointsDF.col("coordinates")

  }

}
