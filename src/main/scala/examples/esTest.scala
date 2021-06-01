package examples

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import utils.Config

object esTest {
  case class Point(id: String, latitude: Double, longitude: Double, timestamp: Long)

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val p1 = Point("11", 1.1, 2.2, 1231L)
    val p2 = Point("22", 1.1, 2.2, 1231L)
    val p3 = Point("33", 1.1, 2.2, 1231L)
    val p4 = Point("44", 1.1, 2.2, 1231L)
    val p5 = Point("55", 1.1, 2.2, 1231L)

    import spark.implicits._
    val df = Seq(p1, p2, p3, p4, p5).toDF()
    df.saveToEs("test")

    val df2  = spark.read
      .format("org.elasticsearch.spark.sql")
//      .option("es.read.field.as.array.include", "")
      .load("test")

    df2.show()
    df2.printSchema()

    df2.write.json("./tmp")



  }
}
