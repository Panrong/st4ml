package preprocessing

import geometry.Rectangle
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source
import scala.reflect.ClassTag


object ReadQueryFile extends Serializable {
  implicit def tuple4Array[T: ClassTag](x: (T, T, T, T)): Array[T] = Array(x._1, x._2, x._3, x._4)

  def apply(f: String): Dataset[(Rectangle)] = {
    var queries = new Array[Rectangle](0)
    for ((line, i) <- (Source.fromFile(f).getLines).zipWithIndex) {
      val r = line.split(" ")
      queries = queries :+ Rectangle((r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble), i.toString)
    }
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    queries.toSeq.toDS()
      .withColumnRenamed("_1", "query")
      .as[Rectangle]
  }
}
