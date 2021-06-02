package preprocessing

import geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import examples.FaceDatasetToEsDemo.Document

object ReadFaceJson {
  def apply(jsonPath: String): RDD[Point] = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    val faceDF = spark.read.json(jsonPath)
    val faceRDD = faceDF.as[Document].rdd.map(p => Point(Array(p.longitude, p.latitude), p.timestamp, p.id))
    faceRDD
  }
}
