package operatorsNew

import instances.{Duration, Event, Extent, Point}
import operatorsNew.converter.DoNothingConverter
import operatorsNew.extractor.AnomalyExtractor
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ParquetReader

object ThreeStepTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("ThreeStepTest")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val numPartitions = 16


    val sQuery = Extent(-90,-180,90,180)
    val tQuery = Duration(0, 1000000000)

    val inputRDD = ParquetReader.readFaceParquet("datasets/face_example.parquet")
    val sQuery = Extent(-180, -90, 180, 90)
    val tQuery = Duration(0, Long.MaxValue)

    val operatorSet = new OperatorSet {
       type I = Event[Point, String, Null]
       type O = Event[Point, String, Null]
       val selector = new DefaultSelector[I](sQuery, tQuery, numPartitions)
       val converter = new DoNothingConverter[I]
       val extractor = new AnomalyExtractor[O]
    }
    val rdd1 = operatorSet.selector.query(inputRDD)
    println(s"${rdd1.count} events")
    val rdd2 = operatorSet.converter.convert(rdd1)
    val rdd3 = operatorSet.extractor.extract(rdd2, Array(23,4), Array(sQuery.toPolygon) )

>>>>>>> bb03aa1eda5d3653a525f262b23172e2c96c9663
    sc.stop()
  }
}
