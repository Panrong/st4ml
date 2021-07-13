package operatorsNew

import instances.{Duration, Event, Extent, Point}
import operators.convertion.DoNothingConverter
import operatorsNew.converter.Converter
import operatorsNew.extractor.Extractor
import operatorsNew.selector.{DefaultSelector, Selector}
import org.apache.spark.sql.SparkSession

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
    val operatorSet = new OperatorSet {
      type I = Event[Point, Null, Null]
      override val selector = new DefaultSelector[I](sQuery, tQuery, numPartitions)
      override val converter = new DoNothingConverter[I]
      override val extractor: Extractor[_] = _
    }
    sc.stop()
  }
}
