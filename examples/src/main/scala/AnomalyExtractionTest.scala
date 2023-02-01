import org.apache.spark.sql.SparkSession
import st4ml.instances.{Event, Point}
import st4ml.operators.selector.Selector
import st4ml.utils.TimeParsing.getHour

object AnomalyExtractionTest {
  case class PreE(shape: String, t: Array[String], data: Map[String, String])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnomalyExtraction")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    println("============================================")
    println("== ST4ML example: finding abnormal events ==")
    println("============================================")
    val threshold = Array(23, 4) // finding events occurring during 23pm to 4am
    type EVENT = Event[Point, Option[String], String]
    val condition = if (threshold(0) > threshold(1)) (x: Double) => x >= threshold(0) || x < threshold(1)
    else (x: Double) => x >= threshold(0) && x < threshold(1)
    val selector = new Selector[EVENT]()
    val eventRDD = selector.selectEventCSV("../datasets/nyc_toy")
    println(s"Processing ${eventRDD.count} events")
    val res = eventRDD.filter(x => condition(getHour(x.duration.start))).map(_.data)
    println(s"Extracted ${res.count} abnormal events occurring during ${threshold(0)} to ${threshold(1)} hrs.")
    println("2 examples: ")
    res.take(2).foreach(println)
    sc.stop()
  }
}
