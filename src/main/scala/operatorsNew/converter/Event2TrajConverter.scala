package operatorsNew.converter

import instances.{Duration, Entry, Event, Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Config
import operatorsNew.selector.SelectionUtils._

import scala.reflect.ClassTag

class Event2TrajConverter extends Converter {
  override val optimization = "none"

  def convert[EV: ClassTag, ED: ClassTag](input: RDD[Event[Point, EV, ED]]): RDD[Trajectory[EV, ED]] = {
    type I = Event[Point, EV, ED]
    type O = Trajectory[EV, ED]
    //    val event = input.take(1).head
    //    assert(event.data != None, "The data field cannot be None")
    //    input.map(e => (e.data.toString, e))
    ////      .repartition(input.getNumPartitions * 4)
    //      .groupByKey()
    //      .filter(_._2.size > 1)
    //      .mapValues(x => Trajectory[EV, ED](
    //        pointArr = x.map(_.entries.map(_.spatial)).toArray.flatten,
    //        durationArr = x.map(_.entries.map(_.duration)).toArray.flatten,
    //        valueArr = x.map(_.entries.map(_.value)).toArray.flatten,
    //        x.head.data)
    //        .sortByTemporal("start"))
    //      .map(_._2)
    input.map(e => (e.data, e.entries))
      .reduceByKey((x, y) => x ++ y)
      .map(x => {
        val entries = x._2.sortBy(_.temporal.start)
        new Trajectory(entries, x._1)
      })
  }

  def convert[EV: ClassTag, ED: ClassTag](input: RDD[Event[Point, EV, ED]], numPartitions: Int): RDD[Trajectory[EV, ED]] = {
    type I = Event[Point, EV, ED]
    type O = Trajectory[EV, ED]
    input.map(e => (e.data, e.entries))
      .reduceByKey((x, y) => x ++ y, numPartitions)
      .map(x => {
        val entries = x._2.sortBy(_.temporal.start)
        new Trajectory(entries, x._1)
      })
  }
}

object Event2TrajConverterTest extends App {

  val dataPath = args(0)
  val partition = args(1).toInt

  val spark = SparkSession.builder()
    .appName("test")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val eventRDD = spark.read.parquet(dataPath).as[E].toRdd.map(x => x.asInstanceOf[Event[Point, None.type, String]])
  val converter = new Event2TrajConverter
  val trajRDD = if (partition == 0) converter.convert(eventRDD) else converter.convert(eventRDD, partition)
  trajRDD.collect.take(5).foreach(println)
}
