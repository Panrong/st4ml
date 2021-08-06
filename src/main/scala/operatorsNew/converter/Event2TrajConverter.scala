package operatorsNew.converter

import instances.{Event, Point, Trajectory}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Event2TrajConverter[EV: ClassTag, ED] extends Converter {
  type I = Event[Point, EV, ED]
  type O = Trajectory[EV, ED]

  override def convert(input: RDD[I]): RDD[O] = {
    val event = input.take(1).head
    assert(event.data != None, "The data field cannot be None")
    //    input.map(e => (e.data.toString, e))
    //      .groupByKey()
    //      .filter(_._2.size > 1)
    //      .mapValues(x => Trajectory[EV, ED](
    //        pointArr = x.map(_.entries.map(_.spatial)).toArray.flatten,
    //        durationArr = x.map(_.entries.map(_.duration)).toArray.flatten,
    //        valueArr = x.map(_.entries.map(_.value)).toArray.flatten,
    //        x.head.data)
    //      .sortByTemporal("start"))
    //      .map(_._2)
    input.map(e => (e.data.toString, Array(e)))
      .reduceByKey(_ ++ _)
      .filter(_._2.length > 1)
      .mapValues(x => Trajectory[EV, ED](
        pointArr = x.flatMap(_.entries.map(_.spatial)),
        durationArr = x.flatMap(_.entries.map(_.duration)),
        valueArr = x.flatMap(_.entries.map(_.value)),
        x.head.data)
      .sortByTemporal("start")
    ).map(_._1)
  }
}
