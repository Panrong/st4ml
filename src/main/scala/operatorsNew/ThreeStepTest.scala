package operatorsNew

import instances.{Duration, Event, Extent, Point, Trajectory}
import operatorsNew.converter.{DoNothingConverter, Event2TrajConverter, Traj2EventConverter}
import operatorsNew.extractor.{AnomalyExtractor, VITExtractor}
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

    val inputRDD = ParquetReader.readFaceParquet("datasets/face_example.parquet")
    val sQuery = Extent(-180, -90, 180, 90)
    val tQuery = Duration(0, Long.MaxValue)

    val operatorSet = new OperatorSet {
      type I = Event[Point, None.type, String]
      type O = Event[Point, None.type, String]
      val selector = new DefaultSelector[I](sQuery, tQuery, numPartitions)
      val converter = new DoNothingConverter[I]
      val extractor = new AnomalyExtractor[O]
    }
    val rdd1 = operatorSet.selector.query(inputRDD)
    println(s"${rdd1.count} events")
    val rdd2 = operatorSet.converter.convert(rdd1)
    val rdd3 = operatorSet.extractor.extract(rdd2, Array(23, 4), Array(sQuery.toPolygon))


    /** test trajectory to point */
    val trajRDD = ParquetReader.readVhcParquet("datasets/traj_example.parquet")
    val converter = new Traj2EventConverter[None.type, String]
    val convertedRDD = converter.convert(trajRDD)
    println(s"${trajRDD.count} trajectories converted to ${convertedRDD.count} events")
    println("5 examples:")
    convertedRDD.take(5).foreach(println(_))

    /** test vit */
    //    val extractor = new VITExtractor[Trajectory[None.type, String]]
    //    val vit = extractor.extract(trajRDD, 40)
    //    vit.take(10).foreach(x => println(x._1.data, x._2.deep))

    /** test event to trajectory */
    val e2tConverter = new Event2TrajConverter[None.type, String]
    val cTrajRDD = e2tConverter.convert(convertedRDD)
    cTrajRDD.take(5).foreach(println(_))
    println(s"Number of trajs after conversion back: ${cTrajRDD.count}")

    sc.stop()
  }
}
