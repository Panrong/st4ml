package operatorsNew

import instances.{Duration, Event, Extent, Point, Trajectory}
import operatorsNew.converter.{DoNothingConverter, Event2TrajConverter, Traj2EventConverter}
import operatorsNew.extractor.{AnomalyExtractor, VITExtractor}
import operatorsNew.selector._
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
      val selector = new DefaultLegacySelector[I](sQuery, tQuery, numPartitions)
      val converter = new DoNothingConverter[I]
      val extractor = new AnomalyExtractor[O]
    }
    val rdd1 = operatorSet.selector.query(inputRDD)
    println(s"${rdd1.count} events")
    val rdd2 = operatorSet.converter.convert(rdd1)
    val rdd3 = operatorSet.extractor.extract(rdd2, Array(23, 4), Array(sQuery.toPolygon))


    /** test trajectory to point */
    //    val trajRDD = ParquetReader.readVhcParquet("datasets/traj_example.parquet")
    //    val converter = new Traj2EventConverter[None.type, String]
    //    val convertedRDD = converter.convert(trajRDD)
    //    println(s"${trajRDD.count} trajectories converted to ${convertedRDD.count} events")
    //    println("5 examples:")
    //    convertedRDD.take(5).foreach(println(_))

    /** test vit */
    //    val extractor = new VITExtractor[Trajectory[None.type, String]]
    //    val vit = extractor.extract(trajRDD, 40)
    //    vit.take(10).foreach(x => println(x._1.data, x._2.deep))

    /** test event to trajectory */
    //    val e2tConverter = new Event2TrajConverter[None.type, String]
    //    val cTrajRDD = e2tConverter.convert(convertedRDD)
    //    cTrajRDD.take(5).foreach(println(_))
    //    println(s"Number of trajs after conversion back: ${cTrajRDD.count}")


    /** test multi-range selector */
    val sQueries = Array(
      //      Extent(-180, -90, 180, 90),
      Extent(120.09998321533203, 30.26715087890625, 120.1716537475586, 30.286340713500977),
      Extent(120.275, 30.0555, 120.354, 30.1579),
      Extent(120.079, 30.2713, 120.239, 30.352),
    )
    val tQueries = Array(
      //      Duration(0, Long.MaxValue),
      Duration(1596258205, 1596284484),
      Duration(1596245600, 1596627600),
      Duration(1596233800, 1596630500),
    )

    println("Testing multi spatial range selection: ")
    val msrs = new MultiSpatialRangeLegacySelector[Event[Point, None.type, String]](sQueries.map(_.toPolygon),
      tQueries.head,
      numPartitions)
    val msrsRes = msrs.query(inputRDD)
    println(s"Queried number: ${msrsRes.count}")
    val msrsInfoRes = msrs.queryWithInfo(inputRDD)
    msrsInfoRes.take(5).foreach(x => println(x._1, x._2.deep))
    println(s"Queried with info number: ${msrsInfoRes.count}")

    val msrsBenchmark = inputRDD.filter(_.intersects(tQueries.head))
      .filter(x => sQueries.exists(x.intersects))
    println(s"Benchmark0 number: ${msrsBenchmark.count}")

    println("Testing multi temporal range selection: ")
    val mtrs = new MultiTemporalRangeLegacySelector[Event[Point, None.type, String]](sQueries.head,
      tQueries,
      numPartitions)
    val mtrsRes = mtrs.query(inputRDD)
    println(s"Queried number: ${mtrsRes.count}")
    val mtrsInfoRes = mtrs.queryWithInfo(inputRDD)
    mtrsInfoRes.take(5).foreach(x => println(x._1, x._2.deep))
    println(s"Queried with info number: ${mtrsInfoRes.count}")

    val mtrsBenchmark = inputRDD.filter(_.intersects(sQueries.head))
      .filter(x => tQueries.exists(x.intersects))
    println(s"Benchmark0 number: ${mtrsBenchmark.count}")

    println("Testing multi ST range selection: ")
    val mstrs = new MultiSTRangeLegacySelector[Event[Point, None.type, String]](sQueries.map(_.toPolygon),
      tQueries,
      numPartitions)
    val mstrsRes = mstrs.query(inputRDD)
    println(s"Queried number: ${mstrsRes.count}")
    val mstrsInfoRes = mstrs.queryWithInfo(inputRDD)
    mstrsInfoRes.take(5).foreach(x => println(x._1, x._2.deep))
    println(s"Queried with info number: ${mstrsInfoRes.count}")

    sc.stop()
  }
}
