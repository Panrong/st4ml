// For case studies

package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import st4ml.instances.Trajectory
import st4ml.utils.Config
import st4ml.operators.selector.SelectionUtils.{T, TrajPoint}
import st4ml.operators.selector.Selector

object ParseXiaoshanRaw {
  def main(args: Array[String]): Unit = {

    // datasets/xiaoshan_cam.csv datasets/20200621.csv 3600 datasets/xiaoshan_traj_example
    val camFile = args(0)
    val dataDir = args(1)
    val tThreshold = args(2).toDouble
    val resDir = args(3)

    val spark = SparkSession.builder()
      .appName("ParseXiaoshanRaw")
      .master(Config.get("master"))
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // parse camFile
    val camDf = spark.read.option("header", true).csv(camFile).drop("name")

    // parse data
    val schema = StructType(Array(
      StructField("v_id", StringType, false),
      StructField("t", TimestampType, false),
      StructField("something1", StringType, true),
      StructField("something2", StringType, true),
      StructField("camera_id", StringType, true),
      StructField("something3", StringType, true),
      StructField("something4", StringType, true),
      StructField("something5", StringType, true),
    ))
    val dataDf = spark.read.schema(schema).csv(dataDir)
      .select("v_id", "t", "camera_id")
      .join(camDf, "camera_id")
      .drop("camera_id")
      .withColumn("timestamp", $"t".cast("long"))
      .filter($"v_id" =!= "UNKNOW")

    println("2 examples of parsed raw data (check the time conversion):")
    dataDf.show(2)

    val dataRDD = dataDf.drop("t").rdd.map(row => (
      row.getString(0),
      row.getString(1).toDouble,
      row.getString(2).toDouble,
      row.getLong(3)
    ))
    println(s"Total number of valid points: ${dataRDD.groupBy(_._1).count}")

    val trajRDD = dataRDD.groupBy(_._1).map { x =>
      val points = x._2.toArray.map(x => (x._2, x._3, x._4)).sortBy(_._3)
      var groupedPoints = new Array[Array[(Double, Double, Long)]](0)
      for (p <- points) {
        if (groupedPoints.length == 0) groupedPoints = groupedPoints :+ Array(p)
        else if (p._3 - groupedPoints.last.last._3 <= tThreshold) {
          val lastGroup = groupedPoints.last
          groupedPoints = groupedPoints.dropRight(1) :+ (lastGroup :+ p)
        }
        else groupedPoints = groupedPoints :+ Array(p)
      }
      groupedPoints.map(g => (x._1, g))
    }.flatMap(x => x)
      .filter(_._2.length > 1)
    println(s"Total number of valid trajectories in $dataDir: ${trajRDD.count}")

    // reformat and save parquet
    val trajDs = trajRDD.map { x =>
      val points = x._2.map(x => TrajPoint(x._1, x._2, Array(x._3, x._3), None))
      T(points, x._1)
    }.toDS()

    trajDs.write.parquet(resDir)

    //    //test reading
    //    val selector = new Selector[Trajectory[None.type, String]]()
    //    selector.selectTraj(resDir).take(2).foreach(println)

    sc.stop()
  }
}
