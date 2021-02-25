package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadTrajJson {

  private case class Point(
                            latitude: String,
                            longitude: String,
                            timestamp: String
                          )

  private case class TmpTraj(
                              id: String,
                              points: Array[Point]
                            )

  def apply(fileName: String, numPartitions: Int): RDD[geometry.Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.option("multiline", "true").json(fileName)
    val ds = df.as[TmpTraj]
      .filter(tmpTraj => tmpTraj.points.length != 0)
      .map(tmpTraj => {
        val id = tmpTraj.id
        val traj = tmpTraj.points.length match {
          case 0 => geometry.Trajectory(id, 0, new Array[geometry.Point](0))
          case _ =>
            try {
              val points = tmpTraj.points.map(p =>
                geometry.Point(Array(p.longitude.toDouble, p.latitude.toDouble), p.timestamp.toLong, id))
              geometry.Trajectory(id, points.head.t, points)
            }
            catch {
              case _: Throwable => geometry.Trajectory("invalid", 0, Array(geometry.Point(Array(-181, -181))))
            }
        }
        traj
      })
    val res = ds.rdd.filter(_.id != "invalid").repartition(numPartitions)
    //    println(s"=== Total number of trajectories: ${res.count}")
    res
  }
}
