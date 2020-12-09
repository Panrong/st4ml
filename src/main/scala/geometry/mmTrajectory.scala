package geometry

case class mmTrajectory(tripID: String, taxiID: String, startTime: Long = 0, points: Array[String]) extends Serializable {
}
