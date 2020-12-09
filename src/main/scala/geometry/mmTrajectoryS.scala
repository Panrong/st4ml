package geometry

case class mmTrajectoryS(tripID: String, taxiID: String, startTime: Long = 0, subTrajectories: Array[subTrajectory]) extends Serializable {
}
