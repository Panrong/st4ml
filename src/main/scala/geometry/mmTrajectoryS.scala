package geometry

case class mmTrajectoryS(tripID: String, startTime: Long = 0, subTrajectories: Array[subTrajectory]) extends Serializable {
}
