package instances

import java.time

object test extends App {

  val t1 = time.Instant.now()
  Thread.sleep(5603)
  val t2 = time.Instant.now()
  val delta = t2.getEpochSecond - t1.getEpochSecond
  println(t2.getEpochSecond)
  println(t1.getEpochSecond)
  println(t1.plusSeconds(5).getEpochSecond)
  println(delta)

}
