package examples

import _root_.STTSession

object RangeQuery {
  def main(args: Array[String]): Unit = {
    val simbaSession = STTSession
      .builder()
      .master("local[4]")
      .appName("RangeQuery")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    runRangeQuery(simbaSession)

    STTSession.stop()
  }

  private def runRangeQuery(stt: STTSession): Unit = ???

}
