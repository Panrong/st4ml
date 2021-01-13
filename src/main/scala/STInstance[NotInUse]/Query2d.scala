package STInstance

case class Query2d(query: geometry.Rectangle, queryID: Long) extends Serializable{
  implicit def toShape: geometry.Rectangle =
    query.assignID(queryID)
}

