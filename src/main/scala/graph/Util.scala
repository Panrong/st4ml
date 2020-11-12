package graph

import scala.annotation.tailrec

/** Some utility functions. */
object Util {
  /** Builds up a list from the right using the given start value.
   *
   * Returns the list `(..., f(f(f(x).get).get).get, f(f(x).get).get, f(x).get, x)`,
   * where the head element h fulfills `f(h) = None`.
   *
   * Does not terminate if `f` never returns `None`.
   */
  def iterateRight[A](x: A)(f: A => Option[A]): List[A] = {
    @tailrec
    def go(x: A, acc: List[A]): List[A] = f(x) match {
      case None => x :: acc
      case Some(y) => go(y, x :: acc)
    }

    go(x, List.empty)
  }

  /** Merges a collection of streams into a single stream.
   *
   * Returns a sorted stream if all input streams are sorted.
   */
  def mergeStreams[A : Ordering](streams: Traversable[Stream[A]]): Stream[A] = {
    val streams1 = streams.toList filterNot (_.isEmpty) sortBy (_.head)
    if (streams1.isEmpty)
      Stream.empty
    else {
      val first = streams1.head
      first.head #:: mergeStreams(first.tail :: streams1.tail)
    }
  }

  /** Merges several streams into a single stream.
   *
   * Returns a sorted stream if all input streams are sorted.
   */
  def mergeStreams[A : Ordering](streams: Stream[A]*): Stream[A] =
    mergeStreams(streams)
}