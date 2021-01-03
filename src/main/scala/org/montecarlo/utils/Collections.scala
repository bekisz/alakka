package org.montecarlo.utils

import scala.annotation.tailrec

object Collections {
  /**
   * Taken from : https://rosettacode.org/wiki/Cartesian_product_of_two_or_more_lists#Scala
   * @param lst Any List
   * @tparam T with Any type
   * @return The list with cartesian product
   */
  def cartesianProduct[T](lst: List[T]*): List[List[T]] = {

    /**
     * Prepend single element to all lists of list
 *
     * @param e single element
     * @param ll list of list
     * @param a accumulator for tail recursive implementation
     * @return list of lists with prepended element e
     */
    @tailrec
    def pel(e: T,
            ll: List[List[T]],
            a: List[List[T]] = Nil): List[List[T]] =
      ll match {
        case Nil => a.reverse
        case x :: xs => pel(e, xs, (e :: x) :: a )
      }

    lst.toList match {
      case Nil => Nil
      case x :: Nil => List(x)
      case x :: _ =>
        x match {
          case Nil => Nil
          case _ =>
            lst.par.foldRight(List(x))( (l, a) =>
              l.flatMap(pel(_, a))
            ).map(_.dropRight(x.size))
        }
    }
  }

}
