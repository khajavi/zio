package zio.concurrent

import zio.{UIO, ZIO}

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Consumer, Predicate}
import scala.jdk.CollectionConverters._

/**
 * A `ConcurrentSet` is a Set wrapper over
 * `java.util.concurrent.ConcurrentHashMap`.
 *
 * A mutable set that fibers can share. Every operation returns a `UIO` that
 * runs the matching call on the underlying key set each time it is executed.
 * Operations that visit the elements (`find`, `fold`, `collectFirst`, ...) see
 * them in the order of the underlying hash table, not in insertion order.
 *
 * {{{
 * import zio._
 *
 * val visited: UIO[Set[String]] =
 *   for {
 *     seen <- ConcurrentSet.empty[String]
 *     _    <- ZIO.foreachParDiscard(List("/home", "/about", "/home"))(seen.add)
 *     all  <- seen.toSet
 *   } yield all // Set(/about, /home)
 * }}}
 *
 * @see
 *   [[https://zio.dev/reference/sync/concurrentset/ ConcurrentSet]]
 */
final class ConcurrentSet[A] private (private val underlying: ConcurrentHashMap.KeySetView[A, java.lang.Boolean])
    extends AnyVal {

  /**
   * Adds a new value. Returns `true` when the value was not in the set yet.
   */
  def add(x: A): UIO[Boolean] =
    ZIO.succeed(underlying.add(x))

  /**
   * Adds all new values. Returns `true` when the set changed.
   */
  def addAll(xs: Iterable[A]): UIO[Boolean] =
    ZIO.succeed(underlying.addAll(xs.asJavaCollection))

  /**
   * Removes all elements.
   */
  def clear: UIO[Unit] =
    ZIO.succeed(underlying.clear())

  /**
   * Finds the first element of a set for which the partial function is defined
   * and applies the function to it. "First" follows the iteration order of the
   * underlying hash table. Returns `None` when `pf` is defined for no element.
   */
  def collectFirst[B](pf: PartialFunction[A, B]): UIO[Option[B]] =
    ZIO.succeed {
      var result = Option.empty[B]
      underlying.forEach {
        makeConsumer { (a: A) =>
          if (result.isEmpty && pf.isDefinedAt(a)) {
            result = Some(pf(a))
          }
        }
      }
      result
    }

  /**
   * Tests whether the element is in the set.
   */
  def contains(x: A): UIO[Boolean] =
    ZIO.succeed(underlying.contains(x))

  /**
   * Tests if the elements in the collection are a subset of the set. Returns
   * `true` for an empty collection.
   */
  def containsAll(xs: Iterable[A]): UIO[Boolean] =
    ZIO.succeed(xs.forall(x => underlying.contains(x)))

  /**
   * Tests whether a given predicate holds true for at least one element in the
   * set.
   */
  def exists(p: A => Boolean): UIO[Boolean] =
    ZIO.succeed {
      var result = false
      underlying.forEach {
        makeConsumer { (a: A) =>
          if (!result && p(a))
            result = true
        }
      }
      result
    }

  /**
   * Returns the first element, in the iteration order of the underlying hash
   * table, that satisfies the predicate, or `None` when no element does.
   */
  def find[B](p: A => Boolean): UIO[Option[A]] =
    ZIO.succeed {
      var result = Option.empty[A]
      underlying.forEach {
        makeConsumer { (a: A) =>
          if (result.isEmpty && p(a))
            result = Some(a)
        }
      }
      result
    }

  /**
   * Folds the elements of a set using the given binary operator.
   */
  def fold[R, E, S](zero: S)(f: (S, A) => S): UIO[S] =
    ZIO.succeed {
      var result: S = zero
      underlying.forEach {
        makeConsumer { (a: A) =>
          result = f(result, a)
        }
      }
      result
    }

  /**
   * Tests whether a predicate is satisfied by all elements of a set.
   */
  def forall(p: A => Boolean): UIO[Boolean] =
    ZIO.succeed {
      var result = true
      underlying.forEach {
        makeConsumer { (a: A) =>
          if (result && !p(a))
            result = false
        }
      }
      result
    }

  /**
   * True if there are no elements in the set.
   */
  def isEmpty: UIO[Boolean] =
    ZIO.succeed(underlying.isEmpty)

  /**
   * Removes `x` from the set. Returns `true` when it was in the set.
   */
  def remove(x: A): UIO[Boolean] =
    ZIO.succeed(underlying.remove(x))

  /**
   * Removes every element of `xs` from the set. Returns `true` when the set
   * changed.
   */
  def removeAll(xs: Iterable[A]): UIO[Boolean] =
    ZIO.succeed(underlying.removeAll(xs.asJavaCollection))

  /**
   * Removes all elements which satisfy the given predicate. Returns `true` when
   * any element was removed.
   */
  def removeIf(p: A => Boolean): UIO[Boolean] =
    ZIO.succeed(underlying.removeIf(makePredicate(p)))

  /**
   * Keeps only the elements that are also in `xs`. Returns `true` when the set
   * changed.
   */
  def retainAll(xs: Iterable[A]): UIO[Boolean] =
    ZIO.succeed(underlying.retainAll(xs.asJavaCollection))

  /**
   * Removes all elements which do not satisfy the given predicate. Returns
   * `true` when any element was removed.
   */
  def retainIf(p: A => Boolean): UIO[Boolean] =
    ZIO.succeed(underlying.removeIf(makePredicate(a => !p(a))))

  /**
   * Number of elements in the set.
   */
  def size: UIO[Int] =
    ZIO.succeed(underlying.size())

  /**
   * Returns the current elements as an immutable `Set`.
   */
  def toSet: UIO[Set[A]] =
    ZIO.succeed(underlying.asScala.toSet)

  /**
   * Transform all elements of the ConcurrentSet using the given function.
   * Elements that `f` maps to the same value end up as one element.
   */
  def transform(f: A => A): UIO[Unit] =
    ZIO.succeed {
      val set = underlying.asScala.toSet
      underlying.removeAll(set.asJavaCollection)
      underlying.addAll(set.map(f).asJavaCollection)
      ()
    }

  private def makeConsumer(f: A => Unit): Consumer[A] =
    new Consumer[A] {
      override def accept(t: A): Unit = f(t)
    }

  private def makePredicate(f: A => Boolean): Predicate[A] =
    new Predicate[A] {
      def test(a: A): Boolean = f(a)
    }
}

/** Constructors for [[ConcurrentSet]]. */
object ConcurrentSet {

  /**
   * Makes an empty ConcurrentSet
   */
  def empty[A]: UIO[ConcurrentSet[A]] =
    ZIO.succeed {
      val keySetView = ConcurrentHashMap.newKeySet[A]()
      new ConcurrentSet(keySetView)
    }

  /**
   * Makes an empty ConcurrentSet with initial capacity
   */
  def empty[A](initialCapacity: Int): UIO[ConcurrentSet[A]] =
    ZIO.succeed {
      val keySetView = ConcurrentHashMap.newKeySet[A](initialCapacity)
      new ConcurrentSet(keySetView)
    }

  /**
   * Makes a new `ConcurrentSet` initialized with provided collection.
   */
  def fromIterable[A](as: Iterable[A]): UIO[ConcurrentSet[A]] =
    ZIO.succeed {
      val keySetView = ConcurrentHashMap.newKeySet[A]()
      as.foreach(x => keySetView.add(x))
      new ConcurrentSet(keySetView)
    }

  /**
   * Makes a new ConcurrentSet initialized with the provided elements
   */
  def make[A](as: A*): UIO[ConcurrentSet[A]] =
    ZIO.succeed {
      val keySetView = ConcurrentHashMap.newKeySet[A]()
      as.foreach(x => keySetView.add(x))
      new ConcurrentSet(keySetView)
    }
}
