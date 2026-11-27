package zio.concurrent

import zio.{Chunk, ChunkBuilder, UIO, ZIO}

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer
import scala.jdk.CollectionConverters._

/**
 * Wrapper over `java.util.concurrent.ConcurrentHashMap`.
 *
 * A mutable map that fibers can share. Every operation returns a `UIO` that
 * runs the matching `ConcurrentHashMap` call each time it is executed. A
 * single call such as `compute`, `computeIfAbsent`, `putIfAbsent` or `replace`
 * updates its key atomically, but a `get` followed by a `put` does not: use
 * `compute` to update a value from its previous value.
 *
 * Operations that visit the entries (`toList`, `fold`, `collectFirst`, ...)
 * see them in the order of the underlying hash table, not in insertion order.
 *
 * {{{
 * import zio._
 *
 * val hits: UIO[Option[Int]] =
 *   for {
 *     counts <- ConcurrentMap.make("/home" -> 0)
 *     _ <- ZIO.foreachParDiscard(1 to 100) { _ =>
 *            counts.compute("/home", (_, n) => n.map(_ + 1))
 *          }
 *     n <- counts.get("/home")
 *   } yield n // Some(100)
 * }}}
 *
 * @see [[https://zio.dev/reference/sync/concurrentmap/ ConcurrentMap]]
 */
final class ConcurrentMap[K, V] private (private val underlying: ConcurrentHashMap[K, V]) extends AnyVal {

  /**
   * Finds the first element of a map for which the partial function is defined
   * and applies the function to it. "First" follows the iteration order of the
   * underlying hash table. Returns `None` when `pf` is defined for no entry.
   */
  def collectFirst[B](pf: PartialFunction[(K, V), B]): UIO[Option[B]] =
    ZIO.succeed {
      var result = Option.empty[B]
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          if (result.isEmpty && pf.isDefinedAt((k, v)))
            result = Some(pf((k, v)))
        }
      }
      result
    }

  /**
   * Attempts to compute a mapping for the specified key and its current mapped
   * value (or None if there is no current mapping).
   *
   * When `remap` returns `None`, the key is removed. Returns the new value, or
   * `None` when the key has no mapping afterwards. The update is atomic: 1000
   * fibers that each add one with `compute` in parallel leave the value 1000
   * higher.
   */
  def compute(key: K, remap: (K, Option[V]) => Option[V]): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.compute(key, remapComputeWith(remap))))

  /**
   * Returns the value bound to `key`. When the key is absent, it first binds
   * the key to `map(key)` and returns that value.
   */
  def computeIfAbsent(key: K, map: K => V): UIO[V] =
    ZIO.succeed(underlying.computeIfAbsent(key, mapWith(map)))

  /**
   * Attempts to compute a new mapping of an existing key. Returns the new
   * value, or `None` when the key is absent, in which case `remap` is not
   * called.
   */
  def computeIfPresent(key: K, remap: (K, V) => V): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.computeIfPresent(key, remapComputeIfPresentWith(remap))))

  /**
   * Tests whether a given predicate holds true for at least one element in a
   * map.
   */
  def exists(p: (K, V) => Boolean): UIO[Boolean] =
    ZIO.succeed {
      var result = false
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          if (!result && p(k, v))
            result = true
        }
      }
      result
    }

  /**
   * Folds the elements of a map using the given binary operator.
   */
  def fold[S](zero: S)(f: (S, (K, V)) => S): UIO[S] =
    ZIO.succeed {
      var result: S = zero
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          result = f(result, (k, v))
        }
      }
      result
    }

  /**
   * Tests whether a predicate is satisfied by all elements of a map.
   */
  def forall(p: (K, V) => Boolean): UIO[Boolean] =
    ZIO.succeed {
      var result = true
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          if (result && !p(k, v))
            result = false
        }
      }
      result
    }

  /**
   * Retrieves the value associated with the given key.
   */
  def get(key: K): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.get(key)))

  /**
   * Adds a new key-value pair and optionally returns previously bound value.
   */
  def put(key: K, value: V): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.put(key, value)))

  /**
   * Binds each key in `keyValues` to its value, replacing any existing
   * binding. When a key appears more than once, the last pair wins.
   */
  def putAll(keyValues: (K, V)*): UIO[Unit] =
    ZIO.succeed(underlying.putAll(keyValues.toMap.asJava))

  /**
   * Adds a new key-value pair, unless the key is already bound to a value
   * (even an equal one). Returns the value already bound to the key, or `None`
   * when the pair was added.
   */
  def putIfAbsent(key: K, value: V): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.putIfAbsent(key, value)))

  /**
   * True if there are no elements in this map.
   */
  def isEmpty: UIO[Boolean] =
    ZIO.succeed(underlying.isEmpty)

  /**
   * Removes the entry for the given key, optionally returning value associated
   * with it.
   */
  def remove(key: K): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.remove(key)))

  /**
   * Removes the entry for the given key if it is mapped to a given value.
   * Returns `true` when the entry was removed.
   */
  def remove(key: K, value: V): UIO[Boolean] =
    ZIO.succeed(underlying.remove(key, value))

  /**
   * Removes all elements which satisfy the given predicate.
   */
  def removeIf(p: (K, V) => Boolean): UIO[Unit] =
    ZIO.succeed(
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          if (p(k, v)) {
            val _ = underlying.remove(k)
          }
        }
      }
    )

  /**
   * Removes all elements which do not satisfy the given predicate.
   */
  def retainIf(p: (K, V) => Boolean): UIO[Unit] =
    ZIO.succeed(
      underlying.forEach {
        makeBiConsumer { (k: K, v: V) =>
          if (!p(k, v)) {
            val _ = underlying.remove(k)
          }
        }
      }
    )

  /**
   * Removes all elements.
   */
  def clear: UIO[Unit] =
    ZIO.succeed(underlying.clear())

  /**
   * Replaces the entry for the given key only if it is mapped to some value.
   * Returns the previous value, or `None` when the key is absent and nothing
   * changed.
   */
  def replace(key: K, value: V): UIO[Option[V]] =
    ZIO.succeed(Option(underlying.replace(key, value)))

  /**
   * Replaces the entry for the given key only if it was previously mapped to a
   * given value. Returns `true` when the entry was replaced.
   */
  def replace(key: K, oldValue: V, newValue: V): UIO[Boolean] =
    ZIO.succeed(underlying.replace(key, oldValue, newValue))

  /**
   * Collects all entries into a chunk.
   */
  def toChunk: UIO[Chunk[(K, V)]] =
    ZIO.succeed {
      val builder = ChunkBuilder.make[(K, V)]()

      val it = underlying.entrySet().iterator()
      while (it.hasNext()) {
        val entry = it.next()
        builder += entry.getKey() -> entry.getValue()
      }

      builder.result()
    }

  /**
   * Collects all entries into a list.
   */
  def toList: UIO[List[(K, V)]] =
    toChunk.map(_.toList)

  private[this] def mapWith(map: K => V): java.util.function.Function[K, V] =
    new java.util.function.Function[K, V] {
      def apply(k: K): V = map(k)
    }

  private[this] def remapComputeWith(remap: (K, Option[V]) => Option[V]): java.util.function.BiFunction[K, V, V] =
    new java.util.function.BiFunction[K, V, V] {
      def apply(k: K, v: V): V = remap(k, Option(v)).getOrElse(null.asInstanceOf[V])
    }

  private[this] def remapComputeIfPresentWith(remap: (K, V) => V): java.util.function.BiFunction[K, V, V] =
    new java.util.function.BiFunction[K, V, V] {
      def apply(k: K, v: V): V = if (v == null) v else remap(k, v)
    }

  private[this] def makeBiConsumer(f: (K, V) => Unit): BiConsumer[K, V] =
    new BiConsumer[K, V] {
      override def accept(t: K, u: V): Unit = f(t, u)
    }
}

/** Constructors for [[ConcurrentMap]]. */
object ConcurrentMap {

  /**
   * Makes an empty `ConcurrentMap`.
   */
  def empty[K, V]: UIO[ConcurrentMap[K, V]] =
    ZIO.succeed(new ConcurrentMap(new ConcurrentHashMap()))

  /**
   * Makes a new `ConcurrentMap` initialized with provided collection of
   * key-value pairs. When a key appears more than once, the last pair wins.
   */
  def fromIterable[K, V](pairs: Iterable[(K, V)]): UIO[ConcurrentMap[K, V]] =
    ZIO.succeed {
      val underlying = new ConcurrentHashMap[K, V]()

      pairs.foreach(kv => underlying.put(kv._1, kv._2))

      new ConcurrentMap(underlying)
    }

  /**
   * Makes a new `ConcurrentMap` initialized with provided key-value pairs. When
   * a key appears more than once, the last pair wins.
   */
  def make[K, V](pairs: (K, V)*): UIO[ConcurrentMap[K, V]] =
    ZIO.succeed {
      val underlying = new ConcurrentHashMap[K, V]()

      pairs.foreach(kv => underlying.put(kv._1, kv._2))

      new ConcurrentMap(underlying)
    }
}
